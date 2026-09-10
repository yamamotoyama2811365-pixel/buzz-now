"""Privacy-light first-party page-view counter backed by Postgres aggregates."""
import hashlib
import hmac
import json
from datetime import date, timedelta
from html import escape as e
from urllib.parse import urlsplit

from fastapi import HTTPException, Query, Request


def _key(secret):
    return hashlib.sha256(('corporate-traffic:'+secret).encode()).digest()


def _path(value,root=''):
    path=urlsplit(value or '/').path
    if root and path.startswith(root):path=path[len(root):] or '/'
    if not path.startswith('/') or '..' in path or len(path)>300:raise ValueError
    return path


def _signature(path,day,secret):
    return hmac.new(_key(secret),(day+':'+path).encode(),hashlib.sha256).hexdigest()


def tracking_tag(path,root,secret):
    try:path=_path(path)
    except ValueError:return ''
    if path.startswith('/api/') or path.startswith('/search') or path.endswith('/correction'):return ''
    day=date.today().isoformat(); token=_signature(path,day,secret)
    payload=json.dumps({'path':root+path,'token':day+'.'+token},ensure_ascii=False,separators=(',',':'))
    payload=payload.replace('<','\\u003c').replace('>','\\u003e').replace('&','\\u0026')
    return '<script>(()=>{if(navigator.doNotTrack==="1")return;const d='+payload+';d.referrer=document.referrer;fetch("'+e(root,quote=True)+'/api/page-view",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(d),keepalive:true,credentials:"omit"}).catch(()=>{});})();</script>'


def _source(referrer,base):
    if not referrer:return 'direct'
    try:
        host=(urlsplit(referrer).hostname or '').lower()
        own=(urlsplit(base).hostname or '').lower()
    except ValueError:return 'other'
    if not host:return 'direct'
    if host==own:return 'internal'
    if host=='google.com' or host.endswith('.google.com'):return 'google'
    if host=='yahoo.co.jp' or host.endswith('.yahoo.co.jp'):return 'yahoo'
    if host=='bing.com' or host.endswith('.bing.com'):return 'bing'
    return 'referral'


def install(app,db,query,base,root,secret,authorize):
    @app.post('/api/page-view',status_code=204)
    async def page_view(request:Request):
        origin=request.headers.get('origin','')
        expected=urlsplit(base).scheme+'://'+urlsplit(base).netloc
        if origin!=expected:raise HTTPException(403)
        if int(request.headers.get('content-length','0') or 0)>2048:raise HTTPException(413)
        try:
            data=await request.json(); path=_path(data.get('path',''),root)
            day,signature=data.get('token','').split('.',1)
        except (AttributeError,ValueError,TypeError,json.JSONDecodeError):raise HTTPException(422)
        valid_days={(date.today()-timedelta(days=n)).isoformat() for n in (0,1)}
        if day not in valid_days or not hmac.compare_digest(_signature(path,day,secret),signature):raise HTTPException(403)
        source=_source(str(data.get('referrer',''))[:1000],base)
        con=db()
        try:
            with con,con.cursor() as cur:
                cur.execute('INSERT INTO corporate_page_views(day,path,source,views) VALUES (CURRENT_DATE,%s,%s,1) ON CONFLICT(day,path,source) DO UPDATE SET views=corporate_page_views.views+1',[path,source])
        finally:con.close()

    @app.get('/api/traffic')
    def traffic(request:Request,days:int=Query(30,ge=1,le=366)):
        authorize(request)
        rows=query("SELECT day,path,source,views FROM corporate_page_views WHERE day>=CURRENT_DATE-%s ORDER BY day DESC,views DESC",[days-1])
        return {'days':days,'views':sum(int(r['views']) for r in rows),'items':rows[:1000]}
