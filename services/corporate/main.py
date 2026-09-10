import json
import os
import threading
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from html import escape as e
from pathlib import Path
from urllib.parse import quote, urlencode
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.responses import HTMLResponse, Response
from .auth import authorize
from .analytics import analytics_tag
from .model import validate, identity, normalized_name, PREFECTURES, INDUSTRIES, preserve_profile, same_entity
from .news import render_reports, validate_reports
from .reference import render_reference

app=FastAPI(title='企業倒産・新規法人情報サイト',docs_url=None,redoc_url=None)
BASE=os.getenv('CORPORATE_PUBLIC_URL','https://buzz-now-1.onrender.com/corporate').rstrip('/')
ROOT='/corporate'
DSN=os.getenv('CORPORATE_DATABASE_URL','')
_cache={}; _lock=threading.Lock()
STYLE=Path(__file__).with_name('style.css').read_text()

def db():
    if not DSN: raise HTTPException(503,'Database is not configured')
    con=psycopg2.connect(DSN,connect_timeout=8)
    try:
        with con.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '10s'")
            cur.execute("SET LOCAL lock_timeout = '2s'")
        return con
    except Exception:
        con.close()
        raise

def query(sql,args=()):
    con=db()
    try:
        with con,con.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql,args); return [dict(x) for x in cur.fetchall()]
    finally: con.close()

def cached(key,fn,ttl=60):
    with _lock:
        hit=_cache.get(key)
        if hit and time.monotonic()-hit[0]<ttl:return hit[1]
    result=fn()
    with _lock:
        if len(_cache)>150:_cache.clear()
        _cache[key]=(time.monotonic(),result)
    return result

def records(kind='',prefecture='',industry='',q='',page=1):
    where=["published AND company NOT IN ('運営会社','老舗','同社','会社','企業','事業者','飲食店','店舗')"]; args=[]
    for field,value in [('kind',kind),('prefecture',prefecture),('industry',industry)]:
        if value: where.append(field+'=%s');args.append(value)
    if q:where.append('company ILIKE %s');args.append('%'+q.replace('%','\\%').replace('_','\\_')+'%')
    clause=' AND '.join(where)
    key=('records',kind,prefecture,industry,q,page)
    def read():
        total=query('SELECT count(*) AS n FROM corporate_events WHERE '+clause,args)[0]['n']
        items=query('SELECT id,payload,updated_at FROM corporate_events WHERE '+clause+' ORDER BY reported_date DESC,id LIMIT 40 OFFSET %s',args+[(page-1)*40])
        return {'total':total,'items':items,'page':page}
    return cached(key,read)

def analysis():
    def read():
        rows=query("SELECT id,prefecture,industry,reported_date,payload,first_seen FROM corporate_events WHERE published AND company NOT IN ('運営会社','老舗','同社','会社','企業','事業者','飲食店','店舗') AND kind='bankruptcy' ORDER BY reported_date,id")
        entities={}
        for r in rows:
            key=r['payload'].get('entity_key',r['id'])
            if key not in entities: entities[key]=r
        today=datetime.now(timezone(timedelta(hours=9))).date()
        recent=[r for r in entities.values() if today-timedelta(days=29)<=r['reported_date']<=today]
        prior=[r for r in entities.values() if today-timedelta(days=59)<=r['reported_date']<today-timedelta(days=29)]
        first=min((r['first_seen'].date() for r in rows),default=today)
        enough=(today-first).days>=60
        current=Counter((r['prefecture'],r['industry']) for r in recent if r['prefecture'] and r['industry'])
        previous=Counter((r['prefecture'],r['industry']) for r in prior if r['prefecture'] and r['industry'])
        groups=[]
        for (pref,ind),n in current.most_common(20):
            old=previous[(pref,ind)]
            status='比較データ蓄積中'
            if enough and old>=5 and n>=5: status='増加傾向' if n/old>=1.5 else '比較期間並み' if n/old>=.8 else '減少傾向'
            elif enough: status='件数が少ないため判定保留'
            causes=Counter(c for r in recent if (r['prefecture'],r['industry'])==(pref,ind) for c in r['payload'].get('causes',[]))
            sources=[{'name':r['payload']['company'],'url':r['payload']['source_url']} for r in recent if (r['prefecture'],r['industry'])==(pref,ind)][:4]
            groups.append(dict(prefecture=pref,industry=ind,current=n,previous=old,status=status,causes=causes.most_common(3),sources=sources))
        registrations=query("SELECT count(*) n FROM corporate_events WHERE published AND company NOT IN ('運営会社','老舗','同社','会社','企業','事業者','飲食店','店舗') AND kind='registration' AND reported_date BETWEEN %s AND %s",[today-timedelta(days=29),today])[0]['n']
        return dict(bankruptcies=len(recent),registrations=registrations,classified=sum(bool(x['industry']) for x in recent),groups=groups,enough=enough,start=str(first))
    return cached('analysis',read,120)

def shell(title,body,path='/',noindex=False,description='企業の倒産速報・新規法人情報を、出典とともに地域・業種別に整理。報道事案の変化と背景を確認できます。'):
    canonical=BASE+path
    page_title=title if title=='企業倒産・新規法人情報サイト' else title+' | 企業倒産・新規法人情報サイト'
    return HTMLResponse('<!doctype html><html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>'+e(page_title)+'</title><meta name="description" content="'+e(description,quote=True)+'"><meta name="robots" content="'+('noindex,follow' if noindex else 'index,follow')+'"><link rel="canonical" href="'+e(canonical,quote=True)+'"><meta property="og:title" content="'+e(title,quote=True)+'"><meta property="og:url" content="'+e(canonical,quote=True)+'">'+analytics_tag()+'<style>'+STYLE+'</style></head><body><header><div class="bar"><a class="logo" href="'+ROOT+'/">企業倒産・新規法人情報サイト</a><nav><a href="'+ROOT+'/bankruptcies">倒産速報</a><a href="'+ROOT+'/registrations">新設・新規法人</a><a href="'+ROOT+'/signals">地域・業種の動き</a></nav></div></header><main>'+body+'</main><footer><strong>企業倒産・新規法人情報サイト</strong><p>公開情報を出典付きで整理する企業情報サイト。報道日・法人番号指定日を表示しています。休廃業・登記閉鎖だけを倒産とは判定しません。</p><a href="'+ROOT+'/about">掲載方針・訂正について</a>　｜　<a href="'+ROOT+'/sources">収集状況</a>　｜　<a href="'+ROOT+'/sitemap.xml">サイトマップ</a></footer></body></html>',headers={'Cache-Control':'public, max-age=60','X-Content-Type-Options':'nosniff'})

def cards(items):
    if not items:return '<p class="empty">該当する情報はありません。地域や業種の条件を変えてお試しください。</p>'
    html=''
    for r in items:
        p=r['payload'];kind=p['kind']; label='倒産速報' if kind=='bankruptcy' else '新規法人'
        html+='<article class="row"><div><span class="tag '+kind+'">'+label+'</span><span class="meta">'+e(p['reported_date'])+' · '+e(p.get('prefecture') or '地域確認中')+'</span></div><h3><a href="'+ROOT+'/company/'+r['id']+'">'+e(p['company'])+'</a></h3><div>'+e(p['stage'])+'</div><div class="meta">'+e(p.get('industry') or '業種未確認')+'　｜　出典：'+e(p['source_name'])+'</div></article>'
    return html

def filters(kind,pref,industry,q):
    def options(values,selected,label):return '<option value="">'+label+'</option>'+''.join('<option'+(' selected' if x==selected else '')+'>'+e(x)+'</option>' for x in values)
    return '<form class="filters" action="'+ROOT+'/search" method="get"><input type="search" name="q" aria-label="会社名" placeholder="会社名で検索" value="'+e(q,quote=True)+'"><input type="hidden" name="kind" value="'+e(kind,quote=True)+'"><select name="prefecture" aria-label="都道府県">'+options(PREFECTURES,pref,'すべての地域')+'</select><select name="industry" aria-label="業種">'+options(list(INDUSTRIES),industry,'すべての業種')+'</select><button type="submit">検索する →</button></form>'

def side():
    return '<aside><section class="panel side"><div class="eyebrow">READ THE SIGNAL</div><h2>速く知り、背景を読む。</h2><p>業種や詳細が未確認でも速報に掲載。確認できた情報を順次追記します。</p><a class="source" href="'+ROOT+'/signals">地域・業種の動きを見る →</a></section><section class="panel side"><h2>地域から探す</h2><div class="links">'+''.join('<a href="'+ROOT+'/area/'+quote(p)+'">'+e(p)+'</a>' for p in PREFECTURES)+'</div></section></aside>'


def kind_tabs(kind,path,prefecture='',industry='',q=''):
    links=[]
    for value,label in [('', 'すべて'),('bankruptcy','倒産'),('registration','新規法人')]:
        if path in {'/','/bankruptcies','/registrations'}:
            target={'':'/','bankruptcy':'/bankruptcies','registration':'/registrations'}[value]
            params={}
        elif path.startswith('/area/') or path.startswith('/industry/'):
            target=path;params={'kind':value} if value else {}
        else:
            target='/search';params={k:v for k,v in dict(kind=value,prefecture=prefecture,industry=industry,q=q).items() if v}
        href=ROOT+target+('?' + urlencode(params) if params else '')
        active=value==kind
        links.append('<a class="status-tab '+(value or 'all')+(' active' if active else '')+'" href="'+e(href,quote=True)+'"'+(' aria-current="page"' if active else '')+'>'+label+'</a>')
    return '<nav class="status-tabs" aria-label="企業情報の切り替え">'+''.join(links)+'</nav>'

def listing(title,kind='',prefecture='',industry='',q='',page=1,path='/',search=False):
    if kind not in {'','bankruptcy','registration'}:raise HTTPException(400,'Invalid kind')
    result=records(kind,prefecture,industry,q,page)
    intro='<div class="eyebrow">BUSINESS INTELLIGENCE / JAPAN</div><div class="hero"><div><h1>'+e(title).replace('・','・<wbr>')+'</h1><p>会社の変化を、いち早く。<br>倒産速報と新しい法人の情報を、地域・業種・出典から読み解く。</p></div><div class="live"><strong>●</strong>公開情報を自動収集</div></div>'
    if path=='/':
        a=analysis();intro+='<div class="metrics"><div class="metric"><span>直近30日・収集倒産事案</span><strong>'+str(a['bankruptcies'])+'</strong><span>名寄せによる参考件数</span></div><div class="metric"><span>直近30日・新規法人番号</span><strong>'+format(a['registrations'],',')+'</strong><span>収集済みの指定データ</span></div><div class="metric"><span>業種を分類できた事案</span><strong>'+str(a['classified'])+'</strong><span>本文に記載のある情報から分類</span></div></div>'
    if kind=='registration': intro+='<div class="notice">国税庁が新たに法人番号を指定した会社を掲載しています。指定日は設立日と一致するとは限りません。</div>'
    page_url=lambda n:ROOT+path+'?'+urlencode(dict(kind=kind,prefecture=prefecture,industry=industry,q=q,page=n))
    pages='<div class="pager">'+('<a href="'+e(page_url(page-1),quote=True)+'">← 前のページ</a>' if page>1 else '<span></span>')+('<a href="'+e(page_url(page+1),quote=True)+'">次のページ →</a>' if result['total']>page*40 else '')+'</div>'
    return shell(title,intro+kind_tabs(kind,path,prefecture,industry,q)+filters(kind,prefecture,industry,q)+'<div class="layout"><section class="panel"><h2>掲載情報 <span class="small">'+str(result['total'])+'件</span></h2>'+cards(result['items'])+pages+'</section>'+side()+'</div>',path+('?' + urlencode({**({'kind':kind} if kind and (path.startswith('/area/') or path.startswith('/industry/')) else {}),**({'page':page} if page>1 else {})}) if page>1 or (kind and (path.startswith('/area/') or path.startswith('/industry/'))) else ''),noindex=search or not result['items'])

@app.get('/health')
def health():return {'ok':True,'database_configured':bool(DSN),'hosting':'shared','news_enrichment_version':2,'reference_enrichment_version':2}
@app.get('/ready')
def ready():query('SELECT 1 FROM corporate_events LIMIT 1');return {'ready':True}
@app.get('/')
def home(page:int=Query(1,ge=1,le=10000)):return listing('企業倒産・新規法人情報サイト',page=page)
@app.get('/bankruptcies')
def bankruptcies(page:int=Query(1,ge=1,le=10000)):return listing('倒産速報',kind='bankruptcy',page=page,path='/bankruptcies')
@app.get('/registrations')
def registrations(page:int=Query(1,ge=1,le=10000)):return listing('新設・新規法人情報',kind='registration',page=page,path='/registrations')
@app.get('/search')
def search(kind:str='',prefecture:str='',industry:str='',q:str=Query('',max_length=100),page:int=Query(1,ge=1,le=10000)):return listing('企業情報を検索',kind,prefecture,industry,q,page,'/search',True)
@app.get('/area/{prefecture}')
def area(prefecture:str,page:int=Query(1,ge=1,le=10000),kind:str=''):
    if prefecture not in PREFECTURES:raise HTTPException(404)
    return listing(prefecture+'の企業の倒産、新規法人情報',kind=kind,prefecture=prefecture,page=page,path='/area/'+quote(prefecture))
@app.get('/industry/{industry}')
def industry_page(industry:str,page:int=Query(1,ge=1,le=10000),kind:str=''):
    if industry not in INDUSTRIES:raise HTTPException(404)
    return listing(industry+'の企業情報',kind=kind,industry=industry,page=page,path='/industry/'+quote(industry))
@app.get('/company/{event_id}')
def detail(event_id:str):
    rows=query('SELECT id,payload FROM corporate_events WHERE id=%s AND published',[event_id])
    if not rows or rows[0]['payload']['company'] in {'運営会社','老舗','同社','会社','企業','事業者','飲食店','店舗'}:raise HTTPException(404)
    p=rows[0]['payload']; title=p['company']+'｜'+p['stage']
    facts=[('状況',p['stage']), (p.get('date_label','確認日'),p['reported_date']),('都道府県',p.get('prefecture') or '未確認'),('業種',p.get('industry') or '未確認'),('所在地',p.get('address') or '未確認'),('法人番号',p.get('corporate_number') or '未照合'),('業種の確認方法',p.get('classification_basis') or '確認できる情報なし')]
    web_profile=p.get('web_profile',{})
    profile=web_profile if web_profile.get('verification_status')!='reference' else {}
    if profile:
        facts.extend([('公式サイト記載の事業分野','、'.join(profile.get('business_tags') or profile.get('industries',[])) or '分類未確認'),('公式サイトの照合方法',profile['match_basis']),('公式サイト確認日',profile['checked_at'][:10])])
    web_html=''
    if profile:
        web_html='<h2>公式サイトから確認した情報</h2><p><a class="source" href="'+e(profile['website_url'],quote=True)+'" target="_blank" rel="noopener noreferrer">公式サイトを確認する ↗</a></p><p class="small">公式サイトに残る事業情報です。現在の営業継続を示すものではありません。</p><div class="links">'+''.join('<a href="'+e(url,quote=True)+'" target="_blank" rel="noopener noreferrer">確認元 '+str(i+1)+' ↗</a>' for i,url in enumerate(profile['evidence_urls']))+'</div>'
        web_html+='<dl class="facts">'+''.join('<dt>'+e(d['label'])+'</dt><dd>'+e(d['value'])+' <a class="source" href="'+e(d['source_url'],quote=True)+'" target="_blank" rel="noopener noreferrer">出典 ↗</a></dd>' for d in profile.get('details',[]))+'</dl>'
    web_html+=render_reference(p.get('web_reference_profile') or web_profile,ROOT+'/company/'+event_id+'/correction')
    web_html+=render_reports(p.get('news_reports',[]))
    web_html+='<p class="small"><a href="'+ROOT+'/company/'+event_id+'/correction">掲載情報の修正依頼はこちら</a></p>'
    facts_html='<dl class="facts">'+''.join('<dt>'+e(k)+'</dt><dd>'+e(v)+'</dd>' for k,v in facts)+'</dl>'
    paragraph=e(p['company'])+'について、公開情報を整理しました。'
    if p['kind']=='bankruptcy':paragraph+=' 手続きの状況は「'+e(p['stage'])+'」です。報道時点の情報のため、その後の変更は出典でもご確認ください。'
    else:paragraph+=' 表示日は法人番号の指定日です。設立年月日を確認した情報ではありません。'
    causes=p.get('causes',[])
    note=('<h2>出典に記載された背景</h2><p>'+e('、'.join(causes))+'に関する記述を検出しました。記事の語句から自動抽出した項目で、影響の大きさや因果関係を評価したものではありません。</p>') if causes else '<p class="small">背景要因は確認できていません。確認できない原因を推測して掲載しません。</p>'
    search_url='https://www.google.com/maps/search/?api=1&query='+quote(p['company']+' '+(p.get('address') or p.get('prefecture','')))
    return shell(title,'<div class="detail"><a class="small" href="'+ROOT+'/">ホーム / 企業情報</a><div class="panel"><div class="eyebrow">COMPANY REPORT</div><h1>'+e(p['company'])+'</h1><span class="tag '+p['kind']+'">'+e(p['stage'])+'</span><p>'+paragraph+'</p>'+facts_html+web_html+note+'<p><a class="source" href="'+e(p['source_url'],quote=True)+'" target="_blank" rel="noopener noreferrer">出典：'+e(p['source_name'])+'で確認する ↗</a></p><h2>関連情報を調べる</h2><p><a class="source" href="'+search_url+'" target="_blank" rel="noopener noreferrer">会社名・所在地でGoogleマップを検索 ↗</a></p><p class="small">検索結果のリンクです。同名企業との一致、営業状況、口コミは未確認です。</p></div></div>','/company/'+event_id)
@app.get('/signals')
def signals():
    a=analysis();body='<div class="eyebrow">REGIONAL SIGNALS</div><h1>地域・業種の動き</h1><p>同じ地域・業種で、どのような事案が報道されているか。</p><div class="notice">全国の倒産統計や個別企業の信用評価ではありません。当サイトが収集し、会社名と地域で仮に名寄せした事案の参考集計です。業種不明の事案は分類別集計から除外します。</div>'
    body+='<p class="small">比較方法：直近30日とその前の30日を比較。収集開始から60日以上、両期間5件以上の場合のみ判定し、1.5倍以上を「増加傾向」とします。</p>'
    for g in a['groups']:
        body+='<section class="panel side"><span class="tag">'+e(g['status'])+'</span><h2>'+e(g['prefecture']+' × '+g['industry'])+'</h2><p>直近30日の収集事案：<strong>'+str(g['current'])+'件</strong>。'+('前の30日：'+str(g['previous'])+'件。' if a['enough'] else '比較に必要な期間の情報を蓄積しています。')+'</p>'
        if g['causes']:body+='<p>出典で言及された項目：'+e('、'.join(c+'（'+str(n)+'事案）' for c,n in g['causes']))+'。記事中の記載を整理したもので、地域全体の原因を断定するものではありません。</p>'
        body+='<div class="links">'+''.join('<a href="'+e(s['url'],quote=True)+'" rel="noopener noreferrer" target="_blank">'+e(s['name'])+'：出典 ↗</a>' for s in g['sources'])+'</div></section>'
    if not a['groups']:body+='<div class="panel"><p>地域と業種を確認できる事案を蓄積しています。</p></div>'
    return shell('地域・業種の動き',body,'/signals')
@app.get('/about')
def about():return shell('掲載方針・訂正について','<div class="detail panel"><h1>掲載方針</h1><h2>速報と確認済み情報</h2><p>倒産関連の報道から会社名・地域・手続きなどの事実を整理します。業種不明でも速報に掲載し、不明項目は未確認と表示します。破産申請の準備、手続きの開始、民事再生などは同一の状況として扱いません。</p><h2>新設・新規法人</h2><p>国税庁法人番号公表サイトの差分データを加工して掲載しています。「新規」は法人番号の新規指定を意味し、設立日を保証しません。登記閉鎖を倒産と分類することはありません。</p><h2>集計・背景の読み方</h2><p>同じ会社名と地域の報道を仮に名寄せした参考値です。同名企業や続報の扱いにより誤差が生じます。全国を網羅した統計ではありません。業種・背景要因は記事の記載からルールで抽出し、推測による因果関係は追加しません。</p><h2>関連サイトの情報</h2><p>公式サイトに加え、企業情報サイトや記事を収集し、会社名・所在地等を照合して事業情報を追記します。出典が一つでも照合できた情報は「参考情報（未確定）」として掲載します。本人・法人への確認は行っていません。参考情報の業種は確定情報の集計に混ぜません。確認元と確認日を表示し、複数事業がある場合は主業種を推測しません。Googleマップは検索へのリンクです。口コミ・写真・営業状態の取得や、掲載企業との照合は行っていません。</p><h2 id="corrections">訂正・削除のご連絡</h2><p>各企業の詳細ページにある「掲載情報の修正依頼はこちら」から、訂正すべき箇所と根拠をお知らせください。受付内容は一般公開しません。秘密情報や個人の連絡先は記載しないでください。</p></div>','/about')
@app.get('/sources')
def sources():
    rows=query("SELECT * FROM corporate_runs WHERE source NOT LIKE %s ORDER BY source",['search-%'])
    body='<h1>自動収集の状況</h1><div class="panel"><table><tr><th>収集元</th><th>最終確認（UTC）</th><th>状況</th></tr>'+''.join('<tr><td>'+e(r['source'])+'</td><td>'+e(str(r['checked_at'])[:19])+'</td><td>'+e({'ok':'正常','error':'取得失敗','partial':'一部の情報を取得できませんでした','deferred':'検索枠の回復待ち'}.get(r['status'],r['status']))+'</td></tr>' for r in rows)+'</table><p>倒産速報は30分ごとに確認、新規法人は国税庁の日次公表データを確認します。公開元や実行基盤の状況によって遅れる場合があります。</p></div>'
    return shell('収集状況',body,'/sources',True)
@app.get('/api/events')
def api_events(kind:str='',prefecture:str='',industry:str='',q:str=Query('',max_length=100),page:int=Query(1,ge=1,le=10000)):return records(kind,prefecture,industry,q,page)
@app.get('/api/signals')
def api_signals():return analysis()
@app.get('/api/collection-state')
def collection_state(request:Request):
    authorize(request)
    runs=query('SELECT source,status,detail,checked_at FROM corporate_runs')
    news=next((x for x in runs if x['source']=='JC-NET'),None)
    nta=next((x for x in runs if x['source']=='国税庁'),None)
    known={}
    for r in query("SELECT payload FROM corporate_events WHERE kind='bankruptcy' AND (reported_date<CURRENT_DATE-7 OR updated_at>now()-interval '24 hours') ORDER BY reported_date DESC LIMIT 5000"):
        p=r['payload'];known[p['source_url']]=p.get('listing_fingerprint','')
    return {'news':known,'nta_files':nta['detail'].split(',') if nta and nta['status']=='ok' else []}

@app.post('/api/ingest')
async def ingest(request:Request):
    authorize(request)
    raw=await request.body()
    if len(raw)>2_000_000:raise HTTPException(413)
    try:
        body=json.loads(raw);rows=body.get('items',[])
        if not isinstance(rows,list) or len(rows)>500:raise ValueError('batch limit')
        for row in rows:validate(row)
    except (ValueError,TypeError,KeyError):raise HTTPException(422,'Invalid collection data')
    con=db(); changed=0
    try:
        with con,con.cursor() as cur:
            cur.execute('SELECT id,payload FROM corporate_events WHERE id=ANY(%s)',[[identity(r) for r in rows]])
            existing=dict(cur.fetchall())
            rows=[preserve_profile(r,existing.get(identity(r))) for r in rows]
            for row in rows:
                row['entity_key']=('n'+row['corporate_number']) if row.get('corporate_number') else (normalized_name(row['company'])+'|'+row['prefecture'] if row.get('prefecture') else identity(row))
                cur.execute('''INSERT INTO corporate_events(id,kind,company,corporate_number,prefecture,industry,stage,reported_date,payload) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(id) DO UPDATE SET company=EXCLUDED.company,prefecture=EXCLUDED.prefecture,industry=EXCLUDED.industry,stage=EXCLUDED.stage,payload=EXCLUDED.payload,updated_at=now() WHERE corporate_events.payload IS DISTINCT FROM EXCLUDED.payload''', [identity(row),row['kind'],row['company'],row.get('corporate_number',''),row.get('prefecture',''),row.get('industry',''),row['stage'],row['reported_date'],Json(row)])
                changed+=cur.rowcount
            updates=body.get('registry_updates',[])
            if not isinstance(updates,list) or len(updates)>500:raise HTTPException(422,'Update batch limit')
            for change in updates:
                import re
                number=change.get('corporate_number','')
                if not re.fullmatch(r'\d{13}',number) or not isinstance(change.get('published'),bool):raise HTTPException(422,'Invalid registry update')
                for key in ['company','prefecture','address']:
                    if not isinstance(change.get(key),str) or len(change[key])>1000:raise HTTPException(422,'Invalid registry field')
                patch={key:change[key] for key in ['company','prefecture','address']}
                cur.execute("""UPDATE corporate_events SET payload=payload-'web_profile'-'web_checked_at',industry='',updated_at=now()
                    WHERE id=%s AND (company<>%s OR payload->>'address' IS DISTINCT FROM %s)""",['n'+number,change['company'],change['address']])
                cur.execute("UPDATE corporate_events SET payload=payload || '{\"industry\":\"\",\"classification_basis\":\"\"}'::jsonb WHERE id=%s AND NOT (payload ? 'web_profile') AND payload->>'classification_basis'='公式サイトの事業内容から自動分類'",['n'+number])
                cur.execute('''UPDATE corporate_events SET company=%s,prefecture=%s,payload=payload || %s,published=%s,updated_at=now()
                 WHERE id=%s AND kind='registration' AND (payload IS DISTINCT FROM payload || %s OR published IS DISTINCT FROM %s)''', [change['company'],change['prefecture'],Json(patch),change['published'],'n'+number,Json(patch),change['published']])
            for status in body.get('sources',[]):
                if status.get('source') not in {'JC-NET','国税庁','公式サイト補完','ニュース補完'}:raise HTTPException(422,'Invalid source')
                cur.execute('INSERT INTO corporate_runs(source,status,received,detail) VALUES(%s,%s,%s,%s) ON CONFLICT(source) DO UPDATE SET checked_at=now(),status=EXCLUDED.status,received=EXCLUDED.received,detail=EXCLUDED.detail',[status['source'],status['status'],status.get('received',0),status.get('detail','')[:200]])
    finally:con.close()
    with _lock:_cache.clear()
    return {'ok':True,'received':len(rows),'changed':changed}


@app.post('/api/search-permit')
def search_permit(request:Request):
    """Reserve before each paid-provider call. Failed calls still consume a reservation."""
    authorize(request)
    now=datetime.now(timezone.utc)
    limits={'search-month:'+now.strftime('%Y-%m'):900,'search-day:'+now.strftime('%Y-%m-%d'):30}
    con=db()
    try:
        with con,con.cursor() as cur:
            for key in sorted(limits):
                cur.execute("INSERT INTO corporate_runs(source,status,received,detail) VALUES(%s,'ok',0,'search reservation') ON CONFLICT(source) DO NOTHING",[key])
            cur.execute('SELECT source,received FROM corporate_runs WHERE source=ANY(%s) ORDER BY source FOR UPDATE',[sorted(limits)])
            counts=dict(cur.fetchall())
            if any(counts[k]>=limit for k,limit in limits.items()):
                return {'allowed':False,'reason':'search quota reached','monthly_limit':900,'daily_limit':30}
            cur.execute('UPDATE corporate_runs SET received=received+1,checked_at=now() WHERE source=ANY(%s)',[sorted(limits)])
            return {'allowed':True,'monthly_limit':900,'daily_limit':30}
    finally:con.close()

@app.get('/api/news-candidates')
def news_candidates(request:Request):
    authorize(request)
    rows=query("SELECT id,payload FROM corporate_events WHERE published AND kind='bankruptcy' AND (COALESCE(payload->>'news_version','0')<>'2' OR COALESCE(payload->>'news_checked_at','')='' OR (payload->>'news_checked_at')::timestamptz<now()-CASE WHEN payload->>'news_check_status'='partial' THEN interval '1 day' ELSE interval '7 days' END) ORDER BY CASE WHEN id=%s THEN 0 ELSE 1 END, COALESCE(payload->>'news_checked_at',''),reported_date DESC,id LIMIT 2",['bfb9524de141817310bcabbb4'])
    return {'items':rows}

@app.post('/api/news-enrichment')
async def save_news_enrichment(request:Request):
    authorize(request)
    raw=await request.body()
    if len(raw)>60_000:raise HTTPException(413)
    try:
        body=json.loads(raw)
        if not isinstance(body['id'],str) or not isinstance(body['entity'],dict):raise ValueError()
        reports=validate_reports(body['reports'])
        if any(r['match_basis']=='速報の出典記事' and r['url']!=body['entity'].get('source_url') for r in reports):raise ValueError()
        if not isinstance(body.get('complete',False),bool):raise ValueError()
    except (ValueError,KeyError,TypeError,AttributeError):raise HTTPException(422,'Invalid news enrichment')
    con=db();changed=0
    try:
        with con,con.cursor() as cur:
            cur.execute("SELECT payload FROM corporate_events WHERE id=%s AND published AND kind='bankruptcy' FOR UPDATE",[body['id']])
            hit=cur.fetchone()
            if hit and same_entity(hit[0],body['entity']):
                p=hit[0];merged={r['url']:r for r in p.get('news_reports',[])}
                merged.update({r['url']:r for r in reports})
                p['news_reports']=sorted(merged.values(),key=lambda r:(r['published_date'],r['url']),reverse=True)[:8]
                p['news_checked_at']=datetime.now(timezone.utc).isoformat(timespec='seconds')
                p['news_check_status']='ok' if body.get('complete') else 'partial'
                p['news_version']=2
                primary=[r for r in reports if r['url']==p['source_url'] and r['match_basis']=='速報の出典記事']
                if not p.get('industry') and len(primary)==1:
                    tags=primary[0]['fields'].get('business','').split('、')
                    mapping={'広告事業':'サービス業','デジタルマーケティング':'サービス業','飲食店運営':'飲食業','宿泊施設運営':'宿泊業','建設業':'建設業','製造業':'製造業','運送業':'運輸業','システム開発':'情報通信業'}
                    industries={mapping[t] for t in tags if t in mapping}
                    if len(industries)==1:
                        p['industry']=industries.pop();p['classification_basis']='速報の出典記事に記載された事業から自動分類'
                cur.execute('UPDATE corporate_events SET payload=%s,industry=%s,updated_at=CASE WHEN %s THEN now() ELSE updated_at END WHERE id=%s',[Json(p),p.get('industry',''),bool(reports),body['id']])
                changed=len(reports)
    finally:con.close()
    with _lock:_cache.clear()
    return {'ok':True,'changed':changed}

@app.get('/api/enrichment-candidates')
def enrichment_candidates(request:Request,search_enabled:bool=False):
    authorize(request)
    eligible="((jsonb_array_length(COALESCE(payload->'website_candidates','[]'::jsonb))>0 OR jsonb_array_length(COALESCE(payload->'news_reports','[]'::jsonb))>0 OR payload ? 'web_profile')"+(" OR kind IN ('registration','bankruptcy')" if search_enabled else '')+")"
    rows=query("SELECT id,payload FROM corporate_events WHERE published AND "+eligible+" AND (COALESCE(payload->>'web_version','')<>'2' OR COALESCE(payload->>'web_checked_at','')='' OR (payload->>'web_checked_at')::timestamptz<now()-interval '7 days') ORDER BY CASE WHEN jsonb_array_length(COALESCE(payload->'website_candidates','[]'::jsonb))>0 OR payload ? 'web_profile' THEN 0 ELSE 1 END, CASE WHEN kind='bankruptcy' THEN 0 ELSE 1 END, COALESCE(payload->>'web_checked_at',''),reported_date DESC,id LIMIT 6")
    return {'items':rows}

@app.post('/api/enrichment')
async def save_enrichment(request:Request):
    authorize(request)
    raw=await request.body()
    if len(raw)>100_000:raise HTTPException(413)
    try:
        body=json.loads(raw);items=body['items']
        if not isinstance(items,list) or len(items)>10:raise ValueError()
        for item in items:
            if not isinstance(item.get('id'),str) or not isinstance(item.get('entity'),dict):raise ValueError()
            profile=item.get('profile')
            if profile:
                from urllib.parse import urlsplit
                urls=[profile['website_url']]+profile['evidence_urls']
                if not 1<=len(urls)<=5:raise ValueError()
                for url in urls:
                    p=urlsplit(url)
                    if p.scheme not in {'http','https'} or not p.hostname or p.username or p.password or len(url)>2000:raise ValueError()
                if profile['primary_industry'] not in list(INDUSTRIES)+['']:raise ValueError()
                if not isinstance(profile['industries'],list) or any(x not in INDUSTRIES for x in profile['industries']):raise ValueError()
                if not isinstance(profile['business_tags'],list) or len(profile['business_tags'])>10 or any(not isinstance(x,str) or len(x)>100 for x in profile['business_tags']):raise ValueError()
                if profile['match_basis'] not in {'会社名・法人番号一致','会社名・所在地一致','会社名・公表所在地の範囲一致','報道元の公式サイトリンク・会社名・都道府県一致'}:raise ValueError()
                details=profile.get('details',[])
                if not isinstance(details,list) or len(details)>6:raise ValueError()
                for detail in details:
                    if detail['label'] not in {'代表者','店舗・ブランド'} or not isinstance(detail['value'],str) or len(detail['value'])>100 or detail['source_url'] not in profile['evidence_urls']:raise ValueError()
                if profile.get('verification_status','official') not in {'official','reference'}:raise ValueError()
                if not isinstance(profile.get('source_title',''),str) or len(profile.get('source_title',''))>200:raise ValueError()
                profile['checked_at']=datetime.now(timezone.utc).isoformat(timespec='seconds')
    except (ValueError,KeyError,TypeError,AttributeError):raise HTTPException(422,'Invalid enrichment')
    con=db();changed=0
    try:
        with con,con.cursor() as cur:
            for item in items:
                cur.execute('SELECT payload FROM corporate_events WHERE id=%s AND published FOR UPDATE',[item['id']])
                hit=cur.fetchone()
                if not hit or not same_entity(hit[0],item['entity']):continue
                p=hit[0];profile=item.get('profile')
                p['web_checked_at']=datetime.now(timezone.utc).isoformat(timespec='seconds');p['web_version']=2
                if profile and profile.get('verification_status')=='reference' and p.get('web_profile') and p['web_profile'].get('verification_status')!='reference':
                    p['web_reference_profile']=profile
                if profile and not (profile.get('verification_status')=='reference' and p.get('web_profile') and p['web_profile'].get('verification_status')!='reference'):
                    p['web_profile']=profile
                    if profile.get('verification_status')!='reference' and (not p.get('industry') or p.get('classification_basis')=='公式サイトの事業内容から自動分類'):
                        p['industry']=profile['primary_industry'];p['classification_basis']='公式サイトの事業内容から自動分類' if p['industry'] else ''
                cur.execute('UPDATE corporate_events SET payload=%s,industry=%s,updated_at=CASE WHEN %s THEN now() ELSE updated_at END WHERE id=%s',[Json(p),p.get('industry',''),bool(profile),item['id']])
                changed+=bool(profile)
    finally:con.close()
    with _lock:_cache.clear()
    return {'ok':True,'changed':changed,'checked':len(items)}

@app.get('/robots.txt')
def robots():return Response('User-agent: *\nAllow: /\nSitemap: '+BASE+'/sitemap.xml\n',media_type='text/plain')
@app.get('/sitemap.xml')
def sitemap():
    n=query("SELECT count(*) n FROM corporate_events WHERE published AND company NOT IN ('運営会社','老舗','同社','会社','企業','事業者','飲食店','店舗')")[0]['n']
    pages=max(1,(n+9999)//10000)
    body='<?xml version="1.0" encoding="UTF-8"?><sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
    for page in range(1,pages+1):body+='<sitemap><loc>'+e(BASE+'/sitemaps/'+str(page)+'.xml')+'</loc></sitemap>'
    return Response(body+'</sitemapindex>',media_type='application/xml',headers={'Cache-Control':'public, max-age=300'})
@app.get('/sitemaps/{page}.xml')
def sitemap_page(page:int):
    if page<1 or page>10000:raise HTTPException(404)
    rows=query("SELECT id,updated_at FROM corporate_events WHERE published AND company NOT IN ('運営会社','老舗','同社','会社','企業','事業者','飲食店','店舗') ORDER BY id LIMIT 10000 OFFSET %s",[(page-1)*10000])
    if not rows and page>1:raise HTTPException(404)
    body='<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
    if page==1:
        for path in ['/','/bankruptcies','/registrations','/signals','/about']:body+='<url><loc>'+e(BASE+path)+'</loc></url>'
        for p in query("SELECT DISTINCT prefecture FROM corporate_events WHERE published AND company NOT IN ('運営会社','老舗','同社','会社','企業','事業者','飲食店','店舗') AND prefecture<>''"):
            body+='<url><loc>'+e(BASE+'/area/'+quote(p['prefecture']))+'</loc></url>'
        for p in query("SELECT DISTINCT industry FROM corporate_events WHERE published AND company NOT IN ('運営会社','老舗','同社','会社','企業','事業者','飲食店','店舗') AND industry<>''"):
            body+='<url><loc>'+e(BASE+'/industry/'+quote(p['industry']))+'</loc></url>'
    for r in rows:body+='<url><loc>'+e(BASE+'/company/'+r['id'])+'</loc><lastmod>'+r['updated_at'].date().isoformat()+'</lastmod></url>'
    return Response(body+'</urlset>',media_type='application/xml',headers={'Cache-Control':'public, max-age=300'})


from .corrections import install as install_corrections
install_corrections(app,db,query,shell,BASE,ROOT,DSN,lambda request:authorize(request))
