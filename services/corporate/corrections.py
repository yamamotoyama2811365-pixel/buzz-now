"""Small private correction inbox. No visitor submissions are auto-published."""
import hashlib
import hmac
import secrets
import time
from html import escape as e
from urllib.parse import parse_qs, urlsplit
from fastapi import HTTPException, Request


def install(app,db,query,shell,base,root,secret,authorize):
    key=hashlib.sha256(('corporate-corrections:'+secret).encode()).digest()

    def token(event_id):
        value=str(int(time.time()))+'.'+secrets.token_hex(12)
        return value+'.'+hmac.new(key,(event_id+':'+value).encode(),hashlib.sha256).hexdigest()

    def valid_token(event_id,value):
        try:
            timestamp,nonce,signature=value.split('.')
            age=time.time()-int(timestamp)
            expected=hmac.new(key,(event_id+':'+timestamp+'.'+nonce).encode(),hashlib.sha256).hexdigest()
            return 0<=age<86400 and hmac.compare_digest(expected,signature)
        except (ValueError,TypeError):return False

    def entity(event_id):
        rows=query('SELECT company FROM corporate_events WHERE id=%s AND published',[event_id])
        if not rows:raise HTTPException(404)
        return rows[0]['company']

    @app.get('/company/{event_id}/correction')
    def form(event_id:str):
        company=entity(event_id)
        path='/company/'+event_id+'/correction'
        body='<div class="detail panel"><h1>掲載情報の修正依頼</h1><p>対象：'+e(company)+'</p><p>訂正したい内容と、確認できる公開ページのURLなどをお知らせください。受付内容は一般公開しません。氏名・メールアドレスの入力は不要です。</p>'
        body+='<form method="post" action="'+e(root+path,quote=True)+'"><input type="hidden" name="token" value="'+token(event_id)+'"><label for="correction-details">修正内容・根拠（必須）</label><textarea id="correction-details" name="details" required minlength="10" maxlength="2000" rows="8" style="width:100%;margin:12px 0;padding:12px;font:inherit"></textarea><p class="small">公開したくない情報や個人の連絡先は入力しないでください。内容を確認してから掲載情報を修正します。</p><button type="submit">修正依頼を送信</button></form><p><a href="'+root+'/company/'+e(event_id,quote=True)+'">企業情報へ戻る</a></p></div>'
        response=shell('掲載情報の修正依頼',body,path,noindex=True)
        response.headers['Cache-Control']='no-store'
        return response

    @app.post('/company/{event_id}/correction')
    async def submit(event_id:str,request:Request):
        entity(event_id)
        origin=request.headers.get('origin','')
        expected=urlsplit(base)
        if origin and origin!=expected.scheme+'://'+expected.netloc:raise HTTPException(403)
        raw=bytearray()
        async for chunk in request.stream():
            raw.extend(chunk)
            if len(raw)>25000:raise HTTPException(413)
        try:data=parse_qs(raw.decode('utf-8'),max_num_fields=4)
        except (ValueError,UnicodeDecodeError):raise HTTPException(422)
        value=data.get('token',[''])[0];details=data.get('details',[''])[0].strip()
        if not valid_token(event_id,value):raise HTTPException(400,'フォームを開き直して送信してください。')
        if not 10<=len(details)<=2000:raise HTTPException(422,'修正内容を10～2000文字で入力してください。')
        submission_id=hashlib.sha256(value.encode()).hexdigest()
        con=db()
        try:
            with con,con.cursor() as cur:
                cur.execute('SELECT pg_advisory_xact_lock(72599107)')
                cur.execute('SELECT id FROM corporate_corrections WHERE id=%s',[submission_id])
                existing=cur.fetchone()
                if not existing:
                    cur.execute("SELECT count(*) FROM corporate_corrections WHERE created_at>now()-interval '1 day'")
                    if cur.fetchone()[0]>=100:raise HTTPException(429,'受付が混み合っています。時間をおいてお試しください。')
                    cur.execute('INSERT INTO corporate_corrections(id,event_id,details) VALUES (%s,%s,%s)',[submission_id,event_id,details])
        finally:con.close()
        response=shell('修正依頼を受け付けました','<div class="detail panel"><h1>修正依頼を受け付けました</h1><p>内容を確認してから掲載情報に反映します。</p><p>受付番号：'+submission_id[:12]+'</p><a href="'+root+'/company/'+e(event_id,quote=True)+'">企業情報へ戻る</a></div>','/company/'+event_id+'/correction',noindex=True)
        response.headers['Cache-Control']='no-store'
        return response

    @app.get('/api/corrections')
    def inbox(request:Request):
        authorize(request)
        return {'items':query("SELECT id,event_id,details,created_at,status FROM corporate_corrections WHERE status='pending' ORDER BY created_at LIMIT 100")}
