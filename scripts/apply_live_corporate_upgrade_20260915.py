from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]


def rep(path,old,new,required=True):
    p=ROOT/path;text=p.read_text()
    if new in text:return False
    if old not in text:
        if required:raise SystemExit(f'missing target {path}: {old[:100]!r}')
        return False
    p.write_text(text.replace(old,new,1));return True

# gBizINFO-first reference enrichment, no extra paid-search call required.
rep(Path('services/corporate/enrich.py'),
    "DIRECTORIES={'houjin.jp','cnavi.g-search.or.jp','salesnow.jp','biz-maps.com','tsukulink.net','companydata.tsujigawa.com','baseconnect.in','newsdig.tbs.co.jp'}",
    "DIRECTORIES={'houjin.jp','info.gbiz.go.jp','cnavi.g-search.or.jp','salesnow.jp','biz-maps.com','tsukulink.net','companydata.tsujigawa.com','baseconnect.in','newsdig.tbs.co.jp'}")
rep(Path('services/corporate/enrich.py'),
    "(?i:URL|HP|ホームページ|ウェブサイト|公式サイト|企業サイト|会社HP|企業URL|ホームページURL|Webサイト)",
    "(?i:URL|HP|ホームページ|企業ホームページ|ウェブサイト|公式サイト|企業サイト|会社HP|企業URL|ホームページURL|Webサイト)")
rep(Path('services/corporate/enrich.py'),
    "    if re.fullmatch(r'\\d{13}',row.get('corporate_number','')):\n        urls.append('https://houjin.jp/c/'+row['corporate_number'])\n",
    "    if re.fullmatch(r'\\d{13}',row.get('corporate_number','')):\n        urls.append('https://info.gbiz.go.jp/hojin/ichiran?hojinBango='+row['corporate_number'])\n        urls.append('https://houjin.jp/c/'+row['corporate_number'])\n")
rep(Path('services/corporate/enrich.py'),'    return result[:4]\n\ndef search_candidates','    return result[:5]\n\ndef search_candidates')

# Model accepts secondary open-news bankruptcy sources and official public-risk rows.
rep(Path('services/corporate/model.py'),
    "def identity(row):\n    if row['kind']=='registration': return 'n'+row['corporate_number']\n    # Without an exact corporate number, use source URL: do not silently merge namesakes.\n    return 'b'+hashlib.sha256(row['source_url'].encode()).hexdigest()[:24]\n",
    "def identity(row):\n    if row['kind']=='registration': return 'n'+row['corporate_number']\n    if row['kind']=='risk': return 'r'+hashlib.sha256(row['source_url'].encode()).hexdigest()[:24]\n    # Without an exact corporate number, use source URL: do not silently merge namesakes.\n    return 'b'+hashlib.sha256(row['source_url'].encode()).hexdigest()[:24]\n")
rep(Path('services/corporate/model.py'),"    if row.get('kind') not in {'bankruptcy','registration'}: raise ValueError('invalid kind')","    if row.get('kind') not in {'bankruptcy','registration','risk'}: raise ValueError('invalid kind')")
rep(Path('services/corporate/model.py'),
    "    host=urlparse(row['source_url']).hostname\n    if host not in {'n-seikei.jp','www.n-seikei.jp','www.houjin-bangou.nta.go.jp'}: raise ValueError('source host')\n",
    "    parsed=urlparse(row['source_url']);host=(parsed.hostname or '').lower()\n    if parsed.scheme not in {'http','https'} or not host: raise ValueError('source host')\n    if row['kind']=='registration' and host!='www.houjin-bangou.nta.go.jp': raise ValueError('source host')\n    if row['kind']=='bankruptcy':\n        jc=host in {'n-seikei.jp','www.n-seikei.jp'}\n        open_news=row.get('discovery_channel')=='GDELT' and parsed.scheme=='https' and host not in {'news.google.com','google.com','www.google.com','search.yahoo.co.jp','www.tdb.co.jp','tdb.co.jp','www.tsr-net.co.jp','tsr-net.co.jp'}\n        if not (jc or open_news): raise ValueError('source host')\n    if row['kind']=='risk' and host not in {'www.mlit.go.jp','mlit.go.jp','jsite.mhlw.go.jp','www.caa.go.jp','caa.go.jp','www.fsa.go.jp','fsa.go.jp'}: raise ValueError('source host')\n")
rep(Path('services/corporate/model.py'),"    if '特別清算' in title: return '特別清算（報道）'\n    return ''\n","    if '特別清算' in title: return '特別清算（報道）'\n    if '会社更生' in title: return '会社更生（報道）'\n    return ''\n")
rep(Path('services/corporate/model.py'),"def parse_news(title, body, url, published, company_hint=''):\n","def parse_news(title, body, url, published, company_hint='', source_name='JC-NET', discovery_channel=''):\n")
rep(Path('services/corporate/model.py'),"    m=re.search(r'^(.{2,100}?)(?:が|に(?:対し)?|、|／|\\s[/／]\\s)(?:.*?)(?:破産|民事再生|特別清算)',t)\n","    m=re.search(r'^(.{2,100}?)(?:が|に(?:対し)?|、|／|\\s[/／]\\s)(?:.*?)(?:破産|民事再生|特別清算|会社更生)',t)\n")
p=ROOT/'services/corporate/model.py';text=p.read_text()
text=text.replace("source_name='JC-NET',source_url=url","source_name=source_name,source_url=url",1)
mark="    event_date,event_label=proceeding_date(body,published)\n"
if mark in text and "if discovery_channel: row['discovery_channel']=discovery_channel" not in text:
    text=text.replace(mark,"    if discovery_channel: row['discovery_channel']=discovery_channel\n"+mark,1)
text=text.replace("for key in ('news_reports','news_checked_at','news_check_status','news_version'):","for key in ('news_reports','news_checked_at','news_check_status','news_version','discovery_sources'):",1)
p.write_text(text)

# Live UI/API: public-risk tab; merge secondary bankruptcy evidence into the existing case page.
p=ROOT/'services/corporate/main.py';text=p.read_text()
text=text.replace("<a href=\"'+ROOT+'/registrations\">新設・新規法人</a><a href=\"'+ROOT+'/signals\">地域・業種の動き</a>","<a href=\"'+ROOT+'/registrations\">新設・新規法人</a><a href=\"'+ROOT+'/risks\">公表リスク情報</a><a href=\"'+ROOT+'/signals\">地域・業種の動き</a>",1)
text=text.replace("label='倒産速報' if kind=='bankruptcy' else '新規法人'","label='倒産速報' if kind=='bankruptcy' else '公表情報' if kind=='risk' else '新規法人'",1)
text=text.replace("for value,label in [('', 'すべて'),('bankruptcy','倒産'),('registration','新規法人')]:","for value,label in [('', 'すべて'),('bankruptcy','倒産'),('registration','新規法人'),('risk','公表リスク')]:",1)
text=text.replace("if path in {'/','/bankruptcies','/registrations'}:","if path in {'/','/bankruptcies','/registrations','/risks'}:",1)
text=text.replace("target={'':'/','bankruptcy':'/bankruptcies','registration':'/registrations'}[value]","target={'':'/','bankruptcy':'/bankruptcies','registration':'/registrations','risk':'/risks'}[value]",1)
text=text.replace("if kind not in {'','bankruptcy','registration'}:raise HTTPException(400,'Invalid kind')","if kind not in {'','bankruptcy','registration','risk'}:raise HTTPException(400,'Invalid kind')",1)
text=text.replace("topic=('倒産速報' if kind=='bankruptcy' else '新規法人情報' if kind=='registration' else '企業の倒産・新規法人情報')","topic=('倒産速報' if kind=='bankruptcy' else '新規法人情報' if kind=='registration' else '公表リスク情報' if kind=='risk' else '企業の倒産・新規法人情報')",1)
route="@app.get('/registrations')\ndef registrations(page:int=Query(1,ge=1,le=10000)):return listing('新設・新規法人情報',kind='registration',page=page,path='/registrations')\n"
if "@app.get('/risks')" not in text:
    if route not in text:raise SystemExit('risk route insertion target missing')
    text=text.replace(route,route+"@app.get('/risks')\ndef risks(page:int=Query(1,ge=1,le=10000)):return listing('公表リスク情報',kind='risk',page=page,path='/risks')\n",1)
text=text.replace("for path in ['/','/bankruptcies','/registrations','/signals','/about']","for path in ['/','/bankruptcies','/registrations','/risks','/signals','/about']",1)
text=text.replace("{'JC-NET','国税庁','公式サイト補完','ニュース補完'}","{'JC-NET','国税庁','公式サイト補完','ニュース補完','公開報道探索','国交省公表情報'}",1)
# Detail date handling and copy for risk.
text=text.replace("    if p['kind']=='bankruptcy':\n        if p.get('event_date'):facts.append((p.get('event_date_label') or '手続日',p['event_date']))\n        facts.append(('報道日',p['reported_date']))\n    else:facts.append((p.get('date_label','確認日'),p['reported_date']))\n",
                  "    if p['kind']=='bankruptcy':\n        if p.get('event_date'):facts.append((p.get('event_date_label') or '手続日',p['event_date']))\n        facts.append(('報道日',p['reported_date']))\n    else:facts.append((p.get('date_label','確認日'),p['reported_date']))\n",1)
old="    else:paragraph+=' 表示日は法人番号の指定日です。設立年月日を確認した情報ではありません。'\n"
new="    elif p['kind']=='risk':paragraph+=' 行政機関等が公表した事実を整理したもので、当サイトによる信用評価・倒産予測ではありません。'\n    else:paragraph+=' 表示日は法人番号の指定日です。設立年月日を確認した情報ではありません。'\n"
if old not in text:raise SystemExit('detail paragraph target missing')
text=text.replace(old,new,1)
needle="    facts_html='<dl class=\"facts\">'+''.join('<dt>'+e(k)+'</dt><dd>'+e(v)+'</dd>' for k,v in facts)+'</dl>'\n"
if "<h2>公表された内容</h2>" not in text:
    if needle not in text:raise SystemExit('facts target missing')
    text=text.replace(needle,needle+"    if p['kind']=='risk':facts_html+='<h2>公表された内容</h2><dl class=\"facts\"><dt>区分</dt><dd>'+e(p.get('risk_type') or '公表情報')+'</dd><dt>公表内容</dt><dd>'+e(p.get('risk_summary') or p['stage'])+'</dd><dt>公表機関</dt><dd>'+e(p.get('risk_agency') or p['source_name'])+'</dd></dl><p class=\"small\">公的機関等の公表内容を整理したものです。当サイトが企業の信用状態を評価・予測したものではありません。</p>'\n",1)
text=text.replace("    topic='倒産速報' if p['kind']=='bankruptcy' else '新規法人情報'\n    topic_path='/bankruptcies' if p['kind']=='bankruptcy' else '/registrations'",
                  "    topic='倒産速報' if p['kind']=='bankruptcy' else '公表リスク情報' if p['kind']=='risk' else '新規法人情報'\n    topic_path='/bankruptcies' if p['kind']=='bankruptcy' else '/risks' if p['kind']=='risk' else '/registrations'",1)
# Risk shouldn't show registration wording in description.
text=text.replace("    if p['kind']=='registration': description+='表示日は法人番号指定日で、設立日とは限りません。'","    if p['kind']=='registration': description+='表示日は法人番号指定日で、設立日とは限りません。'",1)
# Merge one matching secondary bankruptcy report into existing case rather than duplicate.
loop="            for row in rows:\n                row['entity_key']=('n'+row['corporate_number']) if row.get('corporate_number') else (normalized_name(row['company'])+'|'+row['prefecture'] if row.get('prefecture') else identity(row))\n"
if "payload['discovery_sources']" not in text:
    if loop not in text:raise SystemExit('ingest loop target missing')
    repl="""            for row in rows:\n                fallback_key=(normalized_name(row['company'])+'|'+row['prefecture']) if row.get('prefecture') else ''\n                if row['kind']=='bankruptcy' and fallback_key:\n                    cur.execute(\"SELECT id,payload FROM corporate_events WHERE published AND kind='bankruptcy' AND payload->>'entity_key'=%s ORDER BY reported_date DESC LIMIT 2\",[fallback_key])\n                    matches=cur.fetchall()\n                    if len(matches)==1 and matches[0][0]!=identity(row):\n                        existing_id,payload=matches[0];sources=payload.get('discovery_sources',[])\n                        evidence={'source_name':row['source_name'],'source_url':row['source_url'],'reported_date':row['reported_date'],'stage':row['stage']}\n                        if not any(x.get('source_url')==row['source_url'] for x in sources):sources.append(evidence)\n                        payload['discovery_sources']=sources[-8:]\n                        if not payload.get('industry') and row.get('industry'):\n                            payload['industry']=row['industry'];payload['classification_basis']=row.get('classification_basis','')\n                        cur.execute('UPDATE corporate_events SET payload=%s,industry=%s,updated_at=now() WHERE id=%s',[Json(payload),payload.get('industry',''),existing_id]);changed+=cur.rowcount\n                        continue\n                row['entity_key']=('n'+row['corporate_number']) if row.get('corporate_number') else (fallback_key if fallback_key else identity(row))\n"""
    text=text.replace(loop,repl,1)
p.write_text(text)
print('live corporate upgrade patch applied')
