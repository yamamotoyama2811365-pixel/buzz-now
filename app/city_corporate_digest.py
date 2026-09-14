"""One regional roundup in the existing evening slot; no new posting slots."""
import hashlib
import json
import re
from collections import defaultdict
from datetime import date, timedelta
from html import escape
from urllib.parse import urlsplit

VERSION = 'regional-pilot-v2'
BASE = 'https://buzz-now-1.onrender.com/corporate'
KEY = 'citycorp_x:digest:'


BLOCKS = {
    '北海道': '北海道',
    '東北': '青森県 岩手県 宮城県 秋田県 山形県 福島県',
    '関東': '茨城県 栃木県 群馬県 埼玉県 千葉県 東京都 神奈川県',
    '中部': '新潟県 富山県 石川県 福井県 山梨県 長野県 岐阜県 静岡県 愛知県',
    '関西': '三重県 滋賀県 京都府 大阪府 兵庫県 奈良県 和歌山県',
    '中国': '鳥取県 島根県 岡山県 広島県 山口県',
    '四国': '徳島県 香川県 愛媛県 高知県',
    '九州・沖縄': '福岡県 佐賀県 長崎県 熊本県 大分県 宮崎県 鹿児島県 沖縄県',
}
PREF_BLOCK = {p: block for block, prefs in BLOCKS.items() for p in prefs.split()}
# Editorial grouping, not an administrative definition.
EAST = set(('北海道 '+BLOCKS['東北']+' '+BLOCKS['関東']+' 新潟県 山梨県 長野県 静岡県').split())


def select(rows, seen, today):
    levels = [defaultdict(list) for _ in range(4)]
    entities = set()
    for r in rows:
        p = r.get('payload') or {}
        pref = r.get('prefecture') or ''
        name = r.get('company') or ''
        stage = p.get('stage') or ''
        source = p.get('source_url') or ''
        identity = p.get('entity_key') or (pref, name)
        try:
            reported = date.fromisoformat(str(r['reported_date'])[:10])
            valid_url = urlsplit(source).scheme in ('http', 'https') and bool(urlsplit(source).hostname)
        except (ValueError, KeyError):
            continue
        if (pref not in PREF_BLOCK or not name.strip() or not stage.strip()
                or not valid_url or not today-timedelta(days=6) <= reported <= today
                or 'bankruptcy:'+str(r['id']) in seen or identity in entities):
            continue
        entities.add(identity)
        item = {'id':str(r['id']), 'company':name, 'stage':stage, 'prefecture':pref,
                'reported_date':reported.isoformat(), 'source_url':source,
                'source_name':p.get('source_name') or '出典記事'}
        for groups, area in zip(levels, (pref, PREF_BLOCK[pref], '東日本' if pref in EAST else '西日本', '全国')):
            groups[area].append(item)
    from .city_corporate_activation import weighted_length
    for level, groups in enumerate(levels):
        for area, items in sorted(groups.items(), key=lambda kv:(-len(kv[1]), kv[0])):
            minimum = 1 if level == 3 else 3
            if len(items) < minimum:
                continue
            items.sort(key=lambda r:(r['reported_date'], r['id']), reverse=True)
            for n in range(min(5, len(items)), minimum-1, -1):
                for style in ('full', 'names', 'summary'):
                    doc = {'prefecture':area, 'start':(today-timedelta(days=6)).isoformat(),
                           'end':today.isoformat(), 'items':items[:n], 'version':VERSION, 'style':style}
                    doc['id'] = hashlib.sha256(json.dumps(doc, ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:20]
                    if weighted_length(post_text(doc)) <= 280:
                        return doc
    return None


def post_text(doc):
    period = doc['start'][5:].replace('-', '/')+'〜'+doc['end'][5:].replace('-', '/')
    style = doc.get('style', 'full')
    if style == 'summary':
        facts = [str(len(doc['items']))+'社の公表情報をまとめました。', '会社名・公表日・手続き・出典を一覧で確認できます。']
    else:
        facts = [r['company']+('：'+r['stage'] if style == 'full' else '') for r in doc['items']]
    lines = [f"【{doc['prefecture']}｜企業の動き】{period}公表分",
             *facts, '各社の詳細・出典 ↓',
             BASE+'/roundup/'+doc['id']+'?utm_source=twitter&utm_medium=social&utm_campaign=citycorp_regional&utm_content='+doc['id']]
    return '\n'.join(lines)


def candidate(dsn, seen, today):
    if not dsn:
        return None
    import psycopg2
    from psycopg2.extras import RealDictCursor
    con = psycopg2.connect(dsn, connect_timeout=5)
    try:
        with con.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET LOCAL statement_timeout = '8s'")
            cur.execute("""SELECT id,company,prefecture,reported_date,payload FROM corporate_events
                WHERE published AND kind='bankruptcy'
                AND company NOT IN ('運営会社','老舗','同社','会社','企業','事業者','飲食店','店舗')
                AND reported_date BETWEEN %s AND %s
                AND COALESCE(payload->>'source_url','')<>''
                ORDER BY reported_date DESC,id DESC LIMIT 500""", [today-timedelta(days=6), today])
            return select([dict(r) for r in cur.fetchall()], seen, today)
    finally:
        con.close()


def install(app, shared_db, shell):
    from fastapi import HTTPException

    @app.get('/roundup-preview')
    def preview():
        from . import city_corporate_social as feed
        from datetime import datetime
        cfg = feed.config()
        with shared_db() as c:
            seen = {r['key'].removeprefix(feed.PREFIX+'sent:') for r in c.execute('SELECT key FROM system_state WHERE key LIKE ?', (feed.PREFIX+'sent:%',)).fetchall()}
        doc = candidate(cfg['corporate_dsn'], seen, datetime.now(feed.JST).date())
        if not doc:
            return shell('地域まとめ候補', '<h1>地域まとめ候補</h1><p>現在、対象期間内で出典を確認できる未投稿情報がありません。</p>', '/roundup-preview', True)
        return render(doc, '/roundup-preview', True)

    @app.get('/roundup/{digest_id}')
    def roundup(digest_id: str):
        if not re.fullmatch(r'[a-f0-9]{20}', digest_id):
            raise HTTPException(404)
        with shared_db() as c:
            row = c.execute('SELECT value FROM system_state WHERE key=?', (KEY+digest_id,)).fetchone()
        if not row:
            raise HTTPException(404)
        doc = json.loads(row['value'])
        return render(doc, '/roundup/'+digest_id)

    def render(doc, path, noindex=False):
        title = doc['prefecture']+'の企業の動き｜'+doc['start']+'〜'+doc['end']+'公表分'
        body = '<h1>'+escape(title)+'</h1><p>公開情報から選んだ'+str(len(doc['items']))+'件です。地域の全件数や増減を示す統計ではありません。公表日と出来事の発生日は異なります。</p>'
        for r in doc['items']:
            body += '<article class="panel"><h2>'+escape(r['company'])+'</h2><p>'+escape(r['stage'])+'</p><p>所在地：'+escape(r.get('prefecture',''))+'</p><p>公表日：'+escape(r['reported_date'])+'</p><a href="'+BASE+'/company/'+escape(r['id'],quote=True)+'">最新の企業詳細</a> ｜ <a rel="noopener" href="'+escape(r['source_url'],quote=True)+'">出典：'+escape(r['source_name'])+'</a></article>'
        body += '<p>地域区分は本サイトの編集上の区分です。東日本は北海道・東北・関東・新潟・山梨・長野・静岡、西日本はそれ以外です。</p>'
        body += '<p>投稿時点のまとめです。追記・訂正は各社の最新詳細をご確認ください。</p><a href="'+BASE+'/">企業情報一覧へ</a>'
        return shell(title, body, path, noindex=noindex, description=title+'。各社の手続き、公表日、出典を確認できます。')
