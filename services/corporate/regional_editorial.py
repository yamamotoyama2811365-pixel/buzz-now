"""地域の公開レコードから作る、出典をたどれる編集特集。DBへの書込みなし。"""
from collections import Counter
from datetime import date, timedelta
from html import escape
from urllib.parse import quote, urlsplit

EDITORIAL_DATE = '2026-09-14'
TDB_SOURCE = 'https://www.tdb.co.jp/report/bankruptcy/aggregation/20260908-bankruptcy202608/'
# 2026-09-08公表の本文で照合した同一月・同一定義の数字のみ。
TDB_PREF = {'大阪府': (83, 110), '和歌山県': (8, 21), '愛知県': (38, 70), '三重県': (5, 13)}

def source_ok(value):
    try:
        u = urlsplit(value or '')
        return u.scheme in {'http', 'https'} and bool(u.netloc)
    except ValueError:
        return False

def summarize(rows, prefecture, end):
    start = end - timedelta(days=29)
    entities = {}
    for row in rows:
        p = row['payload']
        try:
            day = date.fromisoformat(str(p.get('reported_date', ''))[:10])
        except ValueError:
            continue
        if not start <= day <= end or p.get('prefecture') != prefecture:
            continue
        if p.get('kind') not in {'bankruptcy', 'registration'} or not source_ok(p.get('source_url')):
            continue
        key = (p['kind'], p.get('corporate_number') or p.get('entity_key') or row['id'])
        # 同一法人の続報を別会社として足さず、期間内の最新報道を残す。
        old = entities.get(key)
        if old is None or (p['reported_date'], row['id']) > (old['payload']['reported_date'], old['id']):
            entities[key] = row
    selected = sorted(entities.values(), key=lambda r: (r['payload']['reported_date'], r['id']), reverse=True)
    return {kind: [r for r in selected if r['payload']['kind'] == kind] for kind in ('bankruptcy', 'registration')}

def banner(prefecture, root='/corporate'):
    url = root + '/area/' + quote(prefecture)
    return ('<nav class="regional-features" aria-label="地域の編集特集">'
            '<a href="' + url + '#bankruptcy-feature"><span>編集特集 · 倒産</span><strong>' + escape(prefecture) + 'の倒産動向を読む</strong><small>報道された事案・手続き・統計を確認 →</small></a>'
            '<a href="' + url + '#registration-feature"><span>編集特集 · 新規法人</span><strong>' + escape(prefecture) + 'の新規法人の動きを読む</strong><small>法人番号指定・所在地・確認すべき点 →</small></a></nav>')

def examples(rows, registration, root):
    if not rows:
        return '<p>この期間に出典付きで掲載できる事案はありません。地域全体で発生・設立がなかったという意味ではありません。</p>'
    parts = ['<div class="feature-table"><table><caption>掲載データのうち日付が新しい順、最大5件</caption><thead><tr><th scope="col">法人・出典</th><th scope="col">' + ('法人番号指定日' if registration else '報道日・掲載時の状況') + '</th><th scope="col">所在地（掲載情報）</th></tr></thead><tbody>']
    for row in rows[:5]:
        p = row['payload']
        parts.append('<tr><td><a href="' + root + '/company/' + quote(row['id'], safe='') + '">' + escape(p['company']) + '</a><br><a class="source" href="' + escape(p['source_url'], quote=True) + '" rel="noopener noreferrer" target="_blank">' + escape(p.get('source_name') or '出典') + ' ↗</a></td><td>' + escape(p['reported_date']) + ('' if registration else '<br>' + escape(p.get('stage') or '確認中')) + '</td><td>' + escape(p.get('address') or '詳しい所在地は未確認') + '</td></tr>')
    return ''.join(parts) + '</tbody></table></div>'

def render(prefecture, rows, end, root='/corporate', truncated=False, checked_at=None):
    area = escape(prefecture)
    data = summarize(rows, prefecture, end)
    bankruptcies, registrations = data['bankruptcy'], data['registration']
    start = end - timedelta(days=29)
    period = f'{start.isoformat()}〜{end.isoformat()}'
    collected = [str(r.get('first_seen', ''))[:10] for r in rows if r.get('first_seen')]
    coverage = '今回の対象レコードで最も早い初回収集日：' + min(collected) + '。' if collected else '収集開始日の確認は未完了です。'
    caveat = ('集計上限を超えたため、最新5,000掲載レコード内の参考値です。' if truncated else '')
    header = '<section class="regional-editorial" aria-label="地域の動向特集"><p class="small">集計対象：' + period + '（日本時間の日付・昨日までの30日）。' + coverage + caveat + '編集方針更新：' + EDITORIAL_DATE + '。掲載事例は公開データから自動更新します。</p>'
    if checked_at:
        header += '<p class="small">対象の掲載データ最終更新日時：' + escape(str(checked_at)) + '。元情報の公表から収集まで遅れる場合があります。</p>'
    header += '<p class="notice">当サイトが収集した公開情報の整理です。地域全体の倒産件数・設立件数を網羅した統計ではありません。未収集の期間をゼロとして増減を判定しません。</p>'
    stages = Counter(r['payload'].get('stage') or '状況未確認' for r in bankruptcies)
    stage_text = '、'.join(escape(k) + ' ' + str(v) + '件' for k, v in stages.most_common())
    body = '<article class="panel region-feature" id="bankruptcy-feature"><div class="eyebrow">編集特集 / 倒産動向</div><h2>' + area + 'の倒産動向：直近の事案をどう読むか</h2>'
    body += '<h3>掲載情報から確認できること</h3><p>' + period + 'の報道日で整理すると、出典付きの掲載事案は名寄せ後<strong>' + str(len(bankruptcies)) + '件</strong>です。' + ('掲載時の状況は、' + stage_text + 'です。' if stages else '') + '同じ法人の続報は期間内の最新情報を代表にしています。</p>'
    body += examples(bankruptcies, False, root)
    body += '<h3>編集部の見方：件数より先に、手続きと時点をそろえる</h3><p>地域の変化を追うときは、上の事案を一括して「会社がなくなった」と読むのではなく、それぞれの報道日と手続きの状況を確認するのが出発点です。申請準備の報道と手続き開始の報道が混在するため、日付順の件数だけでは倒産発生の月別推移を表せません。会社詳細と出典を行き来して、続報や事業継続に関する記載を確認できる構成にしました。</p><p>現段階の収集データだけから「' + area + 'の景気が悪化した」「特定業種が危険」とは判断しません。業種は未確認・自動分類の情報を含むため、業種別の原因分析には各出典の事業内容との照合が必要です。</p>'
    body += '<h3>月次統計と照らして読む</h3><p>帝国データバンクの2026年8月集計（9月8日公表）では、全国の倒産は827件、前年同月は751件でした。これは同社の定義による全国値で、上記の当サイト掲載件数とは集計範囲が異なります。'
    if prefecture in TDB_PREF:
        old, current = TDB_PREF[prefecture]
        body += '同じ資料本文に掲載された' + area + 'の値は、前年8月' + str(old) + '件に対し2026年8月' + str(current) + '件です。'
    else:
        body += area + '単独の前年同月比は、ここでは確認済みの数値を掲載していません。全国の増減をそのまま地域に当てはめることはできません。'
    body += '</p><p><a class="source" href="' + TDB_SOURCE + '" target="_blank" rel="noopener noreferrer">出典：帝国データバンク「倒産集計 2026年8月報」</a></p></article>'
    body += '<article class="panel region-feature" id="registration-feature"><div class="eyebrow">編集特集 / 新規法人</div><h2>' + area + 'の新規法人設立動向を調べる：指定日と所在地から読む</h2><h3>法人番号の新規指定から見える動き</h3><p>' + period + 'に法人番号を指定された掲載法人は<strong>' + str(len(registrations)) + '件</strong>です。同じ法人番号は重複して数えません。下表は掲載済み情報の新しい例で、地域の全設立法人を抜き出したものではありません。</p>'
    body += examples(registrations, True, root)
    body += '<h3>編集部の見方：新しい会社を知る入口として使う</h3><p>新規法人の一覧は、' + area + 'でどの法人の情報が新たに公表されたかを知る入口になります。ただし法人番号の指定日は、登記上の設立年月日とは別の項目です。会社詳細では名称・所在地・番号を確認し、同名の法人との取り違えを防いでから、公式サイトなどで事業内容を確かめる読み方を勧めます。</p><p>所在地は本店・主たる事務所の登録情報です。その場所で店舗が開店した、従業員を採用した、事業を開始したという裏付けにはなりません。番号指定の件数だけで雇用増加や地域経済の成長を評価せず、開業告知や事業内容の公表を次の確認材料にします。</p><h3>設立動向として比較するために必要な情報</h3><p>月ごとの比較には、対象法人の範囲、日付の定義、収集漏れ、名称・住所変更と新規指定の区別をそろえる必要があります。当サイトは現在、新規指定の公開データを入口に整理しています。倒産の掲載数を差し引いて「企業の純増」とする計算は、対象と日付が一致しないため行いません。</p><p><a class="source" href="https://www.houjin-bangou.nta.go.jp/setsumei/" target="_blank" rel="noopener noreferrer">出典：国税庁「法人番号とは」</a> · <a class="source" href="https://www.houjin-bangou.nta.go.jp/setsumei/tsuchisho/" target="_blank" rel="noopener noreferrer">指定日の説明</a></p></article></section>'
    return header + body
