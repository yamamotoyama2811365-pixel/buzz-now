from pathlib import Path

p = Path('services/open_close/collectors/seo_pages.py')
s = p.read_text()

if 'from .location_intelligence import compute_category_signal' not in s:
    s = s.replace(
        'from .trade_area import compute_trade_area_score\n',
        'from .trade_area import compute_trade_area_score\nfrom .location_intelligence import compute_category_signal\n',
        1,
    )

if '.commercial-board{' not in s:
    css = '''
.commercial-board{border:1px solid var(--line);border-radius:16px;padding:18px;background:linear-gradient(180deg,#fff,#f8fafc)}
.commercial-head{display:flex;justify-content:space-between;gap:18px;align-items:flex-start}
.commercial-rank{font-size:42px;font-weight:900;line-height:1;color:var(--navy);text-align:right}
.commercial-rank small{display:block;font-size:11px;color:var(--muted);font-weight:700;margin-top:6px}
.commercial-kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-top:14px}
.commercial-kpi{border:1px solid var(--line);border-radius:11px;padding:11px;background:#fff}
.commercial-kpi span{display:block;font-size:10px;color:var(--muted);margin-bottom:3px}
.commercial-kpi strong{font-size:19px;color:var(--navy)}
.commercial-table{display:grid;gap:7px;margin-top:15px}
.commercial-row{display:grid;grid-template-columns:minmax(115px,1.4fr) 58px 58px 70px 58px;gap:8px;align-items:center;border:1px solid var(--line);border-radius:11px;padding:10px 12px;background:#fff}
.commercial-row.head{border:0;background:transparent;padding:0 12px 3px;font-size:10px;color:var(--muted)}
.commercial-row.head span:not(:first-child),.commercial-num{text-align:right}
.commercial-cat{font-size:13px;font-weight:800;color:var(--navy);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.commercial-num{font-size:12px;color:var(--muted)}
.commercial-grade{text-align:right;font-size:18px;font-weight:900;color:var(--navy)}
.commercial-score{text-align:right;font-size:12px;font-weight:800;color:var(--ink)}
.data-coverage{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:14px}
.data-source{border-radius:11px;border:1px solid var(--line);padding:11px;background:#fff}
.data-source strong{display:block;font-size:12px;color:var(--navy);margin-bottom:3px}
.data-source span{font-size:10px;color:var(--muted)}
.data-source.pending{background:#fff9ee;border-color:#efdfbd}
@media(max-width:680px){
 .commercial-kpis{grid-template-columns:1fr 1fr}.data-coverage{grid-template-columns:1fr}
 .commercial-row{grid-template-columns:minmax(105px,1.4fr) 42px 42px 50px 50px;gap:5px;padding:9px 8px}
 .commercial-row.head{padding:0 8px 3px}.commercial-grade{font-size:16px}
}
'''
    s = s.replace('\n"""\n\ndef _connect(database_url):', '\n' + css + '"""\n\ndef _connect(database_url):', 1)

sql_anchor = '''            category_changes = cur.fetchall()\n\n    area_name = f"{prefecture}{city or ''}"'''
assert sql_anchor in s, 'category_changes anchor missing'
if 'commercial_category_rows = []' not in s:
    sql_insert = '''            category_changes = cur.fetchall()

            commercial_category_rows = []
            active_tenant_count = 0
            if city:
                cur.execute(f"""
                    SELECT
                        COALESCE(NULLIF(category,''),'業種未分類') AS category,
                        COUNT(*) AS total,
                        COUNT(*) FILTER(
                            WHERE open_date IS NOT NULL
                              AND open_date >= CURRENT_DATE - INTERVAL '730 days'
                              AND open_date <= CURRENT_DATE
                        ) AS open_24m,
                        COUNT(*) FILTER(
                            WHERE close_date IS NOT NULL
                              AND close_date >= CURRENT_DATE - INTERVAL '730 days'
                              AND close_date <= CURRENT_DATE
                        ) AS close_24m,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (
                            ORDER BY EXTRACT(EPOCH FROM (close_date::timestamp-open_date::timestamp))/2629800.0
                        ) FILTER(
                            WHERE open_date IS NOT NULL
                              AND close_date IS NOT NULL
                              AND close_date > open_date
                        ) AS median_lifetime_months,
                        COUNT(*) FILTER(
                            WHERE open_date IS NOT NULL
                              AND close_date IS NOT NULL
                              AND close_date > open_date
                        ) AS lifetime_samples
                    FROM stores
                    WHERE {area_where}
                    GROUP BY 1
                    HAVING
                        COUNT(*) FILTER(
                            WHERE open_date IS NOT NULL
                              AND open_date >= CURRENT_DATE - INTERVAL '730 days'
                              AND open_date <= CURRENT_DATE
                        )
                        +
                        COUNT(*) FILTER(
                            WHERE close_date IS NOT NULL
                              AND close_date >= CURRENT_DATE - INTERVAL '730 days'
                              AND close_date <= CURRENT_DATE
                        ) > 0
                    ORDER BY total DESC,category
                    LIMIT 24
                """,area_params)
                commercial_category_rows = cur.fetchall()

                cur.execute(f"""
                    SELECT COUNT(DISTINCT tl.store_id)
                    FROM tenant_listings tl
                    JOIN stores s ON s.id=tl.store_id
                    WHERE tl.status IN('detected','active')
                      AND {area_where.replace('COALESCE(status', "COALESCE(s.status").replace("prefecture=%s", "s.prefecture=%s").replace("(city=", "(s.city=").replace(" OR city=", " OR s.city=").replace(" OR city LIKE", " OR s.city LIKE")}
                """,area_params)
                active_tenant_count = int(cur.fetchone()[0] or 0)

    area_name = f"{prefecture}{city or ''}"'''
    s = s.replace(sql_anchor, sql_insert, 1)

calc_anchor = '''    insight_html=f"""\n    <section class="panel">'''
assert calc_anchor in s, 'insight html anchor missing'

body_anchor = '''    body = f"""\n<section class="hero">'''
assert body_anchor in s, 'body anchor missing'
if 'commercial_html = ""' not in s:
    commercial_code = '''    commercial_html = ""
    if city:
        area_activity = compute_activity_score(all_total,open_12m,close_12m,0,0)
        excluded_for_commercial={"業種未分類","未分類","小売","飲食店"}
        signals=[]
        for cat,ctotal,copen,cclose,median_months,lifetime_samples in commercial_category_rows:
            if not cat or cat in excluded_for_commercial:
                continue
            signal=compute_category_signal(ctotal,copen,cclose,median_months,lifetime_samples)
            signal["category"]=cat
            signals.append(signal)
        signals.sort(key=lambda x:(x["rank"]!="-",x["score"],x["events_24m"],x["total"]),reverse=True)
        signals=signals[:8]

        category_signal_html=""
        if signals:
            signal_rows=[]
            for sig in signals:
                score_text=str(sig["score"]) if sig["rank"]!="-" else "-"
                signal_rows.append(f"""
                <div class="commercial-row">
                  <div class="commercial-cat"><a href="/category/{qpath(sig['category'])}">{esc(sig['category'])}</a></div>
                  <div class="commercial-num">{sig['open_24m']}</div>
                  <div class="commercial-num">{sig['close_24m']}</div>
                  <div class="commercial-grade">{sig['rank']}</div>
                  <div class="commercial-score">{score_text}</div>
                </div>""")
            category_signal_html=(
                '<div class="commercial-row head"><span>業種</span><span>開店</span><span>閉店</span><span>参考</span><span>点</span></div>'
                + '<div class="commercial-table">' + ''.join(signal_rows) + '</div>'
            )
        else:
            category_signal_html='<div class="empty">業種別の出店参考スコアを算出できるデータを蓄積中です。</div>'

        commercial_html=f"""
        <section class="panel" id="commercial-analysis">
          <div class="section-head">
            <div><h2>{esc(display_name)}の商圏・出店分析</h2><p>不動産・店舗開発向けの参考データ</p></div>
          </div>
          <div class="commercial-board">
            <div class="commercial-head">
              <div>
                <strong style="font-size:16px;color:var(--navy)">エリア活力度</strong>
                <div class="side-copy" style="margin:5px 0 0">開店・閉店の確認件数から算出。人流や売上ではありません。</div>
              </div>
              <div class="commercial-rank">{area_activity['score']}<small>/100　{esc(area_activity['label'])}</small></div>
            </div>
            <div class="commercial-kpis">
              <div class="commercial-kpi"><span>直近12か月 開店</span><strong>{open_12m}件</strong></div>
              <div class="commercial-kpi"><span>直近12か月 閉店</span><strong>{close_12m}件</strong></div>
              <div class="commercial-kpi"><span>店舗入替イベント</span><strong>{observed_12m}件</strong></div>
              <div class="commercial-kpi"><span>テナント募集情報を確認</span><strong>{active_tenant_count}件</strong></div>
            </div>
          </div>

          <div class="section-head" style="margin-top:22px">
            <div><h2 style="font-size:18px">業種別 出店参考ランク</h2><p>直近24か月の開閉店＋営業継続データ</p></div>
          </div>
          {category_signal_html}
          <div class="insight-note">※ S〜Dは当サイト内データから算出する参考ランクです。売上・利益・出店成功率を示すものではありません。イベント3件未満は「-」として評価を保留します。</div>

          <div class="section-head" style="margin-top:22px">
            <div><h2 style="font-size:18px">商圏データのカバー状況</h2><p>今後、人流・交通データを段階的に追加</p></div>
          </div>
          <div class="data-coverage">
            <div class="data-source"><strong>開店・閉店動向</strong><span>連携済み／直近12〜24か月</span></div>
            <div class="data-source"><strong>業種別の店舗動向</strong><span>連携済み／参考ランクに反映</span></div>
            <div class="data-source pending"><strong>駅乗降客数・人流</strong><span>未連携／現在のスコアには含めていません</span></div>
          </div>
        </section>
        """

'''
    s = s.replace(body_anchor, commercial_code + body_anchor, 1)

if '{commercial_html}' not in s:
    s = s.replace('''    {summary_html}\n    {insight_html}\n    <section class="panel">''', '''    {summary_html}\n    {insight_html}\n    {commercial_html}\n    <section class="panel">''', 1)

# Add an explicit path from store detail into the city-level commercial analysis.
store_anchor = '''    area_links=[]\n    if d["prefecture"]:'''
assert store_anchor in s, 'store area links anchor missing'
if 'commercial_area_link=' not in s:
    store_insert = '''    commercial_area_link=""
    if d["prefecture"] and d["city"] and city_is_indexable(d["prefecture"],d["city"]):
        commercial_area_link=(
            f'<div class="action-row"><a class="action-link primary" href="/area/{qpath(d["prefecture"])}/{qpath(d["city"])}#commercial-analysis" '
            f'data-ga-event="commercial_area_click" data-ga-store-id="{d["id"]}">このエリアの商圏・出店分析を見る →</a></div>'
        )

    area_links=[]
    if d["prefecture"]:'''
    s = s.replace(store_anchor, store_insert, 1)

store_section_anchor = '''      {trade_area_html}\n      <div class="notice">商圏ポテンシャルは売上・利益・出店成功率を示すものではありません。現時点では当サイトの開店・閉店履歴と同業種の営業継続傾向をもとにした参考指標です。</div>'''
assert store_section_anchor in s, 'store commercial section anchor missing'
if '{commercial_area_link}' not in s:
    s = s.replace(store_section_anchor, store_section_anchor + '\n      {commercial_area_link}', 1)

p.write_text(s)
print('patched', p)
