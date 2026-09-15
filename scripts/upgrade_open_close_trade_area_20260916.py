from pathlib import Path

p = Path('services/open_close/collectors/seo_pages.py')
s = p.read_text()

if 'from .trade_area import compute_trade_area_score' not in s:
    s = s.replace('from .activity import compute_activity_score\n', 'from .activity import compute_activity_score\nfrom .trade_area import compute_trade_area_score\n', 1)

css_marker = '''ACTIVITY_CSS = """\n.activity-card'''
assert css_marker in s
if '.trade-area-card{' not in s:
    insert = '''
.trade-area-card{border:1px solid var(--line);border-radius:16px;padding:18px;background:#fff}
.trade-area-top{display:flex;justify-content:space-between;align-items:flex-start;gap:16px}
.trade-area-rank{font-size:38px;line-height:1;font-weight:900;color:var(--navy)}
.trade-area-score{font-size:13px;color:var(--muted);margin-top:5px}
.trade-area-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:14px}
.trade-area-kpi{padding:11px;border:1px solid var(--line);border-radius:11px;background:#fbfcfd}
.trade-area-kpi span{display:block;font-size:10px;color:var(--muted);margin-bottom:3px}
.trade-area-kpi strong{font-size:18px;color:var(--navy)}
.trade-area-components{display:grid;gap:8px;margin-top:14px}
.trade-area-component{display:grid;grid-template-columns:minmax(0,1fr) 46px;gap:10px;align-items:center}
.trade-area-component-name{font-size:12px;color:var(--ink)}
.trade-area-component-bar{height:7px;background:#e9eef3;border-radius:999px;overflow:hidden;margin-top:5px}
.trade-area-component-bar span{display:block;height:100%;background:#17263a;border-radius:999px}
.trade-area-component-score{text-align:right;font-size:12px;font-weight:800;color:var(--navy)}
.trade-area-flow{margin-top:12px;padding:10px 12px;border-radius:10px;background:#fff7e8;border:1px solid #f0d8aa;font-size:11px;color:#7b5a23}
@media(max-width:520px){.trade-area-grid{grid-template-columns:1fr}.trade-area-top{align-items:flex-end}}
'''
    s = s.replace('''@media(max-width:520px){
  .activity-counts{grid-template-columns:1fr 1fr}
}
"""''', '''@media(max-width:520px){
  .activity-counts{grid-template-columns:1fr 1fr}
}
''' + insert + '"""', 1)

query_anchor = '''            activity_row=cur.fetchone()\n\n    activity=compute_activity_score(*(activity_row or (0,0,0,0,0)))\n'''
assert query_anchor in s
if 'trade_area_row=cur.fetchone()' not in s:
    query_block = '''            activity_row=cur.fetchone()

            cur.execute("""
                SELECT
                    COUNT(*) FILTER(
                        WHERE category=%s
                          AND status IN('open','opening')
                          AND open_date IS NOT NULL
                          AND open_date >= CURRENT_DATE - INTERVAL '730 days'
                          AND open_date <= CURRENT_DATE
                    ),
                    COUNT(*) FILTER(
                        WHERE category=%s
                          AND status IN('closed','closing')
                          AND close_date IS NOT NULL
                          AND close_date >= CURRENT_DATE - INTERVAL '730 days'
                          AND close_date <= CURRENT_DATE
                    ),
                    PERCENTILE_CONT(0.5) WITHIN GROUP (
                        ORDER BY EXTRACT(EPOCH FROM (close_date::timestamp-open_date::timestamp))/2629800.0
                    ) FILTER(
                        WHERE category=%s
                          AND status IN('closed','closing')
                          AND open_date IS NOT NULL
                          AND close_date IS NOT NULL
                          AND close_date > open_date
                    ),
                    COUNT(*) FILTER(
                        WHERE category=%s
                          AND status IN('closed','closing')
                          AND open_date IS NOT NULL
                          AND close_date IS NOT NULL
                          AND close_date > open_date
                    )
                FROM stores
                WHERE COALESCE(status,'') <> 'excluded'
                  AND (COALESCE(NULLIF(address,''),'')<>'' OR COALESCE(confidence,0)>=94)
                  AND COALESCE(prefecture,'')=COALESCE(%s,'')
                  AND COALESCE(city,'')=COALESCE(%s,'')
            """,(d["category"],d["category"],d["category"],d["category"],d["prefecture"],d["city"]))
            trade_area_row=cur.fetchone()

    activity=compute_activity_score(*(activity_row or (0,0,0,0,0)))
    same_open_24m,same_close_24m,median_lifetime_months,lifetime_sample_count = trade_area_row or (0,0,None,0)
    trade_area=compute_trade_area_score(
        activity["score"],same_open_24m,same_close_24m,
        median_lifetime_months,lifetime_sample_count,foot_traffic_score=None
    )
'''
    s = s.replace(query_anchor, query_block, 1)

render_anchor = '''    activity_html=f"""\n    <div class="activity-card">'''
assert render_anchor in s
if 'trade_area_html=f"""' not in s:
    trade_render = '''    component_html="".join(
        f"<div class=\"trade-area-component\"><div><div class=\"trade-area-component-name\">{esc(c['name'])}（重み {c['weight']}%）</div><div class=\"trade-area-component-bar\"><span style=\"width:{c['score']}%\"></span></div></div><div class=\"trade-area-component-score\">{c['score']}</div></div>"
        for c in trade_area["components"]
    )
    lifetime_display=(f'{trade_area["median_lifetime_months"]:.1f}か月' if trade_area["median_lifetime_months"] is not None else '算出中')
    trade_area_html=f"""
    <div class="trade-area-card">
      <div class="trade-area-top">
        <div>
          <div style="font-size:12px;color:var(--muted);font-weight:700">商圏ポテンシャル β</div>
          <div class="trade-area-score">現在は開店閉店データと営業継続傾向から算出</div>
        </div>
        <div style="text-align:right"><div class="trade-area-rank">{trade_area["rank"]}</div><div class="trade-area-score">{trade_area["score"]}/100</div></div>
      </div>
      <div class="trade-area-grid">
        <div class="trade-area-kpi"><span>同業種・直近24か月</span><strong>開店 {trade_area["same_category_open_24m"]} / 閉店 {trade_area["same_category_close_24m"]}</strong></div>
        <div class="trade-area-kpi"><span>同業種の営業期間中央値</span><strong>{esc(lifetime_display)}</strong></div>
        <div class="trade-area-kpi"><span>営業期間サンプル</span><strong>{trade_area["lifetime_sample_count"]}件</strong></div>
      </div>
      <div class="trade-area-components">{component_html}</div>
      <div class="trade-area-flow">人流データ：未連携。現在のスコアには人流・通行量・売上データを含めていません。公的データまたは利用条件を確認した外部人流データを接続した段階で、人流を独立指標として追加します。</div>
    </div>
    """

'''
    s = s.replace(render_anchor, trade_render + render_anchor, 1)

section_anchor = '''    <section class="panel">
      <div class="section-head"><div><h2>周辺活力度</h2><p>{esc(area)}の開店・閉店動向から算出</p></div></div>
      {activity_html}
      <div class="notice">この指標は人流・売上・通行量を示すものではありません。開店閉店マップに蓄積された店舗の開店・閉店情報をもとにした参考値です。データ量により数値は変動します。</div>
    </section>

'''
assert section_anchor in s
if '<h2>商圏ポテンシャル</h2>' not in s:
    trade_section = '''    <section class="panel">
      <div class="section-head"><div><h2>商圏ポテンシャル</h2><p>出店候補地を見るための参考指標（β）</p></div></div>
      {trade_area_html}
      <div class="notice">商圏ポテンシャルは売上・利益・出店成功率を示すものではありません。現時点では当サイトの開店・閉店履歴と同業種の営業継続傾向をもとにした参考指標です。</div>
    </section>

'''
    s = s.replace(section_anchor, section_anchor + trade_section, 1)

p.write_text(s)
print('patched', p)
