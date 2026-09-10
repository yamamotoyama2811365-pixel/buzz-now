from __future__ import annotations
from .public_quality import public_store

import html
import json
from datetime import date, datetime
from urllib.parse import quote, quote_plus
from .activity import compute_activity_score

SITE_NAME = "開店閉店マップ"

CSS = """
:root{
  --navy:#132238;--ink:#243247;--muted:#6b7788;--line:#e5eaf0;
  --bg:#f5f7fa;--white:#fff;--green:#148a62;--green-bg:#eaf7f1;
  --orange:#b96c16;--orange-bg:#fff3e3;--red:#c24848;--red-bg:#fdeeee;
  --blue:#386aa8;--blue-bg:#edf4fc
}
*{box-sizing:border-box}
html{scroll-behavior:smooth}
body{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Hiragino Kaku Gothic ProN","Yu Gothic",Meiryo,sans-serif;background:var(--bg);color:var(--ink);line-height:1.7}
a{color:inherit;text-decoration:none}
.wrap{width:min(1160px,calc(100% - 32px));margin:0 auto}
.site-header{background:#fff;border-bottom:1px solid var(--line);position:sticky;top:0;z-index:20}
.header-inner{height:72px;display:flex;align-items:center;gap:26px}
.brand{font-weight:800;font-size:22px;letter-spacing:-.04em;color:var(--navy);white-space:nowrap}
.brand small{display:block;font-size:10px;letter-spacing:.08em;color:#8691a0;font-weight:600;margin-top:-4px}
.nav{display:flex;gap:20px;margin-left:auto;font-size:13px;color:#4d5a6b}
.search-chip{padding:8px 13px;border:1px solid var(--line);border-radius:999px;background:#fafbfd}
.hero{background:linear-gradient(135deg,#15273e,#263d59);color:white;padding:44px 0 40px}
.crumbs{font-size:12px;color:#7b8796;margin:18px 0}
.hero .crumbs{color:#c7d2df;margin:0 0 15px}
h1{font-size:clamp(28px,4vw,42px);line-height:1.3;letter-spacing:-.04em;margin:0 0 12px}
.hero p{margin:0;color:#dce5ee;max-width:760px}
.stats{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-top:26px;max-width:760px}
.stat{background:rgba(255,255,255,.1);border:1px solid rgba(255,255,255,.16);border-radius:14px;padding:13px 16px}
.stat strong{font-size:22px;display:block}
.stat span{font-size:11px;color:#d4deea}
.layout{display:grid;grid-template-columns:minmax(0,1fr) 290px;gap:28px;padding:28px 0 56px}
.panel{background:#fff;border:1px solid var(--line);border-radius:18px;padding:22px}
.panel+.panel{margin-top:18px}
.section-head{display:flex;justify-content:space-between;align-items:flex-end;gap:15px;margin-bottom:15px}
.section-head h2{margin:0;color:var(--navy);font-size:22px;letter-spacing:-.03em}
.section-head p{margin:0;color:var(--muted);font-size:12px}
.store-list{display:grid;gap:12px}
.store-card{display:grid;grid-template-columns:110px 1fr auto;gap:16px;align-items:center;padding:16px;border:1px solid var(--line);border-radius:15px;background:#fff;transition:.15s}
.store-card:hover{border-color:#bdc9d6;transform:translateY(-1px);box-shadow:0 8px 24px rgba(30,48,72,.06)}
.store-date{font-size:12px;color:var(--muted)}
.store-main h3{font-size:17px;margin:4px 0 3px;line-height:1.45;color:var(--navy)}
.store-main p{font-size:12px;color:var(--muted);margin:0}
.badge{display:inline-flex;align-items:center;border-radius:999px;padding:4px 9px;font-size:11px;font-weight:700;white-space:nowrap}
.badge.opening,.badge.open{background:var(--green-bg);color:var(--green)}
.badge.closing,.badge.closed{background:var(--red-bg);color:var(--red)}
.badge.tenant{background:var(--blue-bg);color:var(--blue)}
.arrow{font-size:20px;color:#a6afbb}
.facets{display:flex;flex-wrap:wrap;gap:8px}
.facet{display:inline-flex;padding:8px 11px;border:1px solid var(--line);background:#fff;border-radius:10px;font-size:12px}
.facet:hover{background:#f7f9fb;border-color:#c8d2dd}
.facet strong{margin-left:6px;color:var(--muted)}
.side-title{margin:0 0 12px;font-size:16px;color:var(--navy)}
.side-copy{font-size:12px;color:var(--muted);margin:-4px 0 13px}
.detail-grid{display:grid;grid-template-columns:160px 1fr;border-top:1px solid var(--line)}
.detail-grid dt,.detail-grid dd{margin:0;padding:12px 8px;border-bottom:1px solid var(--line);font-size:13px}
.detail-grid dt{color:var(--muted);font-weight:600}
.detail-grid dd{font-weight:500}
.source-link,.official-link{display:inline-flex;margin-top:12px;margin-right:8px;padding:9px 12px;border-radius:10px;border:1px solid var(--line);font-size:12px;background:#fff}
.official-link{background:var(--navy);color:#fff;border-color:var(--navy)}
.map{overflow:hidden;border-radius:14px;border:1px solid var(--line);margin-top:16px;height:320px}
.map iframe{border:0;width:100%;height:100%}
.notice{padding:13px 15px;background:#f7f9fb;border-radius:12px;color:#657284;font-size:12px;margin-top:16px}
.action-row{display:flex;gap:8px;flex-wrap:wrap;margin-top:14px}
.action-link{display:inline-flex;align-items:center;padding:10px 13px;border-radius:10px;border:1px solid var(--line);background:#fff;font-size:12px;font-weight:700}
.action-link.primary{background:var(--navy);border-color:var(--navy);color:#fff}
.tenant-box{border:1px solid var(--line);border-radius:14px;padding:15px;margin-top:12px;background:#fbfcfd}
.tenant-box strong{display:block;color:var(--navy);margin-bottom:4px}
.tenant-meta{font-size:12px;color:var(--muted)}
.timeline{display:grid;gap:0;margin-top:6px}
.timeline-item{position:relative;padding:12px 12px 12px 28px;border-left:2px solid #dbe3eb;margin-left:8px}
.timeline-item:before{content:"";position:absolute;left:-6px;top:19px;width:10px;height:10px;border-radius:50%;background:#fff;border:2px solid var(--navy)}
.timeline-date{font-size:11px;color:var(--muted)}
.timeline-name{font-weight:800;color:var(--navy);margin-top:2px}
.timeline-status{font-size:12px;color:var(--muted)}
.empty{padding:38px 18px;text-align:center;color:var(--muted);font-size:13px;border:1px dashed #ced6df;border-radius:14px}
.pagination{display:flex;justify-content:center;gap:8px;margin-top:18px}
.pagination a{padding:8px 12px;background:#fff;border:1px solid var(--line);border-radius:9px;font-size:12px}
.status-tabs{display:flex;gap:8px;padding:4px;background:#eef2f6;border-radius:12px;width:max-content;margin:0 0 18px}
.status-tab{display:inline-flex;align-items:center;justify-content:center;padding:8px 16px;border-radius:9px;font-size:12px;font-weight:800;color:#697586}
.status-tab:hover{background:#fff}
.status-tab.active{background:#17263a;color:#fff;box-shadow:0 2px 6px rgba(0,0,0,.08)}
.status-tab.opening:not(.active){color:#16825d}
.status-tab.closing:not(.active){color:#c34a4a}
.status-tab.active.opening{background:#16825d;color:#fff}
.status-tab.active.closing{background:#c34a4a;color:#fff}

.insight-summary{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin:12px 0 20px}
.insight-kpi{border:1px solid var(--line);border-radius:13px;padding:14px;background:#fbfcfd}
.insight-kpi span{display:block;font-size:11px;color:var(--muted);margin-bottom:3px}
.insight-kpi strong{font-size:24px;line-height:1.25;color:var(--navy)}
.insight-kpi small{font-size:11px;color:var(--muted);margin-left:4px}
.trend-label{display:inline-flex;align-items:center;border-radius:999px;padding:5px 10px;font-size:11px;font-weight:800;margin-bottom:10px}
.trend-label.up{background:var(--green-bg);color:var(--green)}
.trend-label.down{background:var(--red-bg);color:var(--red)}
.trend-label.flat{background:#eef2f6;color:#5c6877}
.monthly-chart{display:grid;grid-template-columns:repeat(12,minmax(34px,1fr));gap:7px;align-items:end;height:190px;padding:18px 4px 4px;border-bottom:1px solid var(--line);margin-top:12px}
.month-col{height:100%;display:flex;flex-direction:column;justify-content:flex-end;align-items:center;gap:5px;min-width:0}
.month-bars{height:138px;width:100%;display:flex;align-items:flex-end;justify-content:center;gap:3px}
.month-bar{width:min(13px,42%);min-height:2px;border-radius:4px 4px 1px 1px}
.month-bar.open{background:var(--green)}
.month-bar.close{background:var(--red)}
.month-name{font-size:9px;color:var(--muted);white-space:nowrap}
.chart-legend{display:flex;gap:14px;font-size:11px;color:var(--muted);margin-top:10px}

.line-chart-wrap{margin-top:14px;padding:14px 14px 8px;border:1px solid var(--line);border-radius:14px;background:#fbfcfd}
.line-chart-title{font-size:12px;font-weight:800;color:var(--navy);margin-bottom:8px}
.line-chart-svg{display:block;width:100%;height:auto;overflow:visible}
.line-grid{stroke:#dfe5eb;stroke-width:1}
.line-axis-label{fill:#8793a1;font-size:9px}
.line-open{fill:none;stroke:var(--green);stroke-width:3;stroke-linecap:round;stroke-linejoin:round}
.line-close{fill:none;stroke:var(--red);stroke-width:3;stroke-linecap:round;stroke-linejoin:round}
.line-open-dot{fill:var(--green);stroke:#fff;stroke-width:2}
.line-close-dot{fill:var(--red);stroke:#fff;stroke-width:2}
.line-zero-note{font-size:10px;color:var(--muted);margin-top:6px}

.combo-chart-wrap{margin-top:14px;padding:14px 14px 8px;border:1px solid var(--line);border-radius:14px;background:#fbfcfd}
.combo-chart-title{font-size:12px;font-weight:800;color:var(--navy);margin-bottom:8px}
.combo-svg{display:block;width:100%;height:auto;overflow:visible}
.combo-grid{stroke:#dfe5eb;stroke-width:1}
.combo-axis{fill:#8793a1;font-size:9px}
.combo-open-bar{fill:var(--green);opacity:.26}
.combo-close-bar{fill:var(--red);opacity:.26}
.combo-open-line{fill:none;stroke:var(--green);stroke-width:3;stroke-linecap:round;stroke-linejoin:round}
.combo-close-line{fill:none;stroke:var(--red);stroke-width:3;stroke-linecap:round;stroke-linejoin:round}
.combo-open-dot{fill:var(--green);stroke:#fff;stroke-width:2}
.combo-close-dot{fill:var(--red);stroke:#fff;stroke-width:2}


.legend-dot{display:inline-block;width:8px;height:8px;border-radius:2px;margin-right:5px}
.legend-dot.open{background:var(--green)}
.legend-dot.close{background:var(--red)}
.category-change-list{display:grid;gap:9px;margin-top:8px}
.category-change{display:grid;grid-template-columns:minmax(100px,1.4fr) 70px 70px 72px;gap:8px;align-items:center;padding:11px 12px;border:1px solid var(--line);border-radius:12px}
.category-change-name{font-size:13px;font-weight:800;color:var(--navy);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.category-change-num{text-align:right;font-size:12px;color:var(--muted)}
.category-delta{text-align:right;font-weight:900;font-size:14px}
.category-delta.up{color:var(--green)}
.category-delta.down{color:var(--red)}
.category-delta.flat{color:#697586}
.category-head{display:grid;grid-template-columns:minmax(100px,1.4fr) 70px 70px 72px;gap:8px;padding:0 12px 4px;font-size:10px;color:var(--muted)}
.category-head span:not(:first-child){text-align:right}
.insight-note{font-size:11px;color:var(--muted);margin-top:12px;line-height:1.7}

.footer{background:#101d2e;color:#c6d0dc;padding:34px 0;margin-top:20px;font-size:12px}
.footer strong{display:block;color:white;font-size:17px;margin-bottom:6px}
.footer-links{display:flex;gap:18px;flex-wrap:wrap;margin-top:13px}
@media(max-width:820px){
  .nav{display:none}.header-inner{height:62px}.layout{grid-template-columns:1fr}.stats{grid-template-columns:1fr 1fr 1fr}
  .store-card{grid-template-columns:1fr auto}.store-date{grid-column:1/-1;margin-bottom:-9px}
  .detail-grid{grid-template-columns:115px 1fr}.side{order:2}
}
@media(max-width:520px){
  .wrap{width:min(100% - 22px,1160px)}.hero{padding:32px 0}.stats{grid-template-columns:1fr}
  .panel{padding:16px;border-radius:14px}.store-card{padding:13px}.section-head{display:block}.section-head p{margin-top:4px}
  .status-tabs{width:100%}.status-tab{flex:1}
  .insight-summary{grid-template-columns:1fr 1fr 1fr;gap:6px}
  .insight-kpi{padding:10px}.insight-kpi strong{font-size:18px}
  .monthly-chart{gap:3px;height:170px;overflow-x:auto}
  .category-change,.category-head{grid-template-columns:minmax(90px,1.4fr) 52px 52px 58px;gap:5px}
}
"""

ACTIVITY_CSS = """
.activity-card{border:1px solid var(--line);border-radius:16px;padding:18px;background:linear-gradient(180deg,#fff,#f8fafc)}
.activity-top{display:flex;align-items:flex-end;justify-content:space-between;gap:16px}
.activity-score{font-size:44px;line-height:1;font-weight:900;color:var(--navy)}
.activity-score small{font-size:14px;font-weight:700;color:var(--muted)}
.activity-label{font-size:15px;font-weight:900}
.activity-meter{height:10px;background:#e9eef3;border-radius:999px;overflow:hidden;margin:14px 0}
.activity-meter span{display:block;height:100%;background:#17263a;border-radius:999px}
.activity-counts{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-top:14px}
.activity-count{padding:10px;border-radius:10px;background:#fff;border:1px solid var(--line);text-align:center}
.activity-count strong{display:block;font-size:18px}
.activity-count span{font-size:11px;color:var(--muted)}
@media(max-width:520px){
  .activity-counts{grid-template-columns:1fr 1fr}
}
"""

def _connect(database_url):
    import psycopg
    return psycopg.connect(database_url)

def esc(value):
    return html.escape(str(value or ""), quote=True)

def qpath(value):
    return quote(str(value or ""), safe="")

def site_url(origin, path):
    return origin.rstrip("/") + path

def fmt_date(value):
    if not value:
        return "日付未確認"
    if isinstance(value, str):
        try:
            value = date.fromisoformat(value[:10])
        except Exception:
            return esc(value)
    return f"{value.year}年{value.month}月{value.day}日"

def event_date(row):
    status = row.get("status") or ""
    return row.get("close_date") if status in ("closed","closing") else row.get("open_date")

def status_info(status, event=None):
    today = date.today()
    d = event
    if isinstance(d, str):
        try:
            d = date.fromisoformat(d[:10])
        except Exception:
            d = None
    if status in ("closing","closed"):
        if d and d <= today:
            return "閉店", "closed"
        return "閉店予定", "closing"
    if status in ("opening","open"):
        if d and d <= today:
            return "開店", "open"
        return "開店予定", "opening"
    if status == "tenant":
        return "テナント募集", "tenant"
    return "店舗情報", "opening"

def row_to_dict(r):
    return public_store({
        "id":r[0],"name":r[1],"status":r[2],"category":r[3],
        "prefecture":r[4],"city":r[5],"address":r[6],
        "facility_name":r[7],"floor":r[8],"postal_code":r[9],
        "open_date":r[10],"close_date":r[11],
        "source_url":r[12],"source_name":r[13],"official_url":r[14],
        "confidence":r[15],"last_verified_at":r[16]
    })

STORE_SELECT = """
    SELECT
        id,name,status,category,prefecture,city,address,
        facility_name,floor,postal_code,
        open_date,close_date,source_url,source_name,official_url,
        confidence,last_verified_at
    FROM stores
"""

def page_shell(origin, title, description, canonical_path, body, json_ld=None, noindex=False):
    canonical = site_url(origin, canonical_path)
    robot = "noindex,follow" if noindex else "index,follow,max-image-preview:large"
    json_ld_html = ""
    if json_ld:
        json_ld_html = '<script type="application/ld+json">' + json.dumps(
            json_ld, ensure_ascii=False, separators=(",",":")
        ).replace("</","<\\/") + "</script>"
    return f"""<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{esc(title)}</title>
<meta name="description" content="{esc(description)}">
<meta name="robots" content="{robot}">
<link rel="canonical" href="{esc(canonical)}">
<meta property="og:type" content="website">
<script async src="https://pagead2.googlesyndication.com/pagead/js/adsbygoogle.js?client=ca-pub-5776658615046901" crossorigin="anonymous"></script>
<meta property="og:site_name" content="{SITE_NAME}">
<meta property="og:title" content="{esc(title)}">
<meta property="og:description" content="{esc(description)}">
<meta property="og:url" content="{esc(canonical)}">
<meta name="twitter:card" content="summary">
<style>{CSS}{ACTIVITY_CSS}</style>
{json_ld_html}
<script src="/analytics.js"></script>
</head>
<body>
<header class="site-header">
  <div class="wrap header-inner">
    <a class="brand" href="/">{SITE_NAME}<small>OPEN / CLOSE DATABASE</small></a>
    <nav class="nav">
      <a href="/area/北海道">エリアから探す</a>
      <a href="/category/飲食">業種から探す</a>
      <a href="/#latest">最新情報</a>
      <a class="search-chip" href="/">店舗を検索</a>
    </nav>
  </div>
</header>
{body}
<footer class="footer">
  <div class="wrap">
    <strong>{SITE_NAME}</strong>
    全国の開店・閉店・店舗情報をエリア・業種ごとに確認できる店舗情報データベースです。
    <div class="footer-links">
      <a href="/">トップ</a><a href="/area/北海道">エリア</a><a href="/category/飲食">業種</a><a href="/about.html">運営・編集方針</a><a href="/privacy.html">プライバシー</a><a href="/contact.html">お問い合わせ・訂正</a>
    </div>
  </div>
</footer>
</body>
</html>"""

def store_cards(rows):
    if not rows:
        return '<div class="empty">現在掲載できる店舗情報はありません。</div>'
    out = ['<div class="store-list">']
    for r in rows:
        d = row_to_dict(r)
        ev = event_date(d)
        label, cls = status_info(d["status"], ev)
        area = " ".join(x for x in [d["prefecture"], d["city"]] if x)
        cat = d["category"] or "業種未分類"
        out.append(f"""
<a class="store-card" href="/store/{d['id']}" data-ga-event="store_select" data-ga-store-id="{d['id']}" data-ga-status="{esc(d['status'] or '')}" data-ga-category="{esc(cat)}">
  <div class="store-date">{fmt_date(ev)}</div>
  <div class="store-main">
    <span class="badge {cls}">{esc(label)}</span>
    <h3>{esc(d['name'])}</h3>
    <p>{esc(area)}　・　{esc(cat)}</p>
  </div>
  <div class="arrow">›</div>
</a>""")
    out.append("</div>")
    return "".join(out)

def breadcrumb_json(origin, items):
    return {
        "@context":"https://schema.org",
        "@type":"BreadcrumbList",
        "itemListElement":[
            {"@type":"ListItem","position":i+1,"name":name,"item":site_url(origin,path)}
            for i,(name,path) in enumerate(items)
        ]
    }

def render_area(database_url, origin, prefecture, city=None, page=1, per_page=40, status='all'):
    prefecture = (prefecture or "").strip()
    city = (city or "").strip() or None
    page = max(1,int(page))
    status = (status or "all").strip().lower()
    if status not in ("all","opening","closing"):
        status = "all"
    offset = (page-1)*per_page

    with _connect(database_url) as conn:
        with conn.cursor() as cur:
            clauses = ["COALESCE(status,'') <> 'excluded'","prefecture=%s"]
            params = [prefecture]
            if city:
                clauses.append("city=%s")
                params.append(city)

            if status == "opening":
                clauses.append("status IN('open','opening')")
            elif status == "closing":
                clauses.append("status IN('closed','closing')")

            where = " AND ".join(clauses)

            stats_clauses = ["COALESCE(status,'') <> 'excluded'","prefecture=%s"]
            stats_params = [prefecture]
            if city:
                stats_clauses.append("city=%s")
                stats_params.append(city)
            stats_where = " AND ".join(stats_clauses)

            cur.execute(f"""
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER(WHERE status IN('open','opening')),
                    COUNT(*) FILTER(WHERE status IN('closed','closing'))
                FROM stores WHERE {stats_where}
            """,stats_params)
            all_total,opens,closes = cur.fetchone()

            cur.execute(f"SELECT COUNT(*) FROM stores WHERE {where}",params)
            total = cur.fetchone()[0]

            cur.execute(STORE_SELECT + f"""
                WHERE {where}
                ORDER BY COALESCE(open_date,close_date,created_at::date) DESC NULLS LAST,id DESC
                LIMIT %s OFFSET %s
            """,params+[per_page,offset])
            rows = cur.fetchall()

            if city:
                cur.execute("""
                    SELECT COALESCE(category,'業種未分類'),COUNT(*)
                    FROM stores
                    WHERE COALESCE(status,'') <> 'excluded'
                      AND prefecture=%s AND city=%s
                    GROUP BY COALESCE(category,'業種未分類')
                    ORDER BY COUNT(*) DESC,1
                    LIMIT 18
                """,(prefecture,city))
                facets = cur.fetchall()
                facet_type = "category"
            else:
                cur.execute("""
                    SELECT city,COUNT(*)
                    FROM stores
                    WHERE COALESCE(status,'') <> 'excluded'
                      AND prefecture=%s
                      AND city IS NOT NULL AND city<>''
                    GROUP BY city
                    ORDER BY COUNT(*) DESC,city
                    LIMIT 30
                """,(prefecture,))
                facets = cur.fetchall()
                facet_type = "city"

            area_clauses = ["COALESCE(status,'') <> 'excluded'","prefecture=%s"]
            area_params = [prefecture]
            if city:
                area_clauses.append("city=%s")
                area_params.append(city)
            area_where = " AND ".join(area_clauses)

            cur.execute(f"""
                WITH months AS (
                    SELECT generate_series(
                        date_trunc('month',CURRENT_DATE) - INTERVAL '11 months',
                        date_trunc('month',CURRENT_DATE),
                        INTERVAL '1 month'
                    )::date AS month_start
                ),
                open_events AS (
                    SELECT date_trunc('month',open_date)::date AS month_start,COUNT(*) AS cnt
                    FROM stores
                    WHERE {area_where}
                      AND open_date IS NOT NULL
                      AND open_date >= date_trunc('month',CURRENT_DATE) - INTERVAL '11 months'
                      AND open_date < date_trunc('month',CURRENT_DATE) + INTERVAL '1 month'
                    GROUP BY 1
                ),
                close_events AS (
                    SELECT date_trunc('month',close_date)::date AS month_start,COUNT(*) AS cnt
                    FROM stores
                    WHERE {area_where}
                      AND close_date IS NOT NULL
                      AND close_date >= date_trunc('month',CURRENT_DATE) - INTERVAL '11 months'
                      AND close_date < date_trunc('month',CURRENT_DATE) + INTERVAL '1 month'
                    GROUP BY 1
                )
                SELECT
                    m.month_start,
                    COALESCE(o.cnt,0) AS opens,
                    COALESCE(c.cnt,0) AS closes
                FROM months m
                LEFT JOIN open_events o USING(month_start)
                LEFT JOIN close_events c USING(month_start)
                ORDER BY m.month_start
            """,area_params + area_params)
            monthly_trend = cur.fetchall()

            cur.execute(f"""
                SELECT
                    COALESCE(NULLIF(category,''),'業種未分類') AS category,
                    COUNT(*) FILTER(
                        WHERE open_date IS NOT NULL
                          AND open_date >= CURRENT_DATE - INTERVAL '365 days'
                          AND open_date <= CURRENT_DATE
                    ) AS open_count,
                    COUNT(*) FILTER(
                        WHERE close_date IS NOT NULL
                          AND close_date >= CURRENT_DATE - INTERVAL '365 days'
                          AND close_date <= CURRENT_DATE
                    ) AS close_count
                FROM stores
                WHERE {area_where}
                GROUP BY 1
                HAVING
                    COUNT(*) FILTER(
                        WHERE open_date IS NOT NULL
                          AND open_date >= CURRENT_DATE - INTERVAL '365 days'
                          AND open_date <= CURRENT_DATE
                    )
                    +
                    COUNT(*) FILTER(
                        WHERE close_date IS NOT NULL
                          AND close_date >= CURRENT_DATE - INTERVAL '365 days'
                          AND close_date <= CURRENT_DATE
                    ) > 0
                ORDER BY
                    (
                        COUNT(*) FILTER(
                            WHERE open_date IS NOT NULL
                              AND open_date >= CURRENT_DATE - INTERVAL '365 days'
                              AND open_date <= CURRENT_DATE
                        )
                        +
                        COUNT(*) FILTER(
                            WHERE close_date IS NOT NULL
                              AND close_date >= CURRENT_DATE - INTERVAL '365 days'
                              AND close_date <= CURRENT_DATE
                        )
                    ) DESC,
                    category
                LIMIT 10
            """,area_params)
            category_changes = cur.fetchall()

    area_name = f"{prefecture}{city or ''}"
    if city:
        title = f"{city}の開店・閉店情報｜最新店舗一覧｜{SITE_NAME}"
        desc = f"{prefecture}{city}の開店・閉店情報を掲載。新規オープン、閉店予定、閉店した店舗を住所・日付・業種から確認できます。"
        canonical_path = f"/area/{qpath(prefecture)}/{qpath(city)}"
        crumbs = [("トップ","/"),(prefecture,f"/area/{qpath(prefecture)}"),(city,canonical_path)]
    else:
        title = f"{prefecture}の開店・閉店情報｜新店・閉店一覧｜{SITE_NAME}"
        desc = f"{prefecture}の開店・閉店情報を市区町村別に掲載。新規オープン、閉店予定、閉店した店舗の最新情報を確認できます。"
        canonical_path = f"/area/{qpath(prefecture)}"
        crumbs = [("トップ","/"),(prefecture,canonical_path)]

    base_canonical = canonical_path

    if status == "opening":
        title = title.replace("開店・閉店情報","開店情報")
        desc = desc.replace("開店・閉店情報","開店情報")
    elif status == "closing":
        title = title.replace("開店・閉店情報","閉店情報")
        desc = desc.replace("開店・閉店情報","閉店情報")

    query_parts = []
    if status != "all":
        query_parts.append(f"status={status}")
    if page > 1:
        title = f"{title}（{page}ページ）"
        query_parts.append(f"page={page}")
    if query_parts:
        canonical_path += "?" + "&".join(query_parts)

    facet_html = []
    for name,count in facets:
        if not name:
            continue
        href = f"/area/{qpath(prefecture)}/{qpath(name)}" if facet_type=="city" else f"/category/{qpath(name)}"
        facet_html.append(f'<a class="facet" href="{href}">{esc(name)} <strong>{count}</strong></a>')

    prev_next = []
    base_path = f"/area/{qpath(prefecture)}" + (f"/{qpath(city)}" if city else "")

    def page_href(target_page):
        qs=[]
        if status != "all":
            qs.append(f"status={status}")
        if target_page > 1:
            qs.append(f"page={target_page}")
        return base_path + (("?"+"&".join(qs)) if qs else "")

    if page > 1:
        prev_next.append(f'<a href="{page_href(page-1)}">‹ 前へ</a>')
    if offset + len(rows) < total:
        prev_next.append(f'<a href="{page_href(page+1)}">次へ ›</a>')

    open_12m = sum(int(r[1] or 0) for r in monthly_trend)
    close_12m = sum(int(r[2] or 0) for r in monthly_trend)
    net_12m = open_12m - close_12m
    observed_12m = open_12m + close_12m

    if observed_12m < 5:
        trend_text = "データ蓄積中"
        trend_class = "flat"
        trend_copy = "直近12か月のイベント件数がまだ少ないため、傾向は参考値です。"
    elif net_12m >= max(2,round(observed_12m * 0.12)):
        trend_text = "開店優勢"
        trend_class = "up"
        trend_copy = "直近12か月では、閉店より開店の確認件数が多い傾向です。"
    elif net_12m <= -max(2,round(observed_12m * 0.12)):
        trend_text = "閉店優勢"
        trend_class = "down"
        trend_copy = "直近12か月では、開店より閉店の確認件数が多い傾向です。"
    else:
        trend_text = "ほぼ均衡"
        trend_class = "flat"
        trend_copy = "直近12か月の開店・閉店件数は、おおむね均衡しています。"

    max_month = max(
        [int(r[1] or 0) for r in monthly_trend]
        + [int(r[2] or 0) for r in monthly_trend]
        + [1]
    )

    month_cols=[]
    for month_start,mopen,mclose in monthly_trend:
        mopen=int(mopen or 0)
        mclose=int(mclose or 0)
        oh=max(2,round((mopen/max_month)*128)) if mopen else 2
        ch=max(2,round((mclose/max_month)*128)) if mclose else 2
        month_cols.append(f"""
          <div class="month-col" title="{month_start.year}年{month_start.month}月 開店{mopen}件 / 閉店{mclose}件">
            <div class="month-bars">
              <span class="month-bar open" style="height:{oh}px;opacity:{'1' if mopen else '.18'}"></span>
              <span class="month-bar close" style="height:{ch}px;opacity:{'1' if mclose else '.18'}"></span>
            </div>
            <span class="month-name">{month_start.month}月</span>
          </div>
        """)

    # --- Combined chart: bars show monthly volume, lines show direction/trend
    chart_w=760
    chart_h=250
    pad_l=34
    pad_r=18
    pad_t=18
    pad_b=36
    plot_w=chart_w-pad_l-pad_r
    plot_h=chart_h-pad_t-pad_b

    combo_max=max(
        [int(r[1] or 0) for r in monthly_trend]
        + [int(r[2] or 0) for r in monthly_trend]
        + [1]
    )

    y_max=max(4,combo_max)
    if y_max<=10:
        y_max=((y_max+1)//2)*2
    else:
        y_max=((y_max+4)//5)*5

    step=plot_w/max(1,len(monthly_trend))
    group_w=min(36,step*.72)
    bar_w=max(5,(group_w-4)/2)

    def cx(i):
        return pad_l + step*(i+.5)

    def sy(v):
        return pad_t + plot_h - (plot_h*(float(v)/float(y_max)))

    open_points=[]
    close_points=[]
    bars=[]
    dots=[]
    x_labels=[]

    for i,(month_start,mopen,mclose) in enumerate(monthly_trend):
        mopen=int(mopen or 0)
        mclose=int(mclose or 0)
        x=cx(i)
        yo=sy(mopen)
        yc=sy(mclose)
        base=pad_t+plot_h

        ox=x-bar_w-2
        cx2=x+2

        bars.append(
            f'<rect class="combo-open-bar" x="{ox:.1f}" y="{yo:.1f}" width="{bar_w:.1f}" height="{max(1,base-yo):.1f}" rx="2">'
            f'<title>{month_start.year}年{month_start.month}月 開店 {mopen}件</title></rect>'
        )
        bars.append(
            f'<rect class="combo-close-bar" x="{cx2:.1f}" y="{yc:.1f}" width="{bar_w:.1f}" height="{max(1,base-yc):.1f}" rx="2">'
            f'<title>{month_start.year}年{month_start.month}月 閉店 {mclose}件</title></rect>'
        )

        open_x=ox+bar_w/2
        close_x=cx2+bar_w/2
        open_points.append(f"{open_x:.1f},{yo:.1f}")
        close_points.append(f"{close_x:.1f},{yc:.1f}")

        dots.append(
            f'<circle class="combo-open-dot" cx="{open_x:.1f}" cy="{yo:.1f}" r="3.5">'
            f'<title>{month_start.year}年{month_start.month}月 開店 {mopen}件</title></circle>'
        )
        dots.append(
            f'<circle class="combo-close-dot" cx="{close_x:.1f}" cy="{yc:.1f}" r="3.5">'
            f'<title>{month_start.year}年{month_start.month}月 閉店 {mclose}件</title></circle>'
        )

        x_labels.append(
            f'<text class="combo-axis" x="{x:.1f}" y="{chart_h-11}" text-anchor="middle">{month_start.month}月</text>'
        )

    grid_lines=[]
    y_labels=[]
    for j in range(5):
        val=round(y_max*(4-j)/4)
        y=pad_t+plot_h*j/4
        grid_lines.append(
            f'<line class="combo-grid" x1="{pad_l}" y1="{y:.1f}" x2="{chart_w-pad_r}" y2="{y:.1f}"></line>'
        )
        y_labels.append(
            f'<text class="combo-axis" x="{pad_l-7}" y="{y+3:.1f}" text-anchor="end">{val}</text>'
        )

    combo_chart_html=f"""
    <div class="combo-chart-wrap">
      <div class="combo-chart-title">12か月の開店・閉店動向</div>
      <svg class="combo-svg" viewBox="0 0 {chart_w} {chart_h}" role="img" aria-label="{esc(area_name)}の直近12か月の開店・閉店動向">
        {''.join(grid_lines)}
        {''.join(y_labels)}
        {''.join(x_labels)}
        {''.join(bars)}
        <polyline class="combo-open-line" points="{' '.join(open_points)}"></polyline>
        <polyline class="combo-close-line" points="{' '.join(close_points)}"></polyline>
        {''.join(dots)}
      </svg>
      <div class="chart-legend">
        <span><i class="legend-dot open"></i>開店</span>
        <span><i class="legend-dot close"></i>閉店</span>
        <span style="margin-left:auto">棒＝件数 / 線＝推移</span>
      </div>
    </div>
    """

    category_rows=[]
    excluded_categories={"業種未分類","未分類","小売","飲食店"}
    for cat,copen,cclose in category_changes:
        if not cat or cat in excluded_categories:
            continue
        copen=int(copen or 0)
        cclose=int(cclose or 0)
        delta=copen-cclose
        delta_class="up" if delta>0 else "down" if delta<0 else "flat"
        delta_text=f"+{delta}" if delta>0 else str(delta)
        category_rows.append(f"""
          <div class="category-change">
            <div class="category-change-name">{esc(cat)}</div>
            <div class="category-change-num">{copen}件</div>
            <div class="category-change-num">{cclose}件</div>
            <div class="category-delta {delta_class}">{delta_text}</div>
          </div>
        """)
        if len(category_rows)>=8:
            break

    scope_word = "この街" if city else "このエリア"
    insight_html=f"""
    <section class="panel">
      <div class="section-head">
        <div>
          <h2>{scope_word}の開店・閉店傾向</h2>
          <p>直近12か月の掲載データ</p>
        </div>
      </div>

      <span class="trend-label {trend_class}">{trend_text}</span>
      <p class="side-copy" style="margin:0 0 12px">{trend_copy}</p>

      <div class="insight-summary">
        <div class="insight-kpi"><span>直近12か月の開店</span><strong>{open_12m}</strong><small>件</small></div>
        <div class="insight-kpi"><span>直近12か月の閉店</span><strong>{close_12m}</strong><small>件</small></div>
        <div class="insight-kpi"><span>開店 − 閉店</span><strong>{'+' if net_12m>0 else ''}{net_12m}</strong><small>件</small></div>
      </div>

      {combo_chart_html}

      <div class="insight-note">
        ※ 開店閉店マップに登録された日付情報から算出した参考値です。実際の地域内すべての店舗数や景況を示すものではありません。
      </div>
    </section>

    <section class="panel">
      <div class="section-head">
        <div><h2>業種別増減</h2><p>直近1年の「開店件数 − 閉店件数」</p></div>
      </div>

      {
        (
          '<div class="category-head"><span>業種</span><span>開店</span><span>閉店</span><span>増減</span></div>'
          + '<div class="category-change-list">' + ''.join(category_rows) + '</div>'
        )
        if category_rows
        else '<div class="empty">業種別の増減を表示できるデータを蓄積中です。</div>'
      }

      <div class="insight-note">
        ※ 「増減」は在庫店舗数そのものではなく、直近1年間に確認できた開店件数から閉店件数を引いた値です。
      </div>
    </section>
    """

    body = f"""
<section class="hero">
  <div class="wrap">
    <div class="crumbs">トップ　›　{esc(prefecture)}{('　›　'+esc(city)) if city else ''}</div>
    <h1>{esc(area_name)}の開店・閉店情報</h1>
    <p>{esc(area_name)}で確認された新規オープン、開店予定、閉店予定、閉店店舗の情報をまとめています。</p>
    <div class="stats">
      <div class="stat"><strong>{all_total}</strong><span>掲載店舗情報</span></div>
      <div class="stat"><strong>{opens}</strong><span>開店・開店予定</span></div>
      <div class="stat"><strong>{closes}</strong><span>閉店・閉店予定</span></div>
    </div>
  </div>
</section>
<div class="wrap layout">
  <main>
    {insight_html}
    <section class="panel">
      <div class="section-head">
        <div><h2>{
          "最新の開店情報" if status=="opening"
          else "最新の閉店情報" if status=="closing"
          else "最新の店舗情報"
        }</h2><p>{esc(area_name)}の開店・閉店情報</p></div>
        <p>{total}件中 {offset+1 if total else 0}〜{min(offset+len(rows),total)}件</p>
      </div>

      <div class="status-tabs" role="tablist" aria-label="開店・閉店の切り替え">
        <a class="status-tab {'active' if status=='all' else ''}" href="{base_path}" role="tab" aria-selected="{'true' if status=='all' else 'false'}">すべて</a>
        <a class="status-tab opening {'active' if status=='opening' else ''}" href="{base_path}?status=opening" role="tab" aria-selected="{'true' if status=='opening' else 'false'}">開店</a>
        <a class="status-tab closing {'active' if status=='closing' else ''}" href="{base_path}?status=closing" role="tab" aria-selected="{'true' if status=='closing' else 'false'}">閉店</a>
      </div>

      {store_cards(rows)}
      <div class="pagination">{''.join(prev_next)}</div>
    </section>
  </main>
  <aside class="side">
    <section class="panel">
      <h2 class="side-title">{'業種から探す' if city else '市区町村から探す'}</h2>
      <p class="side-copy">{esc(area_name)}の情報をさらに絞り込めます。</p>
      <div class="facets">{''.join(facet_html) if facet_html else '<span class="side-copy">分類データを蓄積中です。</span>'}</div>
    </section>
    <section class="panel">
      <h2 class="side-title">掲載情報について</h2>
      <p class="side-copy">店舗の開店・閉店状況は変更される場合があります。最終確認は店舗公式情報・掲載元をご確認ください。</p>
    </section>
  </aside>
</div>"""
    return page_shell(origin,title,desc,canonical_path,body,breadcrumb_json(origin,crumbs),noindex=(total==0))

def render_category(database_url, origin, category, page=1, per_page=40, status='all'):
    category = (category or "").strip()
    page = max(1,int(page))
    status = (status or "all").strip().lower()
    if status not in ("all","opening","closing"):
        status = "all"
    offset = (page-1)*per_page

    with _connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER(WHERE status IN('open','opening')),
                    COUNT(*) FILTER(WHERE status IN('closed','closing'))
                FROM stores
                WHERE COALESCE(status,'') <> 'excluded'
                  AND COALESCE(category,'業種未分類')=%s
            """,(category,))
            all_total,opens,closes = cur.fetchone()

            status_sql=""
            if status=="opening":
                status_sql=" AND status IN('open','opening')"
            elif status=="closing":
                status_sql=" AND status IN('closed','closing')"

            cur.execute(
                "SELECT COUNT(*) FROM stores "
                "WHERE COALESCE(status,'') <> 'excluded' "
                "AND COALESCE(category,'業種未分類')=%s" + status_sql,
                (category,)
            )
            total=cur.fetchone()[0]

            cur.execute(STORE_SELECT + """
                WHERE COALESCE(status,'') <> 'excluded'
                  AND COALESCE(category,'業種未分類')=%s
            """ + status_sql + """
                ORDER BY COALESCE(open_date,close_date,created_at::date) DESC NULLS LAST,id DESC
                LIMIT %s OFFSET %s
            """,(category,per_page,offset))
            rows = cur.fetchall()

            cur.execute("""
                SELECT prefecture,COUNT(*)
                FROM stores
                WHERE COALESCE(status,'') <> 'excluded'
                  AND COALESCE(category,'業種未分類')=%s
                  AND prefecture IS NOT NULL AND prefecture<>''
                GROUP BY prefecture
                ORDER BY COUNT(*) DESC,prefecture
                LIMIT 30
            """,(category,))
            prefs = cur.fetchall()

    canonical_path = f"/category/{qpath(category)}"
    title = f"{category}の開店・閉店情報｜全国の新店・閉店一覧｜{SITE_NAME}"

    if status=="opening":
        title=f"{category}の開店情報｜全国の新店一覧｜{SITE_NAME}"
    elif status=="closing":
        title=f"{category}の閉店情報｜全国の閉店一覧｜{SITE_NAME}"

    query_parts=[]
    if status!="all":
        query_parts.append(f"status={status}")
    if page > 1:
        title += f"（{page}ページ）"
        query_parts.append(f"page={page}")
    if query_parts:
        canonical_path += "?" + "&".join(query_parts)
    desc = f"全国の「{category}」に関する開店・閉店情報。新規オープン、開店予定、閉店予定、閉店店舗をエリア別に確認できます。"

    facets = "".join(
        f'<a class="facet" href="/area/{qpath(pref)}">{esc(pref)} <strong>{count}</strong></a>'
        for pref,count in prefs if pref
    )
    prev_next=[]
    cat_base=f"/category/{qpath(category)}"

    def cat_page_href(target_page):
        qs=[]
        if status!="all":
            qs.append(f"status={status}")
        if target_page>1:
            qs.append(f"page={target_page}")
        return cat_base + (("?"+"&".join(qs)) if qs else "")

    if page>1:
        prev_next.append(f'<a href="{cat_page_href(page-1)}">‹ 前へ</a>')
    if offset+len(rows)<total:
        prev_next.append(f'<a href="{cat_page_href(page+1)}">次へ ›</a>')

    body=f"""
<section class="hero">
  <div class="wrap">
    <div class="crumbs">トップ　›　業種　›　{esc(category)}</div>
    <h1>{esc(category)}の開店・閉店情報</h1>
    <p>全国の{esc(category)}に関する新規オープン、開店予定、閉店予定、閉店店舗を一覧で確認できます。</p>
    <div class="stats">
      <div class="stat"><strong>{all_total}</strong><span>掲載店舗情報</span></div>
      <div class="stat"><strong>{opens}</strong><span>開店・開店予定</span></div>
      <div class="stat"><strong>{closes}</strong><span>閉店・閉店予定</span></div>
    </div>
  </div>
</section>
<div class="wrap layout">
  <main>
    <section class="panel">
      <div class="section-head">
        <div><h2>{
          "最新の"+esc(category)+"開店情報" if status=="opening"
          else "最新の"+esc(category)+"閉店情報" if status=="closing"
          else "最新の"+esc(category)+"情報"
        }</h2><p>全国の店舗動向</p></div>
        <p>{total}件中 {offset+1 if total else 0}〜{min(offset+len(rows),total)}件</p>
      </div>

      <div class="status-tabs" role="tablist" aria-label="開店・閉店の切り替え">
        <a class="status-tab {'active' if status=='all' else ''}" href="{cat_base}">すべて</a>
        <a class="status-tab opening {'active' if status=='opening' else ''}" href="{cat_base}?status=opening">開店</a>
        <a class="status-tab closing {'active' if status=='closing' else ''}" href="{cat_base}?status=closing">閉店</a>
      </div>

      {store_cards(rows)}
      <div class="pagination">{''.join(prev_next)}</div>
    </section>
  </main>
  <aside class="side">
    <section class="panel">
      <h2 class="side-title">エリアから探す</h2>
      <p class="side-copy">{esc(category)}の掲載件数がある都道府県です。</p>
      <div class="facets">{facets or '<span class="side-copy">地域データを蓄積中です。</span>'}</div>
    </section>
  </aside>
</div>"""
    crumbs=[("トップ","/"),("業種","/"),(category,canonical_path.split("?")[0])]
    return page_shell(origin,title,desc,canonical_path,body,breadcrumb_json(origin,crumbs),noindex=(total==0))

def render_store(database_url, origin, store_id):
    with _connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(STORE_SELECT + """
                WHERE id=%s AND COALESCE(status,'') <> 'excluded'
            """,(store_id,))
            r=cur.fetchone()
            if not r:
                return None
            d=row_to_dict(r)

            cur.execute("""
                SELECT id,name,status,category,prefecture,city,address,
                       facility_name,floor,postal_code,open_date,close_date,
                       source_url,source_name,official_url,confidence,last_verified_at
                FROM stores
                WHERE id<>%s
                  AND COALESCE(status,'') <> 'excluded'
                  AND COALESCE(prefecture,'')=COALESCE(%s,'')
                  AND COALESCE(city,'')=COALESCE(%s,'')
                ORDER BY COALESCE(open_date,close_date,created_at::date) DESC NULLS LAST,id DESC
                LIMIT 6
            """,(store_id,d["prefecture"],d["city"]))
            nearby=cur.fetchall()

            tenant_rows=[]
            cur.execute("""
                SELECT source_name,source_url,status,last_verified_at,confidence,match_method
                FROM tenant_listings
                WHERE store_id=%s
                ORDER BY
                    CASE WHEN status='detected' THEN 0 ELSE 1 END,
                    last_verified_at DESC NULLS LAST,id DESC
                LIMIT 8
            """,(store_id,))
            tenant_rows=cur.fetchall()

            history=[]
            if d["address"]:
                cur.execute("""
                    SELECT id,name,status,open_date,close_date,category
                    FROM stores
                    WHERE COALESCE(status,'') <> 'excluded'
                      AND address=%s
                    ORDER BY
                        COALESCE(open_date,close_date,created_at::date) ASC NULLS LAST,
                        id ASC
                    LIMIT 30
                """,(d["address"],))
                history=cur.fetchall()

            cur.execute("""
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER(
                        WHERE status IN('open','opening')
                          AND open_date IS NOT NULL
                          AND open_date >= CURRENT_DATE - INTERVAL '365 days'
                          AND open_date <= CURRENT_DATE
                    ),
                    COUNT(*) FILTER(
                        WHERE status IN('closed','closing')
                          AND close_date IS NOT NULL
                          AND close_date >= CURRENT_DATE - INTERVAL '365 days'
                          AND close_date <= CURRENT_DATE
                    ),
                    COUNT(*) FILTER(
                        WHERE status='opening'
                          AND open_date IS NOT NULL
                          AND open_date > CURRENT_DATE
                    ),
                    COUNT(*) FILTER(
                        WHERE status='closing'
                          AND close_date IS NOT NULL
                          AND close_date > CURRENT_DATE
                    )
                FROM stores
                WHERE COALESCE(status,'') <> 'excluded'
                  AND COALESCE(prefecture,'')=COALESCE(%s,'')
                  AND COALESCE(city,'')=COALESCE(%s,'')
            """,(d["prefecture"],d["city"]))
            activity_row=cur.fetchone()

    activity=compute_activity_score(*(activity_row or (0,0,0,0,0)))

    ev=event_date(d)
    label,cls=status_info(d["status"],ev)
    area="".join(x for x in [d["prefecture"],d["city"]] if x)
    title=f"{d['name']}の開店・閉店情報｜{area or '店舗情報'}｜{SITE_NAME}"
    desc=f"{area}の「{d['name']}」の{label}情報。所在地、日付、業種、掲載元などを確認できます。"
    canonical_path=f"/store/{d['id']}"

    rows=[
        ("店舗名",d["name"]),("状況",label),("日付",fmt_date(ev)),
        ("業種",d["category"] or "未分類"),("所在地",d["address"] or area or "未確認")
    ]
    if d["facility_name"]: rows.append(("施設名",d["facility_name"]))
    if d["floor"]: rows.append(("階",d["floor"]))
    if d["postal_code"]: rows.append(("郵便番号",d["postal_code"]))
    if d["last_verified_at"]:
        v=d["last_verified_at"]
        rows.append(("最終確認",v.strftime("%Y年%m月%d日") if hasattr(v,"strftime") else str(v)[:10]))

    detail_html="".join(f"<dt>{esc(k)}</dt><dd>{esc(v)}</dd>" for k,v in rows)

    links=""
    if d["official_url"]:
        links += f'<a class="official-link" href="{esc(d["official_url"])}" target="_blank" rel="noopener">公式情報を確認 ↗</a>'
    if d["source_url"]:
        links += f'<a class="source-link" href="{esc(d["source_url"])}" target="_blank" rel="noopener" data-ga-event="source_link_click" data-ga-store-id="{d["id"]}">掲載元：{esc(d["source_name"] or "掲載元")} ↗</a>'

    map_html=""
    if d["address"]:
        map_src="https://www.google.com/maps?q="+quote(d["address"])+"&output=embed"
        map_html=f'<div class="map"><iframe loading="lazy" referrerpolicy="no-referrer-when-downgrade" src="{esc(map_src)}" title="{esc(d["name"])}の地図"></iframe></div>'

    google_maps_html=""
    tenant_search_html=""
    if d["address"]:
        maps_query=f'{d["name"]} {d["address"]}'
        maps_url="https://www.google.com/maps/search/?api=1&query="+quote_plus(maps_query)
        google_maps_html=f"""
        <div class="action-row">
          <a class="action-link primary" href="{esc(maps_url)}" target="_blank" rel="noopener" data-ga-event="google_maps_click" data-ga-store-id="{d['id']}">
            Google Mapsで口コミ・周辺写真を見る ↗
          </a>
        </div>
        <div class="notice">
          Google Maps・ストリートビューの撮影時期や位置により、現在の店舗外観・看板・入口と異なる場合があります。
        </div>
        """

        tenant_query=f'"{d["address"]}" テナント募集 貸店舗 居抜き'
        tenant_search_url="https://www.google.com/search?q="+quote_plus(tenant_query)
        tenant_search_html=f'<a class="action-link" href="{esc(tenant_search_url)}" target="_blank" rel="noopener" data-ga-event="tenant_search_click" data-ga-store-id="{d["id"]}">この住所のテナント情報をWeb検索 ↗</a>'

    tenant_html=""
    if tenant_rows:
        parts=[]
        for source_name,source_url,tstatus,last_verified,confidence,match_method in tenant_rows:
            state="募集情報を確認" if tstatus=="detected" else "現在は募集情報を確認できません"
            checked=last_verified.strftime("%Y年%m月%d日") if hasattr(last_verified,"strftime") else (str(last_verified)[:10] if last_verified else "未確認")
            link=(
                f'<a class="source-link" href="{esc(source_url)}" target="_blank" rel="noopener" data-ga-event="tenant_source_click" data-ga-store-id="{d["id"]}">'
                f'{esc(source_name or "掲載元")}を確認 ↗</a>'
                if source_url else ""
            )
            parts.append(f"""
            <div class="tenant-box">
              <strong>{esc(state)}</strong>
              <div class="tenant-meta">最終確認：{esc(checked)}{("　・　照合確度 "+str(confidence)+"%") if confidence else ""}</div>
              {link}
            </div>
            """)
        tenant_html="".join(parts)
    else:
        tenant_html='<div class="tenant-box"><strong>テナント募集情報は現在確認できていません</strong><div class="tenant-meta">公開情報を定期確認しています。募集が見つかった場合に掲載します。</div></div>'

    history_html=""
    if history and len(history)>1:
        parts=[]
        for hid,hname,hstatus,hopen,hclose,hcat in history:
            hev=hclose if hstatus in ("closing","closed") else hopen
            hlabel,_=status_info(hstatus,hev)
            parts.append(f"""
            <div class="timeline-item">
              <div class="timeline-date">{fmt_date(hev)}</div>
              <div class="timeline-name"><a href="/store/{hid}">{esc(hname)}</a></div>
              <div class="timeline-status">{esc(hlabel)}{("　・　"+esc(hcat)) if hcat else ""}</div>
            </div>
            """)
        history_html='<div class="timeline">'+"".join(parts)+'</div>'
    elif d["address"]:
        history_html='<div class="empty">この住所では、現在ほかの店舗履歴を確認できていません。</div>'

    activity_html=f"""
    <div class="activity-card">
      <div class="activity-top">
        <div>
          <div class="activity-label">{esc(activity["label"])}</div>
          <div style="font-size:12px;color:var(--muted);margin-top:5px">{esc(activity["comment"])}</div>
        </div>
        <div class="activity-score">{activity["score"]}<small>/100</small></div>
      </div>
      <div class="activity-meter"><span style="width:{activity["score"]}%"></span></div>
      <div class="activity-counts">
        <div class="activity-count"><strong>{activity["counts"]["recent_open"]}</strong><span>直近1年の開店</span></div>
        <div class="activity-count"><strong>{activity["counts"]["recent_close"]}</strong><span>直近1年の閉店</span></div>
        <div class="activity-count"><strong>{activity["counts"]["planned_open"]}</strong><span>今後の開店予定</span></div>
        <div class="activity-count"><strong>{activity["counts"]["planned_close"]}</strong><span>今後の閉店予定</span></div>
      </div>
    </div>
    """

    area_links=[]
    if d["prefecture"]:
        area_links.append(f'<a class="facet" href="/area/{qpath(d["prefecture"])}">{esc(d["prefecture"])}</a>')
    if d["prefecture"] and d["city"]:
        area_links.append(f'<a class="facet" href="/area/{qpath(d["prefecture"])}/{qpath(d["city"])}">{esc(d["city"])}</a>')
    if d["category"]:
        area_links.append(f'<a class="facet" href="/category/{qpath(d["category"])}">{esc(d["category"])}</a>')

    body=f"""
<section class="hero">
  <div class="wrap">
    <div class="crumbs">トップ　›　{esc(d["prefecture"] or "全国")}　›　{esc(d["city"] or "")}</div>
    <span class="badge {cls}">{esc(label)}</span>
    <h1 style="margin-top:12px">{esc(d["name"])}</h1>
    <p>{esc(area)}の店舗情報。{fmt_date(ev)}の{esc(label)}情報として掲載しています。</p>
  </div>
</section>
<div class="wrap layout">
  <main>
    <section class="panel">
      <div class="section-head"><div><h2>店舗情報</h2><p>{esc(d["name"])}</p></div></div>
      {'<p class="notice">店舗名の抽出結果を確認中です。業種・開閉店日などの断定表示を停止しています。掲載元をご確認ください。</p>' if d.get("quality_pending") else ""}
      <dl class="detail-grid">{detail_html}</dl>
      {links}
      {map_html}
      {google_maps_html}
      <div class="notice">掲載内容は確認時点の情報です。営業状況・開閉店日などは変更される場合があるため、必要に応じて公式情報・掲載元をご確認ください。</div>
    </section>

    <section class="panel">
      <div class="section-head"><div><h2>周辺活力度</h2><p>{esc(area)}の開店・閉店動向から算出</p></div></div>
      {activity_html}
      <div class="notice">この指標は人流・売上・通行量を示すものではありません。開店閉店マップに蓄積された店舗の開店・閉店情報をもとにした参考値です。データ量により数値は変動します。</div>
    </section>

    <section class="panel">
      <div class="section-head"><div><h2>この場所の店舗履歴</h2><p>{esc(d["address"] or area)}</p></div></div>
      {history_html}
    </section>

    <section class="panel">
      <div class="section-head"><div><h2>閉店後・テナント情報</h2><p>貸店舗・居抜き・テナント募集の公開情報</p></div></div>
      {tenant_html}
      <div class="action-row">{tenant_search_html}</div>
      <div class="notice">募集情報が見つからなくなった場合も「成約済み」とは断定せず、「現在は募集情報を確認できません」と表示します。</div>
    </section>

    <section class="panel">
      <div class="section-head"><div><h2>周辺の開店・閉店情報</h2><p>{esc(area)}の関連店舗</p></div></div>
      {store_cards(nearby)}
    </section>
  </main>
  <aside class="side">
    <section class="panel">
      <h2 class="side-title">この店舗を絞り込み</h2>
      <div class="facets">{''.join(area_links)}</div>
    </section>
  </aside>
</div>"""

    crumbs=[("トップ","/")]
    if d["prefecture"]:
        crumbs.append((d["prefecture"],f'/area/{qpath(d["prefecture"])}'))
    if d["prefecture"] and d["city"]:
        crumbs.append((d["city"],f'/area/{qpath(d["prefecture"])}/{qpath(d["city"])}'))
    crumbs.append((d["name"],canonical_path))

    ld={
      "@context":"https://schema.org",
      "@graph":[
        breadcrumb_json(origin,crumbs),
        {
          "@type":"WebPage",
          "@id":site_url(origin,canonical_path),
          "url":site_url(origin,canonical_path),
          "name":title,
          "description":desc,
          "isPartOf":{"@type":"WebSite","name":SITE_NAME,"url":origin.rstrip("/")+"/"},
          "about":{"@type":"Thing","name":d["name"]}
        }
      ]
    }
    return page_shell(origin,title,desc,canonical_path,body,ld,noindex=d.get("quality_pending",False))

def sitemap_xml(database_url, origin, max_urls=45000):
    urls=[origin.rstrip("/")+"/"]
    with _connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT prefecture FROM stores
                WHERE COALESCE(status,'') <> 'excluded'
                  AND prefecture IS NOT NULL AND prefecture<>''
                ORDER BY prefecture
            """)
            for (pref,) in cur.fetchall():
                urls.append(site_url(origin,f"/area/{qpath(pref)}"))

            cur.execute("""
                SELECT DISTINCT prefecture,city FROM stores
                WHERE COALESCE(status,'') <> 'excluded'
                  AND prefecture IS NOT NULL AND prefecture<>''
                  AND city IS NOT NULL AND city<>''
                ORDER BY prefecture,city
            """)
            for pref,city in cur.fetchall():
                urls.append(site_url(origin,f"/area/{qpath(pref)}/{qpath(city)}"))

            cur.execute("""
                SELECT DISTINCT COALESCE(category,'業種未分類') FROM stores
                WHERE COALESCE(status,'') <> 'excluded' ORDER BY 1
            """)
            for (cat,) in cur.fetchall():
                urls.append(site_url(origin,f"/category/{qpath(cat)}"))

            remain=max(0,max_urls-len(urls))
            cur.execute("""
                SELECT id,updated_at FROM stores
                WHERE COALESCE(status,'') <> 'excluded'
                ORDER BY updated_at DESC NULLS LAST,id DESC
                LIMIT %s
            """,(remain,))
            store_rows=cur.fetchall()

    entries=[f"<url><loc>{html.escape(u)}</loc></url>" for u in urls[:max_urls]]
    for sid,updated in store_rows:
        u=site_url(origin,f"/store/{sid}")
        lm=f"<lastmod>{updated.date().isoformat()}</lastmod>" if updated else ""
        entries.append(f"<url><loc>{html.escape(u)}</loc>{lm}</url>")

    return '<?xml version="1.0" encoding="UTF-8"?>' + \
           '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">' + \
           "".join(entries[:max_urls]) + '</urlset>'

def robots_txt(origin):
    return (
        "User-agent: *\n"
        "Allow: /\n"
        "Disallow: /api/\n"
        f"Sitemap: {origin.rstrip('/')}/sitemap.xml\n"
    )


