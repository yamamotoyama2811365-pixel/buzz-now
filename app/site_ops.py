"""Free multi-site traffic/search operations dashboard.

Uses first-party Neon counters immediately. Google Analytics Data API and Search
Console are enabled when GOOGLE_SERVICE_ACCOUNT_JSON is configured. Bing
Webmaster traffic is enabled when BING_WEBMASTER_API_KEY is configured.
"""
from __future__ import annotations

import html
import json
import logging
import os
import re
import threading
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo
from urllib.parse import quote

import httpx
import jwt
import psycopg
from psycopg.rows import dict_row
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import HTMLResponse
from apscheduler.schedulers.background import BackgroundScheduler

log = logging.getLogger("site-ops")
router = APIRouter()

ENABLED = os.getenv("SITE_OPS_ENABLED", "true").lower() == "true"
ACCESS_TOKEN = os.getenv("SITE_OPS_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
CORPORATE_DATABASE_URL = os.getenv("CORPORATE_DATABASE_URL", "").strip()
OPEN_CLOSE_DATABASE_URL = os.getenv("OPEN_CLOSE_DATABASE_URL", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
BING_API_KEY = os.getenv("BING_WEBMASTER_API_KEY", "").strip()
IS_LEGACY = os.getenv("RENDER_SERVICE_ID", "") == "srv-daa321mk1f9s73fbjfcg"

SCOPES = " ".join([
    "https://www.googleapis.com/auth/analytics.readonly",
    "https://www.googleapis.com/auth/webmasters.readonly",
])

SITES = [
    {
        "id": "buzz-now",
        "name": "BUZZ NOW",
        "url": "https://buzz-now-1.onrender.com/",
        "gsc": "https://buzz-now-1.onrender.com/",
        "ga4": "552726997",
        "first_party": "buzz",
    },
    {
        "id": "corporate",
        "name": "企業倒産・新規法人情報",
        "url": "https://buzz-now-1.onrender.com/corporate/",
        "gsc": "https://buzz-now-1.onrender.com/corporate/",
        "ga4": "553524228",
        "first_party": "corporate",
    },
    {
        "id": "tadage",
        "name": "タダゲー手帖",
        "url": "https://tadage-note.pages.dev/",
        "gsc": "https://tadage-note.pages.dev/",
        "ga4": "553712650",
        "first_party": "network",
    },
    {
        "id": "otona-koi",
        "name": "大人の恋のすすめ",
        "url": "https://otona-koi-susume.pages.dev/",
        "gsc": "https://otona-koi-susume.pages.dev/",
        "ga4": "553849114",
        "first_party": "network",
    },
    {
        "id": "biyo-iryo",
        "name": "美容医療コンパス",
        "url": "https://biyo-iryo-compass.pages.dev/",
        "gsc": "https://biyo-iryo-compass.pages.dev/",
        "ga4": "553939743",
        "first_party": "network",
    },
]

_token_cache: dict[str, Any] = {}
_run_lock = threading.Lock()
_scheduler: BackgroundScheduler | None = None
JST = ZoneInfo("Asia/Tokyo")


def _today() -> date:
    return datetime.now(JST).date()


def _connect(url: str = ""):
    dsn = url or DATABASE_URL
    if not dsn:
        raise RuntimeError("DATABASE_URL missing")
    return psycopg.connect(dsn, row_factory=dict_row, connect_timeout=8)


def _init_tables() -> None:
    if not DATABASE_URL:
        return
    statements = [
        """CREATE TABLE IF NOT EXISTS site_ops_daily (
            day DATE NOT NULL,
            site_id TEXT NOT NULL,
            source TEXT NOT NULL,
            metric TEXT NOT NULL,
            value DOUBLE PRECISION NOT NULL DEFAULT 0,
            details JSONB NOT NULL DEFAULT '{}'::jsonb,
            collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY(day, site_id, source, metric)
        )""",
        """CREATE TABLE IF NOT EXISTS site_ops_status (
            source TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'pending',
            message TEXT NOT NULL DEFAULT '',
            last_run_at TIMESTAMPTZ,
            last_success_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS site_ops_indexing (
            site_id TEXT NOT NULL,
            url TEXT NOT NULL,
            verdict TEXT,
            coverage_state TEXT,
            indexing_state TEXT,
            robots_txt_state TEXT,
            page_fetch_state TEXT,
            last_crawl_time TIMESTAMPTZ,
            google_canonical TEXT,
            user_canonical TEXT,
            checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY(site_id,url)
        )""",
    ]
    with _connect() as con:
        with con.cursor() as cur:
            for sql in statements:
                cur.execute(sql)


def _set_status(source: str, status: str, message: str = "", success: bool = False) -> None:
    if not DATABASE_URL:
        return
    now = datetime.now(timezone.utc)
    with _connect() as con:
        with con.cursor() as cur:
            cur.execute(
                """INSERT INTO site_ops_status(source,status,message,last_run_at,last_success_at,updated_at)
                   VALUES(%s,%s,%s,%s,%s,%s)
                   ON CONFLICT(source) DO UPDATE SET
                     status=EXCLUDED.status,
                     message=EXCLUDED.message,
                     last_run_at=EXCLUDED.last_run_at,
                     last_success_at=CASE WHEN %s THEN EXCLUDED.last_run_at ELSE site_ops_status.last_success_at END,
                     updated_at=EXCLUDED.updated_at""",
                (source, status, message[:700], now, now if success else None, now, success),
            )


def _upsert(day: date, site_id: str, source: str, metric: str, value: float, details: Any = None) -> None:
    payload = json.dumps(details or {}, ensure_ascii=False)
    with _connect() as con:
        with con.cursor() as cur:
            cur.execute(
                """INSERT INTO site_ops_daily(day,site_id,source,metric,value,details,collected_at)
                   VALUES(%s,%s,%s,%s,%s,%s::jsonb,NOW())
                   ON CONFLICT(day,site_id,source,metric) DO UPDATE SET
                     value=EXCLUDED.value,
                     details=EXCLUDED.details,
                     collected_at=NOW()""",
                (day, site_id, source, metric, float(value or 0), payload),
            )


def _query_counter(url: str, sql: str, params: tuple) -> tuple[int, dict[str, int], list[dict[str, Any]]]:
    if not url:
        raise RuntimeError("database connection not configured")
    with _connect(url) as con:
        with con.cursor() as cur:
            cur.execute(sql, params)
            rows = list(cur.fetchall())
    total = sum(int(r.get("views") or 0) for r in rows)
    sources: dict[str, int] = {}
    for r in rows:
        src = str(r.get("source") or "unknown")
        sources[src] = sources.get(src, 0) + int(r.get("views") or 0)
    top = sorted(
        [{"path": str(r.get("path") or r.get("page") or "/"), "source": str(r.get("source") or ""), "views": int(r.get("views") or 0)} for r in rows],
        key=lambda x: x["views"],
        reverse=True,
    )[:10]
    return total, sources, top


def collect_first_party(target_day: date | None = None) -> dict[str, Any]:
    day = target_day or _today()
    out: dict[str, Any] = {}
    errors = []
    for site in SITES:
        sid = site["id"]
        try:
            mode = site["first_party"]
            if mode == "buzz":
                total, sources, top = _query_counter(
                    DATABASE_URL,
                    """SELECT path,source,views FROM visitor_pageviews_daily
                       WHERE day=%s AND is_test=0""",
                    (day.isoformat(),),
                )
            elif mode == "network":
                total, sources, top = _query_counter(
                    DATABASE_URL,
                    """SELECT path,source,views FROM network_pageviews_daily
                       WHERE day=%s AND site_id=%s AND is_test=0""",
                    (day.isoformat(), sid),
                )
            elif mode == "corporate":
                total, sources, top = _query_counter(
                    CORPORATE_DATABASE_URL,
                    """SELECT path,source,views FROM corporate_page_views
                       WHERE day=%s""",
                    (day,),
                )
            else:
                continue
            details = {"sources": sources, "top_pages": top}
            _upsert(day, sid, "first_party", "pageviews", total, details)
            out[sid] = {"pageviews": total, **details}
        except Exception as exc:
            errors.append(f"{sid}: {exc}")
            out[sid] = {"error": str(exc)}
    _set_status("first_party", "ok" if not errors else "partial", "; ".join(errors), success=not errors)
    return out


def _service_account() -> dict[str, Any]:
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON missing")
    try:
        return json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    except Exception as exc:
        raise RuntimeError("invalid GOOGLE_SERVICE_ACCOUNT_JSON") from exc


def _google_access_token() -> str:
    cached = _token_cache.get("google")
    now = int(time.time())
    if cached and cached.get("exp", 0) > now + 120:
        return str(cached["token"])
    sa = _service_account()
    claims = {
        "iss": sa["client_email"],
        "scope": SCOPES,
        "aud": "https://oauth2.googleapis.com/token",
        "iat": now,
        "exp": now + 3500,
    }
    assertion = jwt.encode(claims, sa["private_key"], algorithm="RS256")
    with httpx.Client(timeout=20) as client:
        res = client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": assertion,
            },
        )
        res.raise_for_status()
        data = res.json()
    token = data["access_token"]
    _token_cache["google"] = {"token": token, "exp": now + int(data.get("expires_in", 3600))}
    return token


def _ga4_report(property_id: str, start: str, end: str, dimensions: list[str] | None = None, metrics: list[str] | None = None) -> dict[str, Any]:
    token = _google_access_token()
    body: dict[str, Any] = {
        "dateRanges": [{"startDate": start, "endDate": end}],
        "metrics": [{"name": m} for m in (metrics or ["screenPageViews", "sessions", "activeUsers"])],
        "limit": "100",
    }
    if dimensions:
        body["dimensions"] = [{"name": d} for d in dimensions]
    with httpx.Client(timeout=25) as client:
        res = client.post(
            f"https://analyticsdata.googleapis.com/v1beta/properties/{property_id}:runReport",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=body,
        )
        res.raise_for_status()
        return res.json()


def _metric_map(report: dict[str, Any]) -> dict[str, float]:
    headers = [h.get("name", "") for h in report.get("metricHeaders", [])]
    rows = report.get("rows", [])
    if not rows:
        return {h: 0.0 for h in headers}
    values = rows[0].get("metricValues", [])
    out = {}
    for i, h in enumerate(headers):
        raw = values[i].get("value", "0") if i < len(values) else "0"
        try:
            out[h] = float(raw)
        except Exception:
            out[h] = 0.0
    return out


def collect_ga4(target_day: date | None = None) -> dict[str, Any]:
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        _set_status("ga4", "pending", "Google service account not configured")
        return {"configured": False}
    day = target_day or _today()
    ds = day.isoformat()
    out = {}
    errors = []
    for site in SITES:
        sid = site["id"]
        try:
            total_report = _ga4_report(site["ga4"], ds, ds)
            totals = _metric_map(total_report)
            source_report = _ga4_report(site["ga4"], ds, ds, ["sessionSourceMedium"], ["sessions"])
            sources = []
            for row in source_report.get("rows", [])[:12]:
                key = (row.get("dimensionValues") or [{}])[0].get("value", "")
                value = float((row.get("metricValues") or [{}])[0].get("value", "0") or 0)
                sources.append({"source_medium": key, "sessions": value})
            _upsert(day, sid, "ga4", "pageviews", totals.get("screenPageViews", 0), {"sources": sources})
            _upsert(day, sid, "ga4", "sessions", totals.get("sessions", 0))
            _upsert(day, sid, "ga4", "active_users", totals.get("activeUsers", 0))
            out[sid] = {"totals": totals, "sources": sources}
        except Exception as exc:
            errors.append(f"{sid}: {exc}")
            out[sid] = {"error": str(exc)}
    _set_status("ga4", "ok" if not errors else "partial", "; ".join(errors), success=not errors)
    return out


def _gsc_query(site_url: str, start: str, end: str, dimensions: list[str] | None = None, row_limit: int = 100) -> dict[str, Any]:
    token = _google_access_token()
    body: dict[str, Any] = {
        "startDate": start,
        "endDate": end,
        "rowLimit": row_limit,
        "dataState": "all",
    }
    if dimensions:
        body["dimensions"] = dimensions
    encoded = quote(site_url, safe="")
    with httpx.Client(timeout=25) as client:
        res = client.post(
            f"https://www.googleapis.com/webmasters/v3/sites/{encoded}/searchAnalytics/query",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=body,
        )
        res.raise_for_status()
        return res.json()


def collect_gsc(days_back: int = 7) -> dict[str, Any]:
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        _set_status("gsc", "pending", "Google service account not configured")
        return {"configured": False}
    end_day = _today() - timedelta(days=1)
    start_day = end_day - timedelta(days=max(1, days_back) - 1)
    out = {}
    errors = []
    for site in SITES:
        sid = site["id"]
        try:
            report = _gsc_query(site["gsc"], start_day.isoformat(), end_day.isoformat(), ["date"], 100)
            daily = []
            for row in report.get("rows", []):
                keys = row.get("keys") or []
                if not keys:
                    continue
                d = date.fromisoformat(keys[0])
                clicks = float(row.get("clicks") or 0)
                impressions = float(row.get("impressions") or 0)
                ctr = float(row.get("ctr") or 0)
                position = float(row.get("position") or 0)
                _upsert(d, sid, "gsc", "clicks", clicks)
                _upsert(d, sid, "gsc", "impressions", impressions)
                _upsert(d, sid, "gsc", "ctr", ctr)
                _upsert(d, sid, "gsc", "position", position)
                daily.append({"day": d.isoformat(), "clicks": clicks, "impressions": impressions, "ctr": ctr, "position": position})
            q_report = _gsc_query(site["gsc"], start_day.isoformat(), end_day.isoformat(), ["query"], 20)
            p_report = _gsc_query(site["gsc"], start_day.isoformat(), end_day.isoformat(), ["page"], 20)
            top_queries = [
                {"query": (r.get("keys") or [""])[0], "clicks": r.get("clicks", 0), "impressions": r.get("impressions", 0), "position": r.get("position", 0)}
                for r in q_report.get("rows", [])
            ]
            top_pages = [
                {"page": (r.get("keys") or [""])[0], "clicks": r.get("clicks", 0), "impressions": r.get("impressions", 0), "position": r.get("position", 0)}
                for r in p_report.get("rows", [])
            ]
            _upsert(end_day, sid, "gsc", "search_details", 0, {"window_start": start_day.isoformat(), "window_end": end_day.isoformat(), "top_queries": top_queries, "top_pages": top_pages})
            out[sid] = {"daily": daily, "top_queries": top_queries, "top_pages": top_pages}
        except Exception as exc:
            errors.append(f"{sid}: {exc}")
            out[sid] = {"error": str(exc)}
    _set_status("gsc", "ok" if not errors else "partial", "; ".join(errors), success=not errors)
    return out


def collect_indexing() -> dict[str, Any]:
    if not GOOGLE_SERVICE_ACCOUNT_JSON:
        _set_status("indexing", "pending", "Google service account not configured")
        return {"configured": False}
    token = _google_access_token()
    out = {}
    errors = []
    for site in SITES:
        sid = site["id"]
        try:
            with httpx.Client(timeout=25) as client:
                res = client.post(
                    "https://searchconsole.googleapis.com/v1/urlInspection/index:inspect",
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    json={"inspectionUrl": site["url"], "siteUrl": site["gsc"]},
                )
                res.raise_for_status()
                result = res.json().get("inspectionResult", {}).get("indexStatusResult", {})
            def _dt(v: str | None):
                if not v:
                    return None
                try:
                    return datetime.fromisoformat(v.replace("Z", "+00:00"))
                except Exception:
                    return None
            with _connect() as con:
                with con.cursor() as cur:
                    cur.execute(
                        """INSERT INTO site_ops_indexing(
                             site_id,url,verdict,coverage_state,indexing_state,robots_txt_state,
                             page_fetch_state,last_crawl_time,google_canonical,user_canonical,checked_at
                           ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                           ON CONFLICT(site_id,url) DO UPDATE SET
                             verdict=EXCLUDED.verdict,coverage_state=EXCLUDED.coverage_state,
                             indexing_state=EXCLUDED.indexing_state,robots_txt_state=EXCLUDED.robots_txt_state,
                             page_fetch_state=EXCLUDED.page_fetch_state,last_crawl_time=EXCLUDED.last_crawl_time,
                             google_canonical=EXCLUDED.google_canonical,user_canonical=EXCLUDED.user_canonical,
                             checked_at=NOW()""",
                        (
                            sid, site["url"], result.get("verdict"), result.get("coverageState"),
                            result.get("indexingState"), result.get("robotsTxtState"),
                            result.get("pageFetchState"), _dt(result.get("lastCrawlTime")),
                            result.get("googleCanonical"), result.get("userCanonical"),
                        ),
                    )
            out[sid] = result
        except Exception as exc:
            errors.append(f"{sid}: {exc}")
            out[sid] = {"error": str(exc)}
    _set_status("indexing", "ok" if not errors else "partial", "; ".join(errors), success=not errors)
    return out


_BING_DATE_RE = re.compile(r"/Date\((\d+)(?:[+-]\d+)?\)/")


def _bing_day(raw: str) -> date | None:
    m = _BING_DATE_RE.search(str(raw or ""))
    if not m:
        return None
    return datetime.fromtimestamp(int(m.group(1)) / 1000, tz=timezone.utc).date()


def collect_bing() -> dict[str, Any]:
    if not BING_API_KEY:
        _set_status("bing", "pending", "Bing Webmaster API key not configured")
        return {"configured": False}
    out = {}
    errors = []
    for site in SITES:
        try:
            with httpx.Client(timeout=25) as client:
                res = client.get(
                    "https://ssl.bing.com/webmaster/api.svc/json/GetRankAndTrafficStats",
                    params={"siteUrl": site["url"], "apikey": BING_API_KEY},
                )
                res.raise_for_status()
                rows = res.json().get("d", [])
            items = []
            for row in rows:
                d = _bing_day(row.get("Date"))
                if not d:
                    continue
                clicks = float(row.get("Clicks") or 0)
                impressions = float(row.get("Impressions") or 0)
                _upsert(d, site["id"], "bing", "clicks", clicks)
                _upsert(d, site["id"], "bing", "impressions", impressions)
                items.append({"day": d.isoformat(), "clicks": clicks, "impressions": impressions})
            out[site["id"]] = items[-14:]
        except Exception as exc:
            errors.append(f"{site['id']}: {exc}")
            out[site["id"]] = {"error": str(exc)}
    _set_status("bing", "ok" if not errors else "partial", "; ".join(errors), success=not errors)
    return out


def collect_hourly() -> dict[str, Any]:
    if not ENABLED or IS_LEGACY or not DATABASE_URL:
        return {"ok": False, "reason": "disabled"}
    if not _run_lock.acquire(blocking=False):
        return {"ok": False, "reason": "already_running"}
    try:
        result = {"first_party": collect_first_party(_today())}
        if GOOGLE_SERVICE_ACCOUNT_JSON:
            result["ga4"] = collect_ga4(_today())
        return {"ok": True, "result": result}
    finally:
        _run_lock.release()


def collect_daily() -> dict[str, Any]:
    if not ENABLED or IS_LEGACY or not DATABASE_URL:
        return {"ok": False, "reason": "disabled"}
    if not _run_lock.acquire(blocking=False):
        return {"ok": False, "reason": "already_running"}
    try:
        yesterday = _today() - timedelta(days=1)
        result = {
            "first_party": collect_first_party(yesterday),
            "google_configured": bool(GOOGLE_SERVICE_ACCOUNT_JSON),
            "bing_configured": bool(BING_API_KEY),
        }
        if GOOGLE_SERVICE_ACCOUNT_JSON:
            result["ga4_yesterday"] = collect_ga4(yesterday)
            result["gsc"] = collect_gsc(7)
            result["indexing"] = collect_indexing()
        if BING_API_KEY:
            result["bing"] = collect_bing()
        return {"ok": True, "result": result}
    finally:
        _run_lock.release()


def _authorized(key: str | None) -> bool:
    return bool(ACCESS_TOKEN and key and key == ACCESS_TOKEN)


def _dashboard_data(days: int = 8) -> dict[str, Any]:
    start = _today() - timedelta(days=max(2, min(days, 31)) - 1)
    with _connect() as con:
        with con.cursor() as cur:
            cur.execute(
                """SELECT day,site_id,source,metric,value,details,collected_at
                   FROM site_ops_daily WHERE day >= %s
                   ORDER BY day DESC, site_id, source, metric""",
                (start,),
            )
            rows = list(cur.fetchall())
            cur.execute("SELECT * FROM site_ops_status ORDER BY source")
            statuses = list(cur.fetchall())
            cur.execute("SELECT * FROM site_ops_indexing ORDER BY site_id")
            indexing = list(cur.fetchall())
    return {"rows": rows, "statuses": statuses, "indexing": indexing}


def _latest_metric(rows: list[dict[str, Any]], site_id: str, source: str, metric: str) -> dict[str, Any] | None:
    candidates = [r for r in rows if r["site_id"] == site_id and r["source"] == source and r["metric"] == metric]
    return max(candidates, key=lambda r: r["day"]) if candidates else None


@router.get("/api/site-ops/status")
def site_ops_status():
    statuses = []
    if DATABASE_URL:
        try:
            with _connect() as con:
                with con.cursor() as cur:
                    cur.execute("SELECT source,status,message,last_run_at,last_success_at FROM site_ops_status ORDER BY source")
                    statuses = list(cur.fetchall())
        except Exception as exc:
            statuses = [{"source": "database", "status": "error", "message": str(exc)}]
    return {
        "ok": True,
        "enabled": ENABLED and not IS_LEGACY,
        "google_configured": bool(GOOGLE_SERVICE_ACCOUNT_JSON),
        "bing_configured": bool(BING_API_KEY),
        "first_party_configured": bool(DATABASE_URL),
        "sites": [{"id": s["id"], "name": s["name"]} for s in SITES],
        "statuses": statuses,
    }


@router.get("/api/site-ops/data")
def site_ops_data(key: str = Query(default=""), days: int = Query(default=8, ge=2, le=31)):
    if not _authorized(key):
        raise HTTPException(403, "invalid site-ops key")
    return _dashboard_data(days)


@router.post("/api/site-ops/run")
def site_ops_run(key: str = Query(default=""), full: bool = False):
    if not _authorized(key):
        raise HTTPException(403, "invalid site-ops key")
    return collect_daily() if full else collect_hourly()


@router.get("/site-ops", response_class=HTMLResponse)
def site_ops_dashboard(key: str = Query(default="")):
    if not _authorized(key):
        return HTMLResponse(
            """<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
            <title>Site Ops</title><style>body{font-family:system-ui;max-width:520px;margin:80px auto;padding:24px}
            input,button{font-size:16px;padding:12px;width:100%;box-sizing:border-box;margin:8px 0}</style>
            <h1>Site Ops</h1><p>アクセスキーを入力してください。</p>
            <form method="get"><input name="key" type="password" autocomplete="current-password" required>
            <button>ダッシュボードを開く</button></form>""",
            status_code=200,
        )
    data = _dashboard_data(8)
    rows = data["rows"]
    idx = {r["site_id"]: r for r in data["indexing"]}
    cards = []
    for site in SITES:
        fp = _latest_metric(rows, site["id"], "first_party", "pageviews")
        ga = _latest_metric(rows, site["id"], "ga4", "pageviews")
        gc = _latest_metric(rows, site["id"], "gsc", "clicks")
        gi = _latest_metric(rows, site["id"], "gsc", "impressions")
        bp = _latest_metric(rows, site["id"], "bing", "clicks")
        ix = idx.get(site["id"], {})
        def val(r):
            return "—" if not r else f"{int(r['value']) if float(r['value']).is_integer() else round(float(r['value']),2)}"
        cards.append(f"""
        <article class="card">
          <h2>{html.escape(site['name'])}</h2>
          <div class="url">{html.escape(site['url'])}</div>
          <div class="grid">
            <div><b>{val(fp)}</b><span>独自PV</span></div>
            <div><b>{val(ga)}</b><span>GA4 PV</span></div>
            <div><b>{val(gc)}</b><span>Google click</span></div>
            <div><b>{val(gi)}</b><span>Google imp.</span></div>
            <div><b>{val(bp)}</b><span>Bing click</span></div>
            <div><b>{html.escape(str(ix.get('coverage_state') or '—'))}</b><span>Index</span></div>
          </div>
        </article>""")
    status_html = "".join(
        f"<li><b>{html.escape(str(s['source']))}</b> {html.escape(str(s['status']))} "
        f"<small>{html.escape(str(s.get('message') or ''))}</small></li>"
        for s in data["statuses"]
    )
    return HTMLResponse(f"""<!doctype html><html lang="ja"><head><meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1"><title>5サイト運用ダッシュボード</title>
    <style>
    body{{font-family:system-ui,-apple-system,sans-serif;background:#f5f7fb;color:#172033;margin:0}}
    main{{max-width:1100px;margin:auto;padding:28px 18px 60px}}h1{{margin-bottom:4px}}
    .sub{{color:#667085;margin-bottom:24px}}.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(310px,1fr));gap:14px}}
    .card{{background:white;border:1px solid #e7eaf0;border-radius:16px;padding:18px;box-shadow:0 2px 10px #1720330a}}
    .card h2{{font-size:18px;margin:0 0 4px}}.url{{font-size:12px;color:#667085;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
    .grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:16px}}
    .grid div{{background:#f8fafc;border-radius:10px;padding:10px;min-height:56px}}.grid b{{display:block;font-size:19px;overflow:hidden;text-overflow:ellipsis}}
    .grid span{{font-size:11px;color:#667085}}section{{background:white;margin-top:16px;border-radius:16px;padding:18px;border:1px solid #e7eaf0}}
    li{{margin:8px 0}}small{{color:#667085}}a{{color:#2563eb}}@media(max-width:520px){{.grid{{grid-template-columns:repeat(2,1fr)}}}}
    </style></head><body><main>
    <h1>5サイト運用ダッシュボード</h1>
    <div class="sub">独自PV + GA4 + Google Search Console + Bing / 無料API中心</div>
    <div class="cards">{''.join(cards)}</div>
    <section><h2>接続状態</h2><ul>{status_html or '<li>まだ収集履歴がありません</li>'}</ul>
    <p><a href="/api/site-ops/data?key={html.escape(key)}">JSONデータ</a></p></section>
    </main></body></html>""")


def _start_scheduler() -> None:
    global _scheduler
    if not ENABLED or IS_LEGACY or not DATABASE_URL or _scheduler is not None:
        return
    _init_tables()
    scheduler = BackgroundScheduler(timezone="Asia/Tokyo")
    scheduler.add_job(collect_hourly, "cron", minute=12, id="site_ops_hourly", replace_existing=True, max_instances=1, coalesce=True)
    scheduler.add_job(collect_daily, "cron", hour=8, minute=45, id="site_ops_daily", replace_existing=True, max_instances=1, coalesce=True)
    scheduler.start()
    _scheduler = scheduler
    threading.Thread(target=collect_hourly, name="site-ops-seed", daemon=True).start()


try:
    _start_scheduler()
except Exception:
    log.exception("site-ops scheduler failed to start")
