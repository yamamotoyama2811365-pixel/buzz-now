from services.open_close.collectors.public_quality import public_store
import os
import hmac
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Query, HTTPException, Header, Depends, Request
from fastapi.responses import HTMLResponse, Response, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
import psycopg

from services.open_close.collectors.runner import run_collectors
from services.open_close.collectors.source_health import (
    record_cycle_source_runs,get_source_health
)
from services.open_close.collectors.tenant_watcher import (
    promote_existing_tenant_candidates,scan_closed_store_tenant_news,
    verify_tenant_listings,tenant_watch_status
)
from services.open_close.collectors.processor import promote_candidates, enrich_candidates
from services.open_close.collectors.article_enricher import fetch_article_facts
from services.open_close.collectors.address_audit import audit_addresses
from services.open_close.collectors.news_resolver import resolve_google_news_url
from services.open_close.collectors.rescue_processor import rescue_sources
from services.open_close.collectors.address_quality import audit_and_clean
from services.open_close.collectors.non_store_quality import audit_non_store_events
from services.open_close.collectors.text_rules import extract_best_store_name_from_title
from services.open_close.collectors.openclose_hub import (
    collect_openclose_hub,enrich_hub_candidates,
    promote_hub_candidates,reset_and_hide_hub_promotions
)
from services.open_close.collectors.backfill import collect_backfill_step,get_backfill_status
from services.open_close.collectors.seo_pages import (
    render_area,render_category,render_store,sitemap_xml,robots_txt
)
from services.open_close.collectors.category_manager import (
    backfill_store_categories,category_stats
)
from services.open_close.collectors.activity import compute_activity_score
from services.open_close.collectors.prefecture_closing import (
    collect_prefecture_closings,get_prefecture_coverage
)

app=FastAPI(title="Open Close Map API",version="1.14.0")

FRONTEND_ORIGIN=os.getenv(
    "OPEN_CLOSE_FRONTEND_ORIGIN",
    "https://open-close-map.onrender.com"
).strip()

ALLOWED_ORIGINS=[
    x.strip()
    for x in os.getenv("OPEN_CLOSE_ALLOWED_ORIGINS",FRONTEND_ORIGIN).split(",")
    if x.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET","POST","OPTIONS"],
    allow_headers=["Content-Type","X-Admin-Key"]
)

DATABASE_URL=os.getenv("OPEN_CLOSE_DATABASE_URL","").strip()
ADMIN_KEY=os.getenv("OPEN_CLOSE_ADMIN_KEY","").strip()
GA_MEASUREMENT_ID=os.getenv("OPEN_CLOSE_GA_MEASUREMENT_ID","G-ZB4T13GM5P").strip()

PUBLIC_SITE_ORIGIN=os.getenv(
    "OPEN_CLOSE_PUBLIC_SITE_ORIGIN",
    "https://open-close-map.onrender.com"
).strip().rstrip("/")

def require_admin(request: Request, x_admin_key: Optional[str]=Header(default=None,alias="X-Admin-Key")):
    from services.open_close.auth import require_operator
    return require_operator(request, ADMIN_KEY, x_admin_key)


def db_conn():
    return psycopg.connect(DATABASE_URL) if DATABASE_URL else None

def init_db():
    conn=db_conn()
    if conn is None:
        return

    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stores(
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    category TEXT,
                    prefecture TEXT,
                    city TEXT,
                    address TEXT,
                    facility_name TEXT,
                    floor TEXT,
                    postal_code TEXT,
                    open_date DATE,
                    close_date DATE,
                    source_url TEXT,
                    source_name TEXT,
                    official_url TEXT,
                    confidence INTEGER DEFAULT 0,
                    last_verified_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            for q in [
                "ALTER TABLE stores ADD COLUMN IF NOT EXISTS facility_name TEXT",
                "ALTER TABLE stores ADD COLUMN IF NOT EXISTS floor TEXT",
                "ALTER TABLE stores ADD COLUMN IF NOT EXISTS postal_code TEXT",
                "ALTER TABLE stores ADD COLUMN IF NOT EXISTS source_name TEXT"
            ]:
                cur.execute(q)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS tenant_listings(
                    id BIGSERIAL PRIMARY KEY,
                    store_id BIGINT REFERENCES stores(id) ON DELETE SET NULL,
                    source_name TEXT,
                    source_url TEXT,
                    status TEXT DEFAULT 'detected',
                    last_verified_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            for q in [
                "ALTER TABLE tenant_listings ADD COLUMN IF NOT EXISTS confidence INTEGER DEFAULT 0",
                "ALTER TABLE tenant_listings ADD COLUMN IF NOT EXISTS match_method TEXT",
                "ALTER TABLE tenant_listings ADD COLUMN IF NOT EXISTS query_text TEXT"
            ]:
                cur.execute(q)

            cur.execute("""
                DELETE FROM tenant_listings a
                USING tenant_listings b
                WHERE a.id>b.id
                  AND a.store_id IS NOT DISTINCT FROM b.store_id
                  AND a.source_url IS NOT DISTINCT FROM b.source_url
            """)

            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_tenant_listing_store_url
                ON tenant_listings(store_id,source_url)
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS tenant_watch_state(
                    store_id BIGINT PRIMARY KEY REFERENCES stores(id) ON DELETE CASCADE,
                    last_checked_at TIMESTAMPTZ,
                    query_count BIGINT NOT NULL DEFAULT 0,
                    last_error TEXT,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS collector_source_runs(
                    id BIGSERIAL PRIMARY KEY,
                    run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    family TEXT NOT NULL,
                    source_key TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    fetched INTEGER NOT NULL DEFAULT 0,
                    inserted INTEGER NOT NULL DEFAULT 0,
                    duplicates INTEGER NOT NULL DEFAULT 0,
                    rejected INTEGER NOT NULL DEFAULT 0,
                    error_count INTEGER NOT NULL DEFAULT 0,
                    error_text TEXT,
                    ok BOOLEAN NOT NULL DEFAULT TRUE
                )
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_collector_source_runs_key_time
                ON collector_source_runs(source_key,run_at DESC)
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_collector_source_runs_time
                ON collector_source_runs(run_at DESC)
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS source_backfill_state(
                    source_key TEXT PRIMARY KEY,
                    next_page INTEGER NOT NULL DEFAULT 4,
                    last_page INTEGER,
                    completed BOOLEAN NOT NULL DEFAULT FALSE,
                    last_run_at TIMESTAMPTZ,
                    pages_fetched BIGINT NOT NULL DEFAULT 0,
                    items_inserted BIGINT NOT NULL DEFAULT 0,
                    last_error TEXT,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS discovery_items(
                    id BIGSERIAL PRIMARY KEY,
                    fingerprint TEXT UNIQUE NOT NULL,
                    title TEXT NOT NULL,
                    source_name TEXT,
                    source_url TEXT NOT NULL,
                    resolved_source_url TEXT,
                    published_at TIMESTAMPTZ,
                    detected_status TEXT,
                    prefecture TEXT,
                    city TEXT,
                    confidence INTEGER DEFAULT 0,
                    raw_summary TEXT,
                    processed BOOLEAN DEFAULT FALSE,
                    store_name_candidate TEXT,
                    event_date_candidate DATE,
                    category_candidate TEXT,
                    address_candidate TEXT,
                    facility_name_candidate TEXT,
                    floor_candidate TEXT,
                    postal_code_candidate TEXT,
                    rescue_attempted_at TIMESTAMPTZ,
                    rescue_status TEXT,
                    rescue_error TEXT,
                    discovery_channel TEXT,
                    official_url_candidate TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            for q in [
                "ALTER TABLE discovery_items ADD COLUMN IF NOT EXISTS resolved_source_url TEXT",
                "ALTER TABLE discovery_items ADD COLUMN IF NOT EXISTS address_candidate TEXT",
                "ALTER TABLE discovery_items ADD COLUMN IF NOT EXISTS facility_name_candidate TEXT",
                "ALTER TABLE discovery_items ADD COLUMN IF NOT EXISTS floor_candidate TEXT",
                "ALTER TABLE discovery_items ADD COLUMN IF NOT EXISTS postal_code_candidate TEXT",
                "ALTER TABLE discovery_items ADD COLUMN IF NOT EXISTS rescue_attempted_at TIMESTAMPTZ",
                "ALTER TABLE discovery_items ADD COLUMN IF NOT EXISTS rescue_status TEXT",
                "ALTER TABLE discovery_items ADD COLUMN IF NOT EXISTS rescue_error TEXT",
                "ALTER TABLE discovery_items ADD COLUMN IF NOT EXISTS publisher_name TEXT",
                "ALTER TABLE discovery_items ADD COLUMN IF NOT EXISTS publisher_home_url TEXT",
                "ALTER TABLE discovery_items ADD COLUMN IF NOT EXISTS discovery_channel TEXT",
                "ALTER TABLE discovery_items ADD COLUMN IF NOT EXISTS official_url_candidate TEXT",
                "ALTER TABLE stores ADD COLUMN IF NOT EXISTS official_url TEXT"
            ]:
                cur.execute(q)

@app.get("/ready")
def ready():
    conn=db_conn()
    if conn is None:
        raise HTTPException(503,"Database is not configured")
    with conn:
        conn.execute("SELECT 1 FROM stores LIMIT 1")
    return {"ready":True,"service":"open-close-map-api","hosting":"shared"}

@app.get("/")
def root():
    return {
        "service":"open-close-map-api",
        "status":"ok",
        "version":"1.14.0",
        "time":datetime.now(timezone.utc).isoformat()
    }

@app.get("/health")
def health():
    return {
        "ok":True,
        "database_configured":bool(DATABASE_URL),
        "admin_key_configured":bool(ADMIN_KEY),
        "automation_auth":"github_oidc",
        "hosting":"shared",
        "time":datetime.now(timezone.utc).isoformat()
    }

@app.get("/api/public-config")
def public_config():
    """
    Public, non-secret frontend configuration.
    GA Measurement IDs are designed to be public in page source.
    """
    return {
        "ga_measurement_id":GA_MEASUREMENT_ID
    }

@app.get("/api/stats")
def stats():
    conn=db_conn()
    if conn is None:
        return {
            "today_open":0,"week_open":0,"week_close":0,
            "tenant_detected":0,"discovery_unprocessed":0,
            "stores_total":0,"stores_with_address":0,"mode":"no-database"
        }

    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) FILTER(
                        WHERE status IN('open','opening') AND open_date=CURRENT_DATE
                    ),
                    COUNT(*) FILTER(
                        WHERE status IN('open','opening')
                          AND open_date BETWEEN CURRENT_DATE AND CURRENT_DATE+INTERVAL '7 days'
                    ),
                    COUNT(*) FILTER(
                        WHERE status IN('closed','closing')
                          AND close_date BETWEEN CURRENT_DATE AND CURRENT_DATE+INTERVAL '7 days'
                    ),
                    COUNT(*) FILTER(WHERE COALESCE(status,'') <> 'excluded'),
                    COUNT(*) FILTER(
                        WHERE COALESCE(status,'') <> 'excluded'
                          AND address IS NOT NULL AND address<>''
                    )
                FROM stores
            """)
            today,weeko,weekc,total,withaddr=cur.fetchone()

            cur.execute("""
                SELECT COUNT(*)
                FROM tenant_listings
                WHERE status IN('detected','active')
            """)
            tenant=cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*)
                FROM discovery_items
                WHERE processed=FALSE
            """)
            unprocessed=cur.fetchone()[0]

    return {
        "today_open":today,
        "week_open":weeko,
        "week_close":weekc,
        "tenant_detected":tenant,
        "discovery_unprocessed":unprocessed,
        "stores_total":total,
        "stores_with_address":withaddr,
        "mode":"database"
    }

@app.get("/api/stores")
def stores(
    status:Optional[str]=None,
    prefecture:Optional[str]=None,
    city:Optional[str]=None,
    limit:int=Query(default=50,ge=1,le=200)
):
    conn=db_conn()
    if conn is None:
        return {"items":[],"count":0}

    clauses=[]
    params=[]

    if not status:
        clauses.append("COALESCE(status,'') <> 'excluded'")

    if status:
        clauses.append("status=%s")
        params.append(status)
    if prefecture:
        clauses.append("prefecture=%s")
        params.append(prefecture)
    if city:
        clauses.append("city=%s")
        params.append(city)

    where=("WHERE "+" AND ".join(clauses)) if clauses else ""
    params.append(limit)

    with conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                    id,name,status,category,prefecture,city,address,
                    facility_name,floor,postal_code,
                    open_date,close_date,source_url,source_name,official_url,
                    confidence,last_verified_at
                FROM stores
                {where}
                ORDER BY COALESCE(open_date,close_date,created_at::date) DESC NULLS LAST,id DESC
                LIMIT %s
            """,params)
            rows=cur.fetchall()

    return {
        "items":[public_store({
            "id":r[0],"name":r[1],"status":r[2],"category":r[3],
            "prefecture":r[4],"city":r[5],"address":r[6],
            "facility_name":r[7],"floor":r[8],"postal_code":r[9],
            "open_date":r[10].isoformat() if r[10] else None,
            "close_date":r[11].isoformat() if r[11] else None,
            "source_url":r[12],"source_name":r[13],"official_url":r[14],
            "confidence":r[15],
            "last_verified_at":r[16].isoformat() if r[16] else None
        }) for r in rows],
        "count":len(rows)
    }

@app.get("/api/stores/{store_id}")
def store(store_id:int):
    conn=db_conn()
    if conn is None:
        raise HTTPException(503,"Database unavailable")

    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,name,status,category,prefecture,city,address,
                    facility_name,floor,postal_code,
                    open_date,close_date,source_url,source_name,official_url,
                    confidence,last_verified_at
                FROM stores
                WHERE id=%s
            """,(store_id,))
            r=cur.fetchone()

    if not r:
        raise HTTPException(404,"Store not found")

    return public_store({
        "id":r[0],"name":r[1],"status":r[2],"category":r[3],
        "prefecture":r[4],"city":r[5],"address":r[6],
        "facility_name":r[7],"floor":r[8],"postal_code":r[9],
        "open_date":r[10].isoformat() if r[10] else None,
        "close_date":r[11].isoformat() if r[11] else None,
        "source_url":r[12],"source_name":r[13],"official_url":r[14],
        "confidence":r[15],
        "last_verified_at":r[16].isoformat() if r[16] else None
    })

# ---- Restored detail-page APIs ----

@app.get("/api/stores/{store_id}/nearby")
def nearby(store_id:int,limit:int=Query(default=6,ge=1,le=20)):
    conn=db_conn()
    if conn is None:
        raise HTTPException(503,"Database unavailable")

    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT prefecture,city
                FROM stores
                WHERE id=%s
            """,(store_id,))
            base=cur.fetchone()

            if not base:
                raise HTTPException(404,"Store not found")

            pref,city=base

            cur.execute("""
                SELECT id,name,status,category,facility_name
                FROM stores
                WHERE id<>%s
                  AND COALESCE(status,'') <> 'excluded'
                  AND COALESCE(prefecture,'')=COALESCE(%s,'')
                  AND COALESCE(city,'')=COALESCE(%s,'')
                ORDER BY id DESC
                LIMIT %s
            """,(store_id,pref,city,limit))
            rows=cur.fetchall()

    return {
        "items":[public_store({
            "id":r[0],"name":r[1],"status":r[2],
            "category":r[3],"facility_name":r[4]
        }) for r in rows]
    }

@app.get("/api/stores/{store_id}/tenant")
def tenant_info(store_id:int):
    conn=db_conn()
    if conn is None:
        raise HTTPException(503,"Database unavailable")

    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id,source_name,source_url,status,last_verified_at
                FROM tenant_listings
                WHERE store_id=%s
                ORDER BY updated_at DESC
            """,(store_id,))
            rows=cur.fetchall()

    return {
        "items":[{
            "id":r[0],
            "source_name":r[1],
            "source_url":r[2],
            "status":r[3],
            "last_verified_at":r[4].isoformat() if r[4] else None
        } for r in rows]
    }


@app.get("/api/stores/{store_id}/history")
def store_history(store_id:int):
    """
    Exact-address history only. Conservative by design to avoid joining unrelated nearby stores.
    """
    if not DATABASE_URL:
        return {"items":[]}

    conn=db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT address FROM stores
                WHERE id=%s AND COALESCE(status,'') <> 'excluded'
            """,(store_id,))
            row=cur.fetchone()
            if not row or not row[0]:
                return {"items":[]}

            address=row[0]
            cur.execute("""
                SELECT
                    id,name,status,category,prefecture,city,address,
                    open_date,close_date,source_url,source_name,official_url,
                    last_verified_at
                FROM stores
                WHERE COALESCE(status,'') <> 'excluded'
                  AND address=%s
                ORDER BY
                    COALESCE(open_date,close_date,created_at::date) ASC NULLS LAST,
                    id ASC
            """,(address,))
            rows=cur.fetchall()

    return {
        "address":address,
        "items":[
            {
                "id":r[0],"name":r[1],"status":r[2],"category":r[3],
                "prefecture":r[4],"city":r[5],"address":r[6],
                "open_date":r[7].isoformat() if r[7] else None,
                "close_date":r[8].isoformat() if r[8] else None,
                "source_url":r[9],"source_name":r[10],"official_url":r[11],
                "last_verified_at":r[12].isoformat() if r[12] else None
            }
            for r in rows
        ]
    }

@app.get("/api/stores/{store_id}/area-summary")
def area_summary(store_id:int):
    conn=db_conn()
    if conn is None:
        raise HTTPException(503,"Database unavailable")

    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT prefecture,city
                FROM stores
                WHERE id=%s
            """,(store_id,))
            base=cur.fetchone()

            if not base:
                raise HTTPException(404,"Store not found")

            pref,city=base

            cur.execute("""
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER(WHERE status IN('open','opening')),
                    COUNT(*) FILTER(WHERE status IN('closed','closing'))
                FROM stores
                WHERE COALESCE(status,'') <> 'excluded'
                  AND COALESCE(prefecture,'')=COALESCE(%s,'')
                  AND COALESCE(city,'')=COALESCE(%s,'')
            """,(pref,city))
            total,opening,closing=cur.fetchone()

    return {
        "total":total,
        "opening":opening,
        "closing":closing
    }

@app.get("/api/stores/{store_id}/activity-score")
def activity_score(store_id:int):
    conn=db_conn()
    if conn is None:
        raise HTTPException(503,"Database unavailable")

    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT prefecture,city
                FROM stores
                WHERE id=%s
                  AND COALESCE(status,'') <> 'excluded'
            """,(store_id,))
            base=cur.fetchone()

            if not base:
                raise HTTPException(404,"Store not found")

            pref,city=base

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
            """,(pref,city))
            total,recent_open,recent_close,planned_open,planned_close=cur.fetchone()

    result=compute_activity_score(
        total,recent_open,recent_close,planned_open,planned_close
    )
    result["prefecture"]=pref
    result["city"]=city
    return result

# ---- Collection / enrichment ----

@app.get("/api/categories")
def public_categories():
    if not DATABASE_URL:
        return {"items":[]}
    return category_stats(DATABASE_URL)


@app.get("/api/national-insights")
def national_insights():
    conn=db_conn()
    if conn is None:
        raise HTTPException(503,"Database unavailable")

    where="COALESCE(status,'') <> 'excluded'"

    with conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                WITH months AS (
                    SELECT generate_series(
                        date_trunc('month',CURRENT_DATE)-INTERVAL '11 months',
                        date_trunc('month',CURRENT_DATE),
                        INTERVAL '1 month'
                    )::date month_start
                ),
                oe AS (
                    SELECT date_trunc('month',open_date)::date month_start,COUNT(*) cnt
                    FROM stores
                    WHERE {where}
                      AND open_date IS NOT NULL
                      AND open_date >= date_trunc('month',CURRENT_DATE)-INTERVAL '11 months'
                      AND open_date < date_trunc('month',CURRENT_DATE)+INTERVAL '1 month'
                    GROUP BY 1
                ),
                ce AS (
                    SELECT date_trunc('month',close_date)::date month_start,COUNT(*) cnt
                    FROM stores
                    WHERE {where}
                      AND close_date IS NOT NULL
                      AND close_date >= date_trunc('month',CURRENT_DATE)-INTERVAL '11 months'
                      AND close_date < date_trunc('month',CURRENT_DATE)+INTERVAL '1 month'
                    GROUP BY 1
                )
                SELECT m.month_start,COALESCE(o.cnt,0),COALESCE(c.cnt,0)
                FROM months m
                LEFT JOIN oe o USING(month_start)
                LEFT JOIN ce c USING(month_start)
                ORDER BY m.month_start
            """)
            monthly=cur.fetchall()

            cur.execute(f"""
                SELECT
                    COALESCE(NULLIF(category,''),'業種未分類'),
                    COUNT(*) FILTER(
                        WHERE open_date IS NOT NULL
                          AND open_date>=CURRENT_DATE-INTERVAL '365 days'
                          AND open_date<=CURRENT_DATE
                    ),
                    COUNT(*) FILTER(
                        WHERE close_date IS NOT NULL
                          AND close_date>=CURRENT_DATE-INTERVAL '365 days'
                          AND close_date<=CURRENT_DATE
                    )
                FROM stores
                WHERE {where}
                GROUP BY 1
                HAVING
                    COUNT(*) FILTER(
                        WHERE open_date IS NOT NULL
                          AND open_date>=CURRENT_DATE-INTERVAL '365 days'
                          AND open_date<=CURRENT_DATE
                    )
                    +
                    COUNT(*) FILTER(
                        WHERE close_date IS NOT NULL
                          AND close_date>=CURRENT_DATE-INTERVAL '365 days'
                          AND close_date<=CURRENT_DATE
                    ) > 0
                ORDER BY 2+3 DESC,1
                LIMIT 20
            """)
            cats=cur.fetchall()

            cur.execute("""
                SELECT COUNT(*)
                FROM stores
                WHERE COALESCE(status,'') <> 'excluded'
            """)
            total=cur.fetchone()[0]

    monthly_items=[
        {
            "month":m.isoformat(),
            "open":int(o or 0),
            "close":int(c or 0),
            "net":int(o or 0)-int(c or 0)
        }
        for m,o,c in monthly
    ]

    excluded={"業種未分類","未分類","小売","飲食店"}
    category_items=[]
    for cat,o,c in cats:
        if not cat or cat in excluded:
            continue
        category_items.append({
            "category":cat,
            "open":int(o or 0),
            "close":int(c or 0),
            "net":int(o or 0)-int(c or 0)
        })
        if len(category_items)>=6:
            break

    open_total=sum(x["open"] for x in monthly_items)
    close_total=sum(x["close"] for x in monthly_items)
    observed=open_total+close_total
    net=open_total-close_total

    if observed < 10:
        trend="データ蓄積中"
    elif net >= max(3,round(observed*0.08)):
        trend="開店優勢"
    elif net <= -max(3,round(observed*0.08)):
        trend="閉店優勢"
    else:
        trend="ほぼ均衡"

    return {
        "scope":"全国",
        "period":"last_12_months",
        "listed_stores":int(total or 0),
        "open":open_total,
        "close":close_total,
        "net":net,
        "trend":trend,
        "monthly":monthly_items,
        "categories":category_items
    }


@app.get("/api/area-insights")
def area_insights(
    prefecture:str=Query(...,min_length=1),
    city:Optional[str]=Query(default=None)
):
    conn=db_conn()
    if conn is None:
        raise HTTPException(503,"Database unavailable")

    clauses=["COALESCE(status,'') <> 'excluded'","prefecture=%s"]
    params=[prefecture]
    if city:
        clauses.append("city=%s")
        params.append(city)
    where=" AND ".join(clauses)

    with conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                WITH months AS (
                    SELECT generate_series(
                        date_trunc('month',CURRENT_DATE)-INTERVAL '11 months',
                        date_trunc('month',CURRENT_DATE),
                        INTERVAL '1 month'
                    )::date month_start
                ),
                oe AS (
                    SELECT date_trunc('month',open_date)::date month_start,COUNT(*) cnt
                    FROM stores
                    WHERE {where}
                      AND open_date IS NOT NULL
                      AND open_date >= date_trunc('month',CURRENT_DATE)-INTERVAL '11 months'
                      AND open_date < date_trunc('month',CURRENT_DATE)+INTERVAL '1 month'
                    GROUP BY 1
                ),
                ce AS (
                    SELECT date_trunc('month',close_date)::date month_start,COUNT(*) cnt
                    FROM stores
                    WHERE {where}
                      AND close_date IS NOT NULL
                      AND close_date >= date_trunc('month',CURRENT_DATE)-INTERVAL '11 months'
                      AND close_date < date_trunc('month',CURRENT_DATE)+INTERVAL '1 month'
                    GROUP BY 1
                )
                SELECT m.month_start,COALESCE(o.cnt,0),COALESCE(c.cnt,0)
                FROM months m
                LEFT JOIN oe o USING(month_start)
                LEFT JOIN ce c USING(month_start)
                ORDER BY m.month_start
            """,params+params)
            monthly=cur.fetchall()

            cur.execute(f"""
                SELECT
                    COALESCE(NULLIF(category,''),'業種未分類'),
                    COUNT(*) FILTER(
                        WHERE open_date IS NOT NULL
                          AND open_date>=CURRENT_DATE-INTERVAL '365 days'
                          AND open_date<=CURRENT_DATE
                    ),
                    COUNT(*) FILTER(
                        WHERE close_date IS NOT NULL
                          AND close_date>=CURRENT_DATE-INTERVAL '365 days'
                          AND close_date<=CURRENT_DATE
                    )
                FROM stores
                WHERE {where}
                GROUP BY 1
                HAVING
                    COUNT(*) FILTER(
                        WHERE open_date IS NOT NULL
                          AND open_date>=CURRENT_DATE-INTERVAL '365 days'
                          AND open_date<=CURRENT_DATE
                    )
                    +
                    COUNT(*) FILTER(
                        WHERE close_date IS NOT NULL
                          AND close_date>=CURRENT_DATE-INTERVAL '365 days'
                          AND close_date<=CURRENT_DATE
                    ) > 0
                ORDER BY 2+3 DESC,1
                LIMIT 20
            """,params)
            cats=cur.fetchall()

    monthly_items=[
        {"month":m.isoformat(),"open":int(o or 0),"close":int(c or 0),"net":int(o or 0)-int(c or 0)}
        for m,o,c in monthly
    ]
    category_items=[
        {"category":cat,"open":int(o or 0),"close":int(c or 0),"net":int(o or 0)-int(c or 0)}
        for cat,o,c in cats
    ]

    return {
        "prefecture":prefecture,
        "city":city,
        "period":"last_12_months",
        "open":sum(x["open"] for x in monthly_items),
        "close":sum(x["close"] for x in monthly_items),
        "net":sum(x["net"] for x in monthly_items),
        "monthly":monthly_items,
        "categories":category_items
    }


# ---- Public SEO HTML pages ----

@app.get("/seo/area/{prefecture}",response_class=HTMLResponse)
def seo_area_prefecture(
    prefecture:str,
    page:int=Query(default=1,ge=1,le=1000),
    status:str=Query(default="all")
):
    if not DATABASE_URL:
        raise HTTPException(503,"Database unavailable")
    return HTMLResponse(
        render_area(
            DATABASE_URL,PUBLIC_SITE_ORIGIN,prefecture,
            page=page,status=status
        )
    )

@app.get("/seo/area/{prefecture}/{city}",response_class=HTMLResponse)
def seo_area_city(
    prefecture:str,
    city:str,
    page:int=Query(default=1,ge=1,le=1000),
    status:str=Query(default="all")
):
    if not DATABASE_URL:
        raise HTTPException(503,"Database unavailable")
    return HTMLResponse(
        render_area(
            DATABASE_URL,PUBLIC_SITE_ORIGIN,prefecture,
            city=city,page=page,status=status
        )
    )

@app.get("/seo/category/{category}",response_class=HTMLResponse)
def seo_category(
    category:str,
    page:int=Query(default=1,ge=1,le=1000),
    status:str=Query(default="all")
):
    if not DATABASE_URL:
        raise HTTPException(503,"Database unavailable")
    return HTMLResponse(
        render_category(
            DATABASE_URL,PUBLIC_SITE_ORIGIN,category,
            page=page,status=status
        )
    )

@app.get("/seo/store/{store_id}",response_class=HTMLResponse)
def seo_store(store_id:int):
    if not DATABASE_URL:
        raise HTTPException(503,"Database unavailable")
    page=render_store(DATABASE_URL,PUBLIC_SITE_ORIGIN,store_id)
    if page is None:
        raise HTTPException(404,"Store not found")
    return HTMLResponse(page)

@app.get("/seo/sitemap.xml")
def seo_sitemap():
    if not DATABASE_URL:
        raise HTTPException(503,"Database unavailable")
    return Response(content=sitemap_xml(DATABASE_URL,PUBLIC_SITE_ORIGIN),media_type="application/xml")

@app.get("/seo/robots.txt",response_class=PlainTextResponse)
def seo_robots():
    return PlainTextResponse(robots_txt(PUBLIC_SITE_ORIGIN))


@app.post("/api/category-backfill",dependencies=[Depends(require_admin)])
def category_backfill(limit:int=Query(default=200,ge=1,le=1000)):
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}
    return {
        "ok":True,
        **backfill_store_categories(DATABASE_URL,limit=limit)
    }


@app.post("/api/collect",dependencies=[Depends(require_admin)])
def collect():
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}

    result=run_collectors(DATABASE_URL)

    record_cycle_source_runs(
        DATABASE_URL,
        hub=result.get("hub"),
        rss=result
    )

    return {"ok":True,**result}

@app.post("/api/enrich-addresses",dependencies=[Depends(require_admin)])
def enrich_addresses(limit:int=Query(default=50,ge=1,le=200)):
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}
    return {"ok":True,**enrich_candidates(DATABASE_URL,limit=limit,include_processed=True)}

@app.post("/api/backfill-addresses",dependencies=[Depends(require_admin)])
def backfill_addresses(limit:int=Query(default=100,ge=1,le=500)):
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}
    return {"ok":True,**enrich_candidates(DATABASE_URL,limit=limit,include_processed=True)}

@app.post("/api/process",dependencies=[Depends(require_admin)])
def process(
    min_confidence:int=Query(default=80,ge=60,le=98),
    enrich_limit:int=Query(default=40,ge=0,le=200)
):
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}

    return {
        "ok":True,
        **promote_candidates(
            DATABASE_URL,
            min_confidence=min_confidence,
            enrich_limit=enrich_limit
        )
    }

@app.get("/api/test-article",dependencies=[Depends(require_admin)])
def test_article(url:str):
    return fetch_article_facts(url)

@app.get("/api/address-audit",dependencies=[Depends(require_admin)])
def address_audit(limit:int=Query(default=50,ge=1,le=100)):
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}
    return {"ok":True,**audit_addresses(DATABASE_URL,limit=limit)}

@app.get("/api/resolve-news-url",dependencies=[Depends(require_admin)])
def resolve_news_url(url:str):
    return resolve_google_news_url(url)

@app.post("/api/resolve-and-backfill",dependencies=[Depends(require_admin)])
def resolve_and_backfill():
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}
    return {
        "ok":True,
        "batch_size":5,
        **enrich_candidates(DATABASE_URL,limit=5,include_processed=True)
    }

@app.post("/api/rescue-sources",dependencies=[Depends(require_admin)])
def rescue_source_batch():
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}

    # rescue_sources itself catches per-record errors.
    result=rescue_sources(DATABASE_URL,batch_size=5)
    return {"ok":True,**result}

@app.get("/api/rescue-status",dependencies=[Depends(require_admin)])
def rescue_status():
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}

    conn=db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COALESCE(rescue_status,'not_attempted') AS status,
                    COUNT(*)
                FROM discovery_items
                GROUP BY COALESCE(rescue_status,'not_attempted')
                ORDER BY 2 DESC
            """)
            rows=cur.fetchall()

    return {
        "ok":True,
        "summary":{r[0]:r[1] for r in rows}
    }

@app.get("/api/address-quality-audit",dependencies=[Depends(require_admin)])
def address_quality_audit():
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}
    return {"ok":True,**audit_and_clean(DATABASE_URL,apply=False,limit=100)}

@app.post("/api/address-quality-clean",dependencies=[Depends(require_admin)])
def address_quality_clean():
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}
    return {"ok":True,**audit_and_clean(DATABASE_URL,apply=True,limit=100)}

@app.get("/api/non-store-audit",dependencies=[Depends(require_admin)])
def non_store_audit():
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}
    return {"ok":True,**audit_non_store_events(DATABASE_URL,apply=False,limit=200)}

@app.post("/api/non-store-clean",dependencies=[Depends(require_admin)])
def non_store_clean():
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}
    return {"ok":True,**audit_non_store_events(DATABASE_URL,apply=True,limit=200)}

@app.post("/api/rescue-retry",dependencies=[Depends(require_admin)])
def rescue_retry():
    """
    新しい救済ロジックを試せるよう、失敗/未対応/住所未取得を再キュー化。
    excluded_event は戻さない。
    """
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}

    conn=db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE discovery_items
                SET rescue_attempted_at=NULL,
                    rescue_status=NULL,
                    rescue_error=NULL
                WHERE rescue_status IN(
                    'unsupported_publisher',
                    'relocation_failed',
                    'error',
                    'relocated_no_valid_address'
                )
            """)
            reset=cur.rowcount

    return {"ok":True,"reset":reset}

@app.get("/api/name-quality-audit",dependencies=[Depends(require_admin)])
def name_quality_audit(limit:int=Query(default=100,ge=1,le=300)):
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}

    conn=db_conn()
    items=[]
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.id,s.name,d.title
                FROM stores s
                JOIN LATERAL(
                    SELECT title
                    FROM discovery_items d
                    WHERE d.store_name_candidate=s.name
                    ORDER BY d.id DESC
                    LIMIT 1
                ) d ON TRUE
                WHERE COALESCE(s.status,'') <> 'excluded'
                ORDER BY s.id DESC
                LIMIT %s
            """,(limit,))
            rows=cur.fetchall()

    for sid,current,title in rows:
        better=extract_best_store_name_from_title(title)
        if better and better != current:
            items.append({
                "store_id":sid,
                "current_name":current,
                "suggested_name":better,
                "title":title
            })

    return {"ok":True,"count":len(items),"items":items}


@app.get("/api/discovery-source-status",dependencies=[Depends(require_admin)])
def discovery_source_status():
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}

    conn=db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COALESCE(discovery_channel,'unknown') channel,
                    COALESCE(publisher_name,source_name,'unknown') source,
                    detected_status,
                    COUNT(*) total,
                    COUNT(*) FILTER(WHERE processed=FALSE) unprocessed,
                    COUNT(*) FILTER(WHERE address_candidate IS NOT NULL) with_address
                FROM discovery_items
                GROUP BY 1,2,3
                ORDER BY total DESC,source
                LIMIT 250
            """)
            rows=cur.fetchall()

    return {
        "ok":True,
        "items":[
            {
                "channel":r[0],"source":r[1],"status":r[2],
                "total":r[3],"unprocessed":r[4],"with_address":r[5]
            }
            for r in rows
        ]
    }

@app.get("/api/tenant-watch-status",dependencies=[Depends(require_admin)])
def tenant_status():
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}
    return {"ok":True,**tenant_watch_status(DATABASE_URL)}

@app.post("/api/tenant-watch",dependencies=[Depends(require_admin)])
def tenant_watch(
    limit_stores:int=Query(default=5,ge=1,le=20)
):
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}

    existing=promote_existing_tenant_candidates(DATABASE_URL,limit=120)
    searched=scan_closed_store_tenant_news(
        DATABASE_URL,limit_stores=limit_stores,per_store=12
    )
    verified=verify_tenant_listings(DATABASE_URL,limit=20)

    return {
        "ok":True,
        "existing_candidates":existing,
        "closed_store_search":searched,
        "verification":verified
    }

@app.post("/api/full-cycle",dependencies=[Depends(require_admin)])
def full_cycle():
    """
    Hourly low-cost cycle:
    1) dedicated open/close sources + one backfill page
    2) diversified 18-source RSS discovery
    3) rotating prefecture-by-prefecture closing discovery
    4) detail enrichment/promotion
    5) category repair
    6) closed-store tenant watch + listing recheck
    """
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}

    hub_collected=collect_openclose_hub(DATABASE_URL)
    backfill=collect_backfill_step(DATABASE_URL)

    rss=run_collectors(DATABASE_URL,include_hub=False)

    # Closing-heavy regional rotation: eight prefectures per run.
    # This prevents opening-heavy feeds from dominating national coverage.
    prefecture_closing=collect_prefecture_closings(
        DATABASE_URL,batch_size=8,limit_per_prefecture=18
    )

    source_health_log=record_cycle_source_runs(
        DATABASE_URL,
        hub=hub_collected,
        rss=rss,
        backfill=backfill
    )

    hub_enriched=enrich_hub_candidates(DATABASE_URL,limit=20)
    hub_processed=promote_hub_candidates(
        DATABASE_URL,hub_enriched.get("enriched_ids",[])
    )

    # Non-hub feeds are promoted conservatively after article enrichment.
    rss_processed=promote_candidates(
        DATABASE_URL,min_confidence=82,enrich_limit=20
    )

    category_repair=backfill_store_categories(
        DATABASE_URL,limit=100
    )

    tenant_existing=promote_existing_tenant_candidates(
        DATABASE_URL,limit=120
    )
    tenant_search=scan_closed_store_tenant_news(
        DATABASE_URL,limit_stores=5,per_store=12
    )
    tenant_verify=verify_tenant_listings(
        DATABASE_URL,limit=20
    )

    return {
        "ok":True,
        "hub":{
            "collected":hub_collected,
            "backfill":backfill,
            "enriched":hub_enriched,
            "processed":hub_processed
        },
        "rss":rss,
        "prefecture_closing":prefecture_closing,
        "source_health_log":source_health_log,
        "rss_processed":rss_processed,
        "category_repair":category_repair,
        "tenant":{
            "existing_candidates":tenant_existing,
            "closed_store_search":tenant_search,
            "verification":tenant_verify
        }
    }

@app.post("/api/collect-hub",dependencies=[Depends(require_admin)])
def collect_hub():
    """
    開店閉店系の専用サイトだけを収集。
    Google Newsは使わない。
    """
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}
    return {"ok":True,**collect_openclose_hub(DATABASE_URL)}

@app.post("/api/enrich-hub",dependencies=[Depends(require_admin)])
def enrich_hub():
    """
    開店閉店系サイトの個別記事から、
    住所・正式店名・開閉店日・公式URL候補を20件固定で補完。
    """
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}
    return {"ok":True,**enrich_hub_candidates(DATABASE_URL,limit=20)}

@app.post("/api/hub-cycle",dependencies=[Depends(require_admin)])
def hub_cycle():
    """
    専用サイト収集 → 20件だけ詳細補完 → その20件の合格分だけDBへ昇格。
    """
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}

    collected=collect_openclose_hub(DATABASE_URL)
    backfill=collect_backfill_step(DATABASE_URL)

    record_cycle_source_runs(
        DATABASE_URL,
        hub=collected,
        backfill=backfill
    )

    enriched=enrich_hub_candidates(DATABASE_URL,limit=20)
    processed=promote_hub_candidates(
        DATABASE_URL,
        enriched.get("enriched_ids",[])
    )
    category_repair=backfill_store_categories(
        DATABASE_URL,
        limit=100
    )
    return {
        "ok":True,
        "collected":collected,
        "backfill":backfill,
        "enriched":enriched,
        "processed":processed,
        "category_repair":category_repair
    }

@app.post("/api/collect-prefecture-closings",dependencies=[Depends(require_admin)])
def collect_prefecture_closings_api(
    batch_size:int=Query(default=8,ge=1,le=47),
    limit_per_prefecture:int=Query(default=18,ge=5,le=50)
):
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}

    return {
        "ok":True,
        **collect_prefecture_closings(
            DATABASE_URL,
            batch_size=batch_size,
            limit_per_prefecture=limit_per_prefecture
        )
    }

@app.get("/api/prefecture-coverage",dependencies=[Depends(require_admin)])
def prefecture_coverage():
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}

    return {
        "ok":True,
        **get_prefecture_coverage(DATABASE_URL)
    }

@app.get("/api/source-health",dependencies=[Depends(require_admin)])
def source_health():
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}

    return {
        "ok":True,
        **get_source_health(DATABASE_URL)
    }

@app.get("/api/source-hub-status",dependencies=[Depends(require_admin)])
def source_hub_status():
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}

    conn=db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    source_name,
                    detected_status,
                    discovery_channel,
                    COUNT(*),
                    COUNT(*) FILTER(WHERE processed=FALSE),
                    COUNT(*) FILTER(WHERE address_candidate IS NOT NULL),
                    COUNT(*) FILTER(WHERE official_url_candidate IS NOT NULL)
                FROM discovery_items
                WHERE discovery_channel IN('openclose_hub','openclose_hub_backfill')
                GROUP BY source_name,detected_status,discovery_channel
                ORDER BY source_name,detected_status,discovery_channel
            """)
            rows=cur.fetchall()

    return {
        "ok":True,
        "items":[{
            "source_name":r[0],
            "status":r[1],
            "channel":r[2],
            "total":r[3],
            "unprocessed":r[4],
            "with_address":r[5],
            "with_official_url":r[6]
        } for r in rows]
    }

@app.post("/api/hub-repair-reset",dependencies=[Depends(require_admin)])
def hub_repair_reset():
    """
    v1.3.0で詳細未確認のまま昇格したHub由来店舗を一度非表示にし、
    Hub候補を再検証可能な状態へ戻す。
    """
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}
    return {"ok":True,**reset_and_hide_hub_promotions(DATABASE_URL)}

@app.post("/api/backfill-step",dependencies=[Depends(require_admin)])
def backfill_step():
    """
    開店閉店.comの過去ページを1回につき1ページだけ遡る。
    opening / closing は last_run_at の古い方を選ぶので交互に進む。
    """
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}
    return {"ok":True,**collect_backfill_step(DATABASE_URL)}

@app.get("/api/backfill-status",dependencies=[Depends(require_admin)])
def backfill_status():
    """
    開店/閉店それぞれ、今どのページまで遡ったか確認。
    """
    if not DATABASE_URL:
        return {"ok":False,"error":"DATABASE_URL is not configured"}
    return {"ok":True,**get_backfill_status(DATABASE_URL)}


# Public correction form with an authenticated operator inbox.
from services.open_close.inquiries import install_inquiries
install_inquiries(app, db_conn, require_admin)

