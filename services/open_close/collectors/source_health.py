from datetime import datetime, timezone

from .sources import RSS_SOURCES
from .openclose_hub import HUB_SOURCES


def ensure_source_run_table(database_url):
    import psycopg
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
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


def _insert_run(cur, family, source_key, source_name,
                fetched=0, inserted=0, duplicates=0, rejected=0, errors=None):
    errors = errors or []
    error_text = "; ".join(str(x)[:300] for x in errors if x)[:1800] or None
    error_count = len([x for x in errors if x])

    # "ok" means the collector itself returned without a total failure.
    # 0 new inserts is not a failure: duplicates are normal on hourly runs.
    ok = not (error_count > 0 and int(fetched or 0) == 0)

    cur.execute("""
        INSERT INTO collector_source_runs(
            family,source_key,source_name,
            fetched,inserted,duplicates,rejected,
            error_count,error_text,ok
        )
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """,(
        family,source_key,source_name,
        int(fetched or 0),int(inserted or 0),
        int(duplicates or 0),int(rejected or 0),
        error_count,error_text,ok
    ))


def record_cycle_source_runs(database_url, hub=None, rss=None, backfill=None):
    """
    Persist one row per collector source after a cycle.
    Results are intentionally operational metadata only.
    """
    import psycopg

    ensure_source_run_table(database_url)

    written = 0
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            if rss:
                for item in rss.get("sources",[]) or []:
                    _insert_run(
                        cur,
                        "rss",
                        item.get("key") or item.get("source") or "unknown",
                        item.get("source") or item.get("key") or "unknown",
                        fetched=item.get("fetched",0),
                        inserted=item.get("inserted",0),
                        duplicates=item.get("duplicates",0),
                        rejected=item.get("rejected",0),
                        errors=item.get("errors",[])
                    )
                    written += 1

            if hub:
                for item in hub.get("sources",[]) or []:
                    _insert_run(
                        cur,
                        "hub",
                        item.get("key") or item.get("source") or "unknown",
                        item.get("source") or item.get("key") or "unknown",
                        fetched=item.get("found",item.get("fetched",0)),
                        inserted=item.get("inserted",0),
                        duplicates=item.get("duplicates",0),
                        rejected=item.get("rejected",0),
                        errors=item.get("errors",[])
                    )
                    written += 1

            if backfill:
                step = backfill.get("step") if isinstance(backfill,dict) else None
                if step:
                    _insert_run(
                        cur,
                        "backfill",
                        "backfill:" + str(step.get("source_key") or "unknown"),
                        str(step.get("source_name") or step.get("source_key") or "backfill"),
                        fetched=step.get("found",0),
                        inserted=step.get("inserted",0),
                        duplicates=step.get("duplicates",0),
                        rejected=0,
                        errors=step.get("errors",[])
                    )
                    written += 1

    return {"written":written}


def _status(latest):
    if latest is None:
        return "never_run"
    fetched = int(latest.get("fetched") or 0)
    error_count = int(latest.get("error_count") or 0)
    if error_count > 0 and fetched == 0:
        return "error"
    if error_count > 0:
        return "warning"
    if fetched == 0:
        return "idle"
    return "healthy"


def _configured():
    items = []

    for src in RSS_SOURCES:
        items.append({
            "family":"rss",
            "source_key":src["key"],
            "source_name":src["name"],
            "channel":src.get("channel"),
            "configured_limit":src.get("limit")
        })

    for src in HUB_SOURCES:
        items.append({
            "family":"hub",
            "source_key":src["key"],
            "source_name":src["name"],
            "channel":"openclose_hub",
            "configured_limit":src.get("max_items")
        })

    return items


def get_source_health(database_url):
    import psycopg

    ensure_source_run_table(database_url)

    configured = _configured()

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT ON(source_key)
                    source_key,source_name,family,run_at,
                    fetched,inserted,duplicates,rejected,
                    error_count,error_text,ok
                FROM collector_source_runs
                ORDER BY source_key,run_at DESC,id DESC
            """)
            latest_rows = cur.fetchall()

            cur.execute("""
                SELECT
                    source_key,
                    COALESCE(SUM(inserted) FILTER(
                        WHERE run_at >= NOW()-INTERVAL '24 hours'
                    ),0) AS inserted_24h,
                    COALESCE(SUM(fetched) FILTER(
                        WHERE run_at >= NOW()-INTERVAL '24 hours'
                    ),0) AS fetched_24h,
                    COALESCE(SUM(error_count) FILTER(
                        WHERE run_at >= NOW()-INTERVAL '24 hours'
                    ),0) AS errors_24h,
                    COALESCE(SUM(inserted) FILTER(
                        WHERE run_at >= NOW()-INTERVAL '7 days'
                    ),0) AS inserted_7d,
                    COALESCE(SUM(fetched) FILTER(
                        WHERE run_at >= NOW()-INTERVAL '7 days'
                    ),0) AS fetched_7d,
                    MAX(run_at)
                FROM collector_source_runs
                WHERE run_at >= NOW()-INTERVAL '7 days'
                GROUP BY source_key
            """)
            aggregate_rows = cur.fetchall()

    latest = {}
    for r in latest_rows:
        latest[r[0]] = {
            "source_key":r[0],
            "source_name":r[1],
            "family":r[2],
            "last_run_at":r[3].isoformat() if r[3] else None,
            "fetched":r[4],
            "inserted":r[5],
            "duplicates":r[6],
            "rejected":r[7],
            "error_count":r[8],
            "error":r[9],
            "ok":r[10]
        }

    aggregates = {}
    for r in aggregate_rows:
        aggregates[r[0]] = {
            "inserted_24h":int(r[1] or 0),
            "fetched_24h":int(r[2] or 0),
            "errors_24h":int(r[3] or 0),
            "inserted_7d":int(r[4] or 0),
            "fetched_7d":int(r[5] or 0),
            "last_run_at":r[6].isoformat() if r[6] else None
        }

    output = []
    for cfg in configured:
        key = cfg["source_key"]
        last = latest.get(key)
        agg = aggregates.get(key,{
            "inserted_24h":0,"fetched_24h":0,"errors_24h":0,
            "inserted_7d":0,"fetched_7d":0,"last_run_at":None
        })

        row = dict(cfg)
        row["status"] = _status(last)
        row["latest"] = last
        row["rolling"] = agg
        output.append(row)

    counts = {
        "configured":len(output),
        "healthy":sum(1 for x in output if x["status"]=="healthy"),
        "warning":sum(1 for x in output if x["status"]=="warning"),
        "error":sum(1 for x in output if x["status"]=="error"),
        "idle":sum(1 for x in output if x["status"]=="idle"),
        "never_run":sum(1 for x in output if x["status"]=="never_run"),
    }

    return {
        "generated_at":datetime.now(timezone.utc).isoformat(),
        "summary":counts,
        "items":output
    }

