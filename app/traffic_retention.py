"""Bound high-frequency BUZZ NOW history without deleting search content.

Core content tables (trends, sources, related_keywords, corporate_items and social
posts) are intentionally excluded. Forecast traffic history is rolled up to daily
summaries; disposable signal/history tables are pruned by age in bounded batches.
"""
from datetime import datetime, timedelta, timezone
import logging
from math import isclose
import time

logger = logging.getLogger(__name__)
KEEP = 100
BATCH_SIZE = 20000
PRUNE_BATCH_SIZE = 10000
SUMMARY_DAYS = 365
LOCK_KEY = 748290163
RUN_INTERVAL_SECONDS = 6 * 60 * 60
METRICS = ('impressions', 'clicks', 'pageviews', 'ctr', 'traffic_potential')

# These are high-frequency observations, not user-facing/search content.
# Keep enough detail for recent diagnostics while preventing unbounded growth.
RETENTION_POLICIES = (
    ('trend_history', 'recorded_at', 30),
    ('growth_log', 'recorded_at', 30),
    ('source_snapshots', 'captured_at', 14),
    ('v9_signal_history', 'captured_at', 14),
)

_last_attempt_monotonic = 0.0


def should_sample(previous, values, timestamp):
    """Save changes immediately and an unchanged observation at least hourly."""
    if previous is None:
        return True
    for key in METRICS:
        if key in ('ctr', 'traffic_potential'):
            if not isclose(previous[key], values[key], rel_tol=0, abs_tol=0.00001):
                return True
        elif previous[key] != values[key]:
            return True
    try:
        now = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        last = datetime.fromisoformat(previous['recorded_at'].replace('Z', '+00:00'))
        return now < last or now - last >= timedelta(hours=1)
    except (ValueError, TypeError):
        return True


def latest_samples(c, postgres):
    sql = ('''SELECT DISTINCT ON (trend_id) * FROM traffic_history
              ORDER BY trend_id,id DESC''' if postgres else
           '''SELECT h.* FROM traffic_history h JOIN
              (SELECT trend_id,max(id) AS id FROM traffic_history GROUP BY trend_id) x
              ON h.id=x.id''')
    return {r['trend_id']: dict(r) for r in c.execute(sql).fetchall()}


_columns = ',\n'.join(f'{m}_sum DOUBLE PRECISION NOT NULL, {m}_max DOUBLE PRECISION NOT NULL' for m in METRICS)
SCHEMA_SQL = f'''CREATE TABLE IF NOT EXISTS traffic_daily_archive (
    trend_id BIGINT NOT NULL,
    day TEXT NOT NULL,
    samples BIGINT NOT NULL,
    first_recorded_at TEXT NOT NULL,
    last_recorded_at TEXT NOT NULL,
    {_columns},
    PRIMARY KEY (trend_id,day)
)'''
INDEX_SQL = '''CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_traffic_history_trend_id
               ON traffic_history(trend_id,id DESC)'''

_names = ','.join(f'{m}_sum,{m}_max' for m in METRICS)
_groups = ','.join(f'sum({m}::double precision),max({m}::double precision)' for m in METRICS)
_updates = ','.join(f'''{m}_sum=traffic_daily_archive.{m}_sum+excluded.{m}_sum,
    {m}_max=greatest(traffic_daily_archive.{m}_max,excluded.{m}_max)''' for m in METRICS)
ARCHIVE_SQL = f'''
WITH cutoffs AS MATERIALIZED (
    SELECT t.id AS trend_id,
      (SELECT h.id FROM traffic_history h WHERE h.trend_id=t.id
       ORDER BY h.id DESC OFFSET {KEEP} LIMIT 1) AS cutoff
    FROM trends t
), candidates AS MATERIALIZED (
    SELECT h.id FROM traffic_history h JOIN cutoffs c ON c.trend_id=h.trend_id
    WHERE h.id<=c.cutoff ORDER BY h.id LIMIT {BATCH_SIZE}
), removed AS (
    DELETE FROM traffic_history h USING candidates c WHERE h.id=c.id
    RETURNING h.*
), saved AS (
    INSERT INTO traffic_daily_archive
      (trend_id,day,samples,first_recorded_at,last_recorded_at,{_names})
    SELECT trend_id,substring(recorded_at,1,10),count(*),min(recorded_at),max(recorded_at),{_groups}
    FROM removed GROUP BY trend_id,substring(recorded_at,1,10)
    ON CONFLICT (trend_id,day) DO UPDATE SET
      samples=traffic_daily_archive.samples+excluded.samples,
      first_recorded_at=least(traffic_daily_archive.first_recorded_at,excluded.first_recorded_at),
      last_recorded_at=greatest(traffic_daily_archive.last_recorded_at,excluded.last_recorded_at),
      {_updates}
    RETURNING samples
)
SELECT (SELECT count(*) FROM removed) AS archived_rows,
       (SELECT count(*) FROM saved) AS summary_groups
'''


def _table_has_column(c, table, column):
    row = c.execute('''SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name=? AND column_name=?
    ) AS present''', (table, column)).fetchone()
    return bool(row and row['present'])


def _prune_by_age(c, table, timestamp_column, days):
    """Delete at most one bounded batch and return only the count to the app."""
    if not _table_has_column(c, table, timestamp_column):
        return 0
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    # Identifiers come only from RETENTION_POLICIES constants above.
    sql = f'''WITH doomed AS MATERIALIZED (
        SELECT id FROM {table}
        WHERE {timestamp_column} < ?
        ORDER BY id
        LIMIT {PRUNE_BATCH_SIZE}
    ), removed AS (
        DELETE FROM {table} t USING doomed d WHERE t.id=d.id RETURNING 1
    ) SELECT count(*) AS deleted FROM removed'''
    row = c.execute(sql, (cutoff,)).fetchone()
    return int(row['deleted'] if row else 0)


def _largest_tables(c, limit=12):
    """Small diagnostic payload; no table contents leave Postgres."""
    rows = c.execute('''SELECT relname AS table_name,
        pg_total_relation_size(relid) AS bytes
        FROM pg_catalog.pg_statio_user_tables
        ORDER BY pg_total_relation_size(relid) DESC
        LIMIT ?''', (limit,)).fetchall()
    return [{'table': r['table_name'], 'bytes': int(r['bytes'])} for r in rows]


def run(db_factory):
    """Run retention at most once every six hours per process.

    The scheduler may invoke this wrapper every two minutes, but the local throttle
    prevents those invocations from opening a database connection. This is important
    on metered Neon plans where connection/query churn itself contributes to usage.
    """
    global _last_attempt_monotonic
    now_mono = time.monotonic()
    if _last_attempt_monotonic and now_mono - _last_attempt_monotonic < RUN_INTERVAL_SECONDS:
        return {'skipped': 'local_throttle'}
    _last_attempt_monotonic = now_mono

    try:
        with db_factory() as c:
            c.execute("SET LOCAL lock_timeout='2s'")
            c.execute("SET LOCAL statement_timeout='45s'")
            locked = c.execute('SELECT pg_try_advisory_xact_lock(?) AS locked', (LOCK_KEY,)).fetchone()['locked']
            if not locked:
                return {'skipped': 'already_running'}

            c.execute(SCHEMA_SQL)
            sizes_before = _largest_tables(c)
            result = dict(c.execute(ARCHIVE_SQL).fetchone())
            archive_cutoff = (datetime.now(timezone.utc).date() - timedelta(days=SUMMARY_DAYS-1)).isoformat()
            c.execute('DELETE FROM traffic_daily_archive WHERE day < ?', (archive_cutoff,))

            pruned = {}
            for table, column, days in RETENTION_POLICIES:
                pruned[table] = _prune_by_age(c, table, column, days)
            sizes_after = _largest_tables(c)
            result.update({'pruned': pruned, 'largest_before': sizes_before, 'largest_after': sizes_after})

        logger.info('DB retention committed: %s', result)
        return result
    except Exception as exc:
        # Quota exhaustion must not create a tight reconnect/error loop.
        logger.warning('DB retention unavailable; no rows were changed: %s', exc)
        return {'skipped': 'database_unavailable', 'error': type(exc).__name__}


def daily(c, trend_id, cutoff):
    """Merge archived and still-live samples: no holes or duplicate counting."""
    raw = ','.join(f'sum({m}) AS {m}_sum,max({m}) AS {m}_max' for m in METRICS)
    final = ','.join(f'sum({m}_sum)*1.0/sum(samples) AS {m}_average,max({m}_max) AS {m}_max' for m in METRICS)
    return [dict(r) for r in c.execute(f'''
        WITH combined AS (
          SELECT day,samples,{_names} FROM traffic_daily_archive WHERE trend_id=? AND day>=?
          UNION ALL
          SELECT substr(recorded_at,1,10) AS day,count(*) AS samples,{raw}
          FROM traffic_history WHERE trend_id=? AND recorded_at>=?
          GROUP BY substr(recorded_at,1,10)
        )
        SELECT day,sum(samples) AS samples,{final}
        FROM combined GROUP BY day ORDER BY day
    ''', (trend_id,cutoff,trend_id,cutoff)).fetchall()]
