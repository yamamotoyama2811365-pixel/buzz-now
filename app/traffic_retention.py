"""Bound forecast history; archive summaries and delete raw rows atomically.

These metrics are forecasts, not measured website traffic. Never sum forecasts
and label the result as actual PV. The sums below are only averaging weights.
"""
from datetime import datetime, timedelta, timezone
import logging
from math import isclose

logger = logging.getLogger(__name__)
KEEP = 100
BATCH_SIZE = 20000
SUMMARY_DAYS = 365
LOCK_KEY = 748290163
METRICS = ('impressions', 'clicks', 'pageviews', 'ctr', 'traffic_potential')


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


def run(db_factory):
    """One bounded transaction; another instance skips rather than double-counts."""
    try:
        with db_factory() as c:
            c.execute("SET LOCAL lock_timeout='2s'")
            c.execute("SET LOCAL statement_timeout='45s'")
            locked = c.execute('SELECT pg_try_advisory_xact_lock(?) AS locked', (LOCK_KEY,)).fetchone()['locked']
            if not locked:
                return {'skipped': 'already_running'}
            result = dict(c.execute(ARCHIVE_SQL).fetchone())
            cutoff = (datetime.now(timezone.utc).date() - timedelta(days=SUMMARY_DAYS-1)).isoformat()
            c.execute('DELETE FROM traffic_daily_archive WHERE day < ?', (cutoff,))
        logger.info('Traffic retention committed: %s', result)
        return result
    except Exception:
        logger.exception('Traffic retention rolled back; original rows remain available')
        raise


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
