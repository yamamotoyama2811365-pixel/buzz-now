import os
import unittest
from datetime import datetime, timedelta, timezone
from app import traffic_retention as retention


class SamplingTest(unittest.TestCase):
    def setUp(self):
        self.values = dict(zip(retention.METRICS, (100, 10, 8, 2.0, 80.0)))
        self.previous = dict(self.values, recorded_at='2026-09-10T00:00:00+00:00')

    def test_first_change_and_heartbeat(self):
        self.assertTrue(retention.should_sample(None, self.values, '2026-09-10T00:00:00+00:00'))
        self.assertFalse(retention.should_sample(self.previous, self.values, '2026-09-10T00:59:59+00:00'))
        self.assertTrue(retention.should_sample(self.previous, self.values, '2026-09-10T01:00:00+00:00'))
        for metric in retention.METRICS:
            changed = dict(self.values); changed[metric] += 1
            self.assertTrue(retention.should_sample(self.previous, changed, '2026-09-10T00:01:00+00:00'))

    def test_postgres_real_rounding_does_not_create_fake_changes(self):
        previous = dict(self.previous, ctr=2.369999885559082, traffic_potential=80.0999984741211)
        values = dict(self.values, ctr=2.37, traffic_potential=80.1)
        self.assertFalse(retention.should_sample(previous, values, '2026-09-10T00:30:00+00:00'))

    def test_bad_timestamp_and_clock_reversal_keep_sample(self):
        for timestamp in ('invalid', '2026-09-09T23:00:00+00:00'):
            self.assertTrue(retention.should_sample(self.previous, self.values, timestamp))


@unittest.skipUnless(os.getenv('TEST_DATABASE_URL'), 'PostgreSQL integration runs in CI')
class ArchiveTest(unittest.TestCase):
    def setUp(self):
        import psycopg
        from psycopg.rows import dict_row
        self.url = os.environ['TEST_DATABASE_URL']
        # This suite is destructive only to its disposable local CI fixture.
        assert self.url == 'postgresql://postgres:postgres@localhost:5432/retention_test'
        self.connect = lambda: psycopg.connect(self.url, row_factory=dict_row)
        with self.connect() as c:
            c.execute('DROP SCHEMA public CASCADE; CREATE SCHEMA public')
            c.execute('CREATE TABLE trends(id bigint primary key)')
            c.execute('CREATE TABLE traffic_history(id bigserial primary key,trend_id bigint,impressions bigint,clicks bigint,pageviews bigint,ctr real,traffic_potential real,recorded_at text)')
            c.execute('CREATE INDEX ON traffic_history(trend_id,id DESC)')
            c.execute(retention.SCHEMA_SQL)
            c.execute('INSERT INTO trends VALUES (1),(2),(3)')
            self.day = (datetime.now(timezone.utc).date()-timedelta(days=1)).isoformat()
            c.execute('''INSERT INTO traffic_history(trend_id,impressions,clicks,pageviews,ctr,traffic_potential,recorded_at)
                         SELECT t,n*10,n*2,n,2.5,n/10.0,%s FROM generate_series(1,2) t CROSS JOIN generate_series(1,250) n''', (self.day+'T00:00:00+00:00',))
            c.execute('''INSERT INTO traffic_history(trend_id,impressions,clicks,pageviews,ctr,traffic_potential,recorded_at)
                         SELECT 3,n,n,n,1,1,%s FROM generate_series(1,5) n''', (self.day+'T00:00:00+00:00',))
            self.latest = c.execute('SELECT id FROM (SELECT id,row_number() OVER(PARTITION BY trend_id ORDER BY id DESC) rn FROM traffic_history) x WHERE rn<=100 ORDER BY id').fetchall()
            self.before = c.execute('SELECT count(*) AS n,sum(pageviews) AS pv FROM traffic_history').fetchone()
        class Adapter:
            def __init__(adapter): adapter.c = self.connect()
            def execute(adapter, sql, args=()): return adapter.c.execute(sql.replace('?', '%s'), args)
            def __enter__(adapter): return adapter
            def __exit__(adapter, *args): return adapter.c.__exit__(*args)
        self.db = Adapter

    def test_conservation_latest_rows_and_idempotence(self):
        result = retention.run(self.db)
        self.assertEqual(result['archived_rows'], 300)
        with self.db() as c:
            self.assertEqual(c.execute('SELECT id FROM traffic_history ORDER BY id').fetchall(), self.latest)
            combined = c.execute('SELECT (SELECT count(*) FROM traffic_history)+(SELECT sum(samples) FROM traffic_daily_archive) n,(SELECT sum(pageviews) FROM traffic_history)+(SELECT sum(pageviews_sum) FROM traffic_daily_archive) pv').fetchone()
            self.assertEqual(combined, self.before)
            daily = retention.daily(c, 1, self.day)
            self.assertEqual(daily[0]['samples'], 250)
            self.assertAlmostEqual(daily[0]['pageviews_average'], 125.5)
            self.assertEqual(daily[0]['pageviews_max'], 250)
        self.assertEqual(retention.run(self.db)['archived_rows'], 0)

    def test_archive_failure_rolls_back_raw_deletion(self):
        with self.connect() as c:
            c.execute('ALTER TABLE traffic_daily_archive ADD CONSTRAINT reject_archive CHECK(samples<0)')
        with self.assertRaises(Exception): retention.run(self.db)
        with self.connect() as c:
            self.assertEqual(c.execute('SELECT count(*) AS n,sum(pageviews) AS pv FROM traffic_history').fetchone(), self.before)

    def test_concurrent_worker_skips(self):
        with self.connect() as c:
            c.execute('SELECT pg_advisory_xact_lock(%s)', (retention.LOCK_KEY,))
            self.assertEqual(retention.run(self.db), {'skipped': 'already_running'})

    def test_archive_expiry_preserves_recent_raw_samples(self):
        old = (datetime.now(timezone.utc).date()-timedelta(days=400)).isoformat()
        with self.connect() as c: c.execute('UPDATE traffic_history SET recorded_at=%s', (old+'T00:00:00+00:00',))
        retention.run(self.db)
        with self.connect() as c:
            self.assertEqual(c.execute('SELECT count(*) n FROM traffic_daily_archive').fetchone()['n'], 0)
            self.assertEqual(c.execute('SELECT id FROM traffic_history ORDER BY id').fetchall(), self.latest)

if __name__ == '__main__': unittest.main()
