import importlib.util
import os
from pathlib import Path
import unittest
from unittest.mock import patch
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

here = Path(__file__).resolve()
p = here.parent/'traffic.py'
if not p.exists():
    p = here.parents[1]/'traffic.py'
spec = importlib.util.spec_from_file_location('ocm_traffic_test_module', p)
t = importlib.util.module_from_spec(spec)
spec.loader.exec_module(t)


def authorized():
    raise HTTPException(401, 'Authentication required')


class CounterTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        t.install(self.app, 'unused', 'https://open-close-map.onrender.com', authorized)
        self.client = TestClient(self.app)
        self.headers = {'Origin': 'https://open-close-map.onrender.com', 'User-Agent': 'Mozilla/5.0'}
        self.payload = {'page': '/', 'source': 'google', 'is_test': False}

    def post(self, data=None, **kwargs):
        return self.client.post('/api/page-view', json=self.payload if data is None else data, headers=self.headers, **kwargs)

    def test_valid_and_minimal_saved_fields(self):
        with patch.object(t, 'record', return_value=True) as save:
            r = self.post()
            self.assertEqual(r.status_code, 204)
            self.assertEqual(r.content, b'')
            self.assertEqual(r.headers['cache-control'], 'no-store')
            self.assertNotIn('set-cookie', r.headers)
            save.assert_called_once_with('unused', '/', 'google', False)

    def test_other_origin_rejected(self):
        self.headers['Origin'] = 'https://evil.example'
        with patch.object(t, 'record') as save:
            self.assertEqual(self.post().status_code, 403)
            save.assert_not_called()

    def test_raw_referrer_ip_ids_rejected(self):
        for key in ('referrer', 'ip', 'userId', 'query'):
            self.assertEqual(self.post(dict(self.payload, **{key:'sensitive'})).status_code, 422)

    def test_query_fragment_foreign_paths_rejected(self):
        for page in ('/?email=secret', '/#secret', '/api/stores', '/corporate/', '//evil', '/area/%2e%2e', '/area/%3fsecret', '/store/0', '/store/-1'):
            with self.subTest(page=page):
                self.assertEqual(self.post(dict(self.payload, page=page)).status_code, 422)

    def test_normalization(self):
        self.assertEqual(t.normalize_page('/open'), '/open/')
        self.assertEqual(t.normalize_page('/index.html'), '/')
        self.assertEqual(t.normalize_page('/area/%E5%8C%97%E6%B5%B7%E9%81%93'), '/area/北海道')

    def test_privacy_signals_and_bots_do_not_write(self):
        for h, v in [('DNT','1'), ('Sec-GPC','1'), ('Purpose','prefetch'), ('User-Agent','Googlebot')]:
            with patch.object(t, 'record') as save:
                r = self.client.post('/api/page-view', json=self.payload, headers=dict(self.headers, **{h:v}))
                self.assertEqual(r.status_code, 204)
                save.assert_not_called()

    def test_test_traffic_separated(self):
        with patch.object(t, 'record', return_value=True) as save:
            self.assertEqual(self.post(dict(self.payload, is_test=True)).status_code, 204)
            self.assertTrue(save.call_args.args[3])
        for marker in (1, 'true', None):
            self.assertEqual(self.post(dict(self.payload, is_test=marker)).status_code, 422)

    def test_source_enum(self):
        for source in ([], 'https://www.google.com/search?q=secret', 'google_fake'):
            self.assertEqual(self.post(dict(self.payload, source=source)).status_code, 422)

    def test_large_body(self):
        self.assertEqual(self.post(dict(self.payload, page='/'+'x'*2000)).status_code, 413)

    def test_nonexistent_page_not_counted(self):
        with patch.object(t, 'record', return_value=False):
            self.assertEqual(self.post().status_code, 422)

    def test_failures_do_not_expose_secrets(self):
        with patch.object(t, 'record', side_effect=RuntimeError('postgres://secret')):
            r = self.post()
            self.assertEqual(r.status_code, 503)
            self.assertNotIn('secret', r.text)

    def test_private_report(self):
        self.assertEqual(self.client.get('/api/pv-report').status_code, 401)

    def test_install_once(self):
        before = len(self.app.routes)
        t.install(self.app, 'unused', 'https://open-close-map.onrender.com', authorized)
        self.assertEqual(len(self.app.routes), before)

    def test_rate_limit(self):
        b = t.Budget()
        b.tokens = 0
        self.assertFalse(b.take())


@unittest.skipUnless(os.getenv('OCM_PV_TEST_DSN'), 'CI-only disposable Postgres')
class DatabaseTests(unittest.TestCase):
    def test_aggregates_and_test_exclusion(self):
        import psycopg
        from datetime import timedelta
        dsn = os.environ['OCM_PV_TEST_DSN']
        root = Path(__file__).resolve().parents[3]
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute((root/'migrations/open_close_pv.sql').read_text())
            cur.execute("CREATE TABLE IF NOT EXISTS stores(id bigint PRIMARY KEY,status text,prefecture text,city text,category text)")
            cur.execute("INSERT INTO stores VALUES(42,'open','北海道','札幌市','カフェ') ON CONFLICT DO NOTHING")
        t.record(dsn, '/', 'google', False)
        t.record(dsn, '/', 'google', False)
        t.record(dsn, '/', 'google', True)
        self.assertTrue(t.record(dsn, '/store/42', 'internal', False))
        self.assertFalse(t.record(dsn, '/store/9999', 'referral', False))
        self.assertFalse(t.record(dsn, '/area/秘密の検索語', 'referral', False))
        r = t.report(dsn, 1, True)
        self.assertEqual(r['views'], 3)
        self.assertTrue(r['tests_excluded'])
        self.assertEqual(sum(x['views'] for x in r['by_source']), 3)
        self.assertEqual(sum(x['views'] for x in r['by_day']), 3)
        self.assertEqual(r['timezone'], 'Asia/Tokyo')
        self.assertEqual(t.report(dsn, 1, False)['views'], 0)
        with patch.object(t, 'japan_day', return_value=t.japan_day()+timedelta(days=1)):
            self.assertEqual(t.report(dsn, 1, False)['views'], 3)

if __name__ == '__main__':
    unittest.main()
