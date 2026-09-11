import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timezone
from fastapi import FastAPI
from fastapi.testclient import TestClient

spec = importlib.util.spec_from_file_location('visitor_analytics', Path(__file__).resolve().parents[1] / 'app/visitor_analytics.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class VisitorAnalyticsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'test.sqlite'
        self.db = lambda: sqlite3.connect(self.path)
        with m.managed_connection(self.db) as c:
            c.execute('CREATE TABLE trends (slug TEXT PRIMARY KEY)')
            c.execute("INSERT INTO trends VALUES ('テスト')")
        self.app = FastAPI()
        m.install_visitor_analytics(self.app, self.db)
        self.client = TestClient(self.app)
        self.client.__enter__()
        self.headers = {'Origin': 'https://buzz-now-1.onrender.com', 'User-Agent': 'Mozilla/5.0 BUZZNOW-Diagnostic'}

    def tearDown(self):
        self.client.__exit__(None, None, None)
        self.temp.cleanup()

    def post(self, **kwargs):
        data = {'path': '/', 'source': 'x', 'test': False, 'nonce': 'document-1234567890'}
        data.update(kwargs)
        return self.client.post('/api/visitor-analytics/pageview', json=data, headers=self.headers)

    def rows(self):
        with m.managed_connection(self.db) as c:
            return c.execute('SELECT day, path, source, is_test, views FROM visitor_pageviews_daily ORDER BY is_test').fetchall()

    def test_status(self):
        self.assertTrue(self.client.get('/api/visitor-analytics/status').json()['ready'])

    def test_record_and_nonce_dedup(self):
        self.assertEqual(self.post().status_code, 204)
        self.assertEqual(self.post().status_code, 204)
        self.assertEqual(self.rows()[0][4], 1)

    def test_test_traffic_separate(self):
        self.post()
        self.post(test=True, nonce='diagnostic-123456789')
        self.assertEqual([(r[3], r[4]) for r in self.rows()], [(0, 1), (1, 1)])

    def test_query_and_external_paths_rejected(self):
        for value in ('/?email=secret', 'https://evil.example/', '/admin', '/trend/foo%3Femail=secret'):
            self.assertEqual(self.post(path=value).status_code, 400)
        self.assertEqual(self.rows(), [])

    def test_encoded_path_normalized(self):
        self.assertEqual(self.post(path='/trend/%E3%83%86%E3%82%B9%E3%83%88').status_code, 204)
        self.assertEqual(self.rows()[0][1], '/trend/テスト')

    def test_unknown_page_rejected(self):
        self.assertEqual(self.post(path='/trend/nonexistent').status_code, 400)
        self.assertEqual(self.rows(), [])

    def test_unknown_fields_rejected(self):
        self.assertEqual(self.post(referrer='https://secret.example/?token=private').status_code, 400)
        self.assertEqual(self.rows(), [])

    def test_origin_rejected(self):
        self.headers['Origin'] = 'https://evil.example'
        self.assertEqual(self.post().status_code, 403)
        self.assertEqual(self.rows(), [])

    def test_bot_excluded(self):
        self.headers['User-Agent'] = 'Googlebot'
        self.assertEqual(self.post().status_code, 204)
        self.assertEqual(self.rows(), [])

    def test_dnt_excluded(self):
        self.headers['DNT'] = '1'
        self.assertEqual(self.post().status_code, 204)
        self.assertEqual(self.rows(), [])

    def test_jst_boundary(self):
        m.record_pageview(self.db, '/', 'google', False, datetime(2026, 9, 10, 15, 0, tzinfo=timezone.utc))
        self.assertEqual(self.rows()[0][0], '2026-09-11')

    def test_only_aggregate_columns(self):
        with m.managed_connection(self.db) as c:
            cols = {r[1] for r in c.execute('PRAGMA table_info(visitor_pageviews_daily)')}
        self.assertEqual(cols, {'day', 'path', 'source', 'is_test', 'views'})

    def test_oversized_payload(self):
        self.assertEqual(self.post(nonce='a' * 5000).status_code, 413)

    def test_source_allowlist(self):
        self.assertEqual(self.post(source='secret@example.com').status_code, 400)

    def test_real_browser_headers(self):
        self.headers['Sec-Fetch-Site'] = 'same-origin'
        self.assertEqual(self.post().status_code, 204)

    def test_cross_site_rejected(self):
        self.headers['Sec-Fetch-Site'] = 'cross-site'
        self.assertEqual(self.post().status_code, 403)


if __name__ == '__main__':
    unittest.main()
