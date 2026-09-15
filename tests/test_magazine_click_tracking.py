import importlib.util
import sqlite3
import tempfile
import unittest
from pathlib import Path
from fastapi import FastAPI
from fastapi.testclient import TestClient

spec = importlib.util.spec_from_file_location('visitor_analytics_clicks', Path(__file__).resolve().parents[1] / 'app/visitor_analytics.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class MagazineClickTrackingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'test.sqlite'
        self.db = lambda: sqlite3.connect(self.path)
        with m.managed_connection(self.db) as c:
            c.execute('CREATE TABLE trends (slug TEXT PRIMARY KEY)')
            c.executemany('INSERT INTO trends VALUES (?)', [('source',), ('target',)])
        self.app = FastAPI()
        m.install_visitor_analytics(self.app, self.db)
        self.client = TestClient(self.app)
        self.client.__enter__()
        self.headers = {
            'Origin': 'https://buzz-now-1.onrender.com',
            'User-Agent': 'Mozilla/5.0 BUZZNOW-ClickTest',
            'Sec-Fetch-Site': 'same-origin',
        }

    def tearDown(self):
        self.client.__exit__(None, None, None)
        self.temp.cleanup()

    def post(self, **kwargs):
        payload = {
            'source_path': '/trend/source',
            'target_path': '/trend/target',
            'placement': 'magazine_stream',
            'test': False,
            'nonce': 'click-document-1234567890',
        }
        payload.update(kwargs)
        return self.client.post('/api/visitor-analytics/internal-click', json=payload, headers=self.headers)

    def rows(self):
        with m.managed_connection(self.db) as c:
            return c.execute('SELECT day,source_path,target_path,placement,is_test,clicks FROM visitor_internal_clicks_daily ORDER BY is_test').fetchall()

    def test_records_aggregate_click(self):
        self.assertEqual(self.post().status_code, 204)
        row = self.rows()[0]
        self.assertEqual(row[1:], ('/trend/source', '/trend/target', 'magazine_stream', 0, 1))

    def test_nonce_dedup_is_memory_only(self):
        self.assertEqual(self.post().status_code, 204)
        self.assertEqual(self.post().status_code, 204)
        self.assertEqual(self.rows()[0][5], 1)

    def test_separate_test_traffic(self):
        self.post()
        self.post(test=True, nonce='click-test-1234567890123')
        self.assertEqual([(r[4], r[5]) for r in self.rows()], [(0, 1), (1, 1)])

    def test_home_target_allowed_for_more_link(self):
        self.assertEqual(self.post(target_path='/', placement='magazine_more').status_code, 204)
        self.assertEqual(self.rows()[0][2:4], ('/', 'magazine_more'))

    def test_invalid_or_unknown_values_rejected(self):
        self.assertEqual(self.post(placement='secret').status_code, 400)
        self.assertEqual(self.post(target_path='/trend/missing', nonce='click-missing-123456789').status_code, 400)
        self.assertEqual(self.post(source_path='/?token=secret', nonce='click-query-12345678901').status_code, 400)
        self.assertEqual(self.rows(), [])

    def test_cross_site_rejected(self):
        self.headers['Origin'] = 'https://evil.example'
        self.assertEqual(self.post().status_code, 403)
        self.assertEqual(self.rows(), [])

    def test_aggregate_schema_contains_no_identifier_columns(self):
        with m.managed_connection(self.db) as c:
            cols = {r[1] for r in c.execute('PRAGMA table_info(visitor_internal_clicks_daily)')}
        self.assertEqual(cols, {'day', 'source_path', 'target_path', 'placement', 'is_test', 'clicks'})


if __name__ == '__main__':
    unittest.main()
