import unittest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from services.open_close.inquiries import install_inquiries

class FakeDB:
    def __init__(self): self.inserted = []; self.count = 0
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def execute(self, sql, args=None):
        if sql.startswith('INSERT'): self.inserted.append(args)
        return self
    def fetchone(self): return (self.count,)

class InquiriesTest(unittest.TestCase):
    def setUp(self):
        self.db = FakeDB()
        app = FastAPI()
        def denied(): raise HTTPException(401)
        install_inquiries(app, lambda:self.db, denied)
        self.client = TestClient(app)
        self.data = dict(consent=True, message='掲載内容の訂正に関する動作確認です。', page_url='https://example.com/store/443')
    def test_persists_and_returns_only_receipt(self):
        response = self.client.post('/api/inquiries', json=self.data)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(set(response.json()), {'ok','receipt'})
        self.assertEqual(self.db.inserted[0][3], self.data['message'])
        self.assertEqual(response.headers['cache-control'], 'no-store')
    def test_private_inbox(self):
        self.assertEqual(self.client.get('/api/admin/inquiries').status_code, 401)
    def test_validation_never_stores(self):
        for change in [dict(consent=False),dict(message='short'),dict(website='bot'),dict(email='bad'),dict(message=23)]:
            self.assertEqual(self.client.post('/api/inquiries',json=self.data|change).status_code,400)
        self.assertEqual(self.db.inserted, [])
    def test_oversized_and_quota(self):
        self.assertEqual(self.client.post('/api/inquiries',content='x'*17000).status_code,413)
        self.db.count=30
        self.assertEqual(self.client.post('/api/inquiries',json=self.data).status_code,429)
        self.assertEqual(self.db.inserted, [])

if __name__ == '__main__': unittest.main()

