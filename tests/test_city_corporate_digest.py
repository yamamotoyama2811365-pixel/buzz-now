import json
import sqlite3
import unittest
from contextlib import contextmanager
from datetime import date, datetime, timezone
from unittest.mock import Mock, patch
from app import city_corporate_digest as d, city_corporate_social as s
from app.city_corporate_activation import weighted_length


def rows():
    return [{'id':str(i),'company':'検証'+str(i)+'社','prefecture':'北海道','reported_date':'2026-09-14',
             'payload':{'stage':'破産手続開始','source_url':'https://example.com/'+str(i)}} for i in range(5)]


class DigestTests(unittest.TestCase):
    def test_selection_excludes_stale_future_missing_source_and_seen(self):
        data=rows()
        data[0]['reported_date']='2026-09-01';data[1]['reported_date']='2026-09-15'
        self.assertIsNotNone(d.select(data,set(),date(2026,9,14)))
        self.assertEqual(len(d.select(data,{'bankruptcy:2'},date(2026,9,14))['items']),2)
        data[2]['payload']['source_url']=''
        self.assertEqual(len(d.select(data,set(),date(2026,9,14))['items']),2)

    def test_dedup_and_same_region(self):
        data=rows()[:2]
        self.assertEqual(len(d.select(data+data,set(),date(2026,9,14))['items']),2)
        data=rows()[:3];data[2]['prefecture']='東京都'
        self.assertEqual(d.select(data,set(),date(2026,9,14))['prefecture'],'東日本')

    def test_names_stages_preserved_with_weighted_limit(self):
        doc=d.select(rows(),set(),date(2026,9,14));text=d.post_text(doc)
        self.assertLessEqual(weighted_length(text),280)
        self.assertGreaterEqual(len(doc['items']),3)
        self.assertIn('utm_content='+doc['id'],text)
        for row in doc['items']:
            self.assertIn(row['company'],text)
        data=rows()
        for r in data:r['company']='長い正式社名'*40
        self.assertEqual(d.select(data,set(),date(2026,9,14))['style'],'summary')

    def test_block_and_national_fallback(self):
        data=rows()[:3]
        for r,pref in zip(data,['東京都','神奈川県','千葉県']):r['prefecture']=pref
        self.assertEqual(d.select(data,set(),date(2026,9,14))['prefecture'],'関東')
        for r,pref in zip(data,['北海道','大阪府','沖縄県']):r['prefecture']=pref
        self.assertEqual(d.select(data,set(),date(2026,9,14))['prefecture'],'全国')
        self.assertEqual(len(d.select(data[:1],set(),date(2026,9,14))['items']),1)
        self.assertIsNone(d.select([],set(),date(2026,9,14)))
        self.assertEqual(len(d.PREF_BLOCK),47)

    def test_page_escapes_snapshot_and_missing_returns_404(self):
        from fastapi import FastAPI
        from fastapi.responses import HTMLResponse
        from fastapi.testclient import TestClient
        doc=d.select(rows(),set(),date(2026,9,14))
        doc['items'][0]['company']='<script>bad</script>'
        connection=sqlite3.connect(':memory:',check_same_thread=False);connection.row_factory=sqlite3.Row
        connection.execute('CREATE TABLE system_state(key TEXT PRIMARY KEY,value TEXT)')
        connection.execute('INSERT INTO system_state VALUES(?,?)',(d.KEY+doc['id'],json.dumps(doc)))
        @contextmanager
        def db():yield connection
        app=FastAPI();d.install(app,db,lambda title,body,path,**kw:HTMLResponse(body))
        client=TestClient(app);r=client.get('/roundup/'+doc['id'])
        self.assertEqual(r.status_code,200);self.assertIn('&lt;script&gt;',r.text)
        self.assertEqual(client.get('/roundup/'+'0'*20).status_code,404)
        self.assertIn('https://example.com/',r.text)
        connection.close()

    def test_evening_only_snapshot_before_send_and_no_retry(self):
        connection=sqlite3.connect(':memory:');connection.row_factory=sqlite3.Row
        connection.execute('CREATE TABLE system_state(key TEXT PRIMARY KEY,value TEXT)')
        @contextmanager
        def db():yield connection
        doc=d.select(rows(),set(),date(2026,9,14))
        def send(*args):
            self.assertIsNotNone(connection.execute('SELECT value FROM system_state WHERE key=?',(d.KEY+doc['id'],)).fetchone())
            raise TimeoutError()
        sender=Mock(side_effect=send)
        env={'CITY_CORP_X_ENABLED':'true','CITY_CORP_X_BUFFER_CHANNEL_ID':'dedicated'}
        with patch.dict('os.environ',env),patch.object(d,'candidate',return_value=doc):
            now=datetime(2026,9,14,9,15,tzinfo=timezone.utc)
            self.assertEqual(s.run(db,sender,now)['reason'],'submission_uncertain')
            self.assertEqual(s.run(db,sender,now)['reason'],'slot_already_attempted')
        for row in doc['items']:
            self.assertIsNotNone(s._state_get(connection,'sent:bankruptcy:'+row['id']))
        sender.assert_called_once();connection.close()

if __name__=='__main__':unittest.main()
