import json, sqlite3, unittest
from unittest.mock import patch
from app import editorial as e

class EditorialTests(unittest.TestCase):
 def test_private_urls_rejected(self):
  for url in ['file:///etc/passwd','http://localhost:9000','https://u:p@example.com','http://127.0.0.1/']:
   with self.assertRaises(ValueError): e.public_url(url)
 def test_evidence_must_match_two_bodies(self):
  articles=[{'body':'これは最初の記事本文で確認された情報です。'}, {'body':'こちらは二つ目の記事本文の情報です。'}]
  r={'summary':'要約。'*40,'viewpoint':'独自の解釈。'*15,'uncertainty':'確認できない点があります。','evidence':[{'source_id':1,'quote':'最初の記事本文で確認された情報'},{'source_id':2,'quote':'二つ目の記事本文の情報'}]}
  e.validate(r,articles)
  r['evidence'][1]['quote']='本文にない架空の情報です'
  with self.assertRaises(ValueError): e.validate(r,articles)
 def test_failed_fetch_does_not_call_ai(self):
  connection=sqlite3.connect(':memory:');connection.row_factory=sqlite3.Row
  connection.executescript('CREATE TABLE trends(id INTEGER,keyword TEXT,pre_buzz_score REAL,updated_at TEXT);CREATE TABLE sources(trend_id INTEGER,title TEXT,url TEXT,publisher TEXT,published_at TEXT,id INTEGER);INSERT INTO trends VALUES(1,"テスト",90,"2026-09-10");INSERT INTO sources VALUES(1,"記事","https://example.com","例","2026-09-10",1);INSERT INTO sources VALUES(1,"記事2","https://example.com/2","例","2026-09-10",2);')
  with patch.dict('os.environ',{'OPENAI_API_KEY':'not-a-key'}),patch.object(e,'article_body',side_effect=ValueError('blocked')),patch.object(e,'generate') as gen:
   e.run(lambda:connection)
   gen.assert_not_called()
   self.assertEqual(connection.execute('SELECT state FROM editorial_briefs').fetchone()[0],'failed')
 def test_daily_budget_prevents_work(self):
  c=sqlite3.connect(':memory:');c.row_factory=sqlite3.Row;e.init(c)
  for i in range(24):c.execute('INSERT INTO editorial_attempts VALUES(?,?)',(str(i),e.utcnow().isoformat()))
  c.commit()
  with patch.dict('os.environ',{'OPENAI_API_KEY':'not-a-key'}): e.run(lambda:c)
 def test_paywall_not_summarized(self):
  with patch.object(e,'allowed',return_value=True),patch.object(e,'download',return_value=('"isAccessibleForFree":false','https://example.com')):
   with self.assertRaisesRegex(ValueError,'paid_article'):e.article_body(None,{'url':'https://example.com'},'テスト',{})



from unittest.mock import MagicMock
class ReviewTests(unittest.TestCase):
 def test_review_rejection_stops_publication(self):
  articles=[{'body':'これは最初の記事本文で確認された情報です。'},{'body':'こちらは二つ目の記事本文の情報です。'}]
  draft={'summary':'要約。'*40,'viewpoint':'独自の解釈。'*15,'uncertainty':'確認できない点があります。','evidence':[{'source_id':1,'quote':'最初の記事本文で確認された情報'},{'source_id':2,'quote':'二つ目の記事本文の情報'}]}
  def response(data):
   r=MagicMock();r.json.return_value={'status':'completed','output':[{'content':[{'type':'output_text','text':json.dumps(data)}]}]};return r
  with patch.dict('os.environ',{'OPENAI_API_KEY':'fake'}),patch.object(e.httpx,'Client') as client:
   client.return_value.__enter__.return_value.post.side_effect=[response(draft),response({'approved':False,'issues':'日時が本文と不一致'})]
   with self.assertRaisesRegex(ValueError,'review_rejected'):e.generate('テスト',articles)
if __name__=='__main__':unittest.main()
