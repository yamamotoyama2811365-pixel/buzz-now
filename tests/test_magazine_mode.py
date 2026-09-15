import os
os.environ['DEMO_MODE']='false'
os.environ['REAL_DATA_MODE']='false'
import sqlite3
import unittest
from datetime import datetime, timezone
from pathlib import Path
from app import magazine

ROOT=Path(__file__).resolve().parents[1]


class MagazineDataTest(unittest.TestCase):
    def setUp(self):
        self.c=sqlite3.connect(':memory:')
        self.c.row_factory=sqlite3.Row
        self.c.executescript('''
          CREATE TABLE buzzing_quote_posts(id INTEGER PRIMARY KEY,keyword TEXT,tweet_id TEXT,tweet_url TEXT,status TEXT,sent_at TEXT);
          CREATE TABLE trends(id INTEGER PRIMARY KEY,keyword TEXT,slug TEXT,status TEXT,pre_buzz_score REAL,buzz_score REAL,acceleration REAL,category TEXT,updated_at TEXT,is_indexable INTEGER);
          CREATE TABLE visitor_pageviews_daily(day TEXT,path TEXT,source TEXT,is_test INTEGER,views INTEGER);
        ''')
        now=datetime.now(timezone.utc).isoformat()
        self.c.execute("INSERT INTO buzzing_quote_posts VALUES(1,'テスト','123456789','https://x.com/i/web/status/123456789','sent',?)",(now,))
        self.c.execute("INSERT INTO buzzing_quote_posts VALUES(2,'別話題','987654321','https://x.com/example/status/987654321','sent',?)",(now,))
        self.c.execute("INSERT INTO buzzing_quote_posts VALUES(3,'危険','1','https://evil.example/status/1','sent',?)",(now,))
        for i in range(1,5):
            self.c.execute("INSERT INTO trends VALUES(?,?,?,?,?,?,?,?,?,1)",(i,f'話題{i}',f'slug{i}','上昇中',70+i,80+i,i/10,'総合',now))
        day=datetime.now(magazine.JST).date().isoformat()
        self.c.execute("INSERT INTO visitor_pageviews_daily VALUES(?, '/trend/slug2','x',0,8)",(day,))
        self.c.execute("INSERT INTO visitor_pageviews_daily VALUES(?, '/trend/slug3','direct',0,4)",(day,))
        self.c.execute("INSERT INTO visitor_pageviews_daily VALUES(?, '/trend/slug4','google',1,99)",(day,))

    def tearDown(self): self.c.close()

    def test_viral_posts_are_public_x_links_only(self):
        rows=magazine.viral_posts(self.c,'テスト',4)
        self.assertEqual([r['tweet_id'] for r in rows],['123456789','987654321'])
        self.assertTrue(rows[0]['is_related'])
        self.assertFalse(rows[1]['is_related'])

    def test_internal_rows_prefer_real_pv_and_exclude_test(self):
        rows=magazine.internal_buzz_rows(self.c,1,3)
        self.assertEqual(rows[0]['slug'],'slug2')
        self.assertEqual(rows[0]['views'],8)
        self.assertEqual(rows[1]['slug'],'slug3')
        self.assertNotEqual(rows[2]['slug'],'slug1')

    def test_points_use_grounded_inputs(self):
        trend={'keyword':'テスト','status':'急上昇','why_now':'公開データで上昇を確認。追加情報を確認中。'}
        points=magazine.three_points(trend,[{'title':'テストに関する新しい発表'}],{'summary':'複数の公開情報で新しい発表が確認された。詳細は継続確認中。'})
        self.assertEqual(len(points),3)
        self.assertIn('新しい発表',points[0])
        self.assertTrue(any('関連報道' in p for p in points))


class IntegrationSourceTest(unittest.TestCase):
    def test_no_x_content_is_copied_into_template(self):
        html=(ROOT/'templates/_magazine_stream.html').read_text()
        self.assertIn('platform.twitter.com/embed/Tweet.html?id={{ post.tweet_id }}',html)
        self.assertIn('{{ post.tweet_url }}',html)
        self.assertNotIn('platform.twitter.com/widgets.js',html)
        self.assertNotIn('post_text',html)
        self.assertNotIn('image_url',html)

    def test_install_targets_exist(self):
        main=(ROOT/'app/main.py').read_text()
        trend=(ROOT/'templates/trend.html').read_text()
        self.assertIn('def _build_social_post_text',main)
        self.assertIn('prebuzz_rows = _trend_prebuzz_rows',main)
        self.assertIn('確認できた関連ニュース</h1>',trend)
        self.assertIn('a8-advertisement',trend)


if __name__=='__main__': unittest.main()