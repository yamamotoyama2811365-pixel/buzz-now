"""No real social senders, live databases or network calls are used here."""
import os
os.environ['DEMO_MODE']='false'
os.environ['REAL_DATA_MODE']='false'
import sqlite3
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import patch
from app import social_mix as mix, social_schedule as plan, detective_posts


class XNewsOnlyTest(unittest.TestCase):
    def setUp(self):
        import app.main as main
        self.main=main
        self.temp=tempfile.TemporaryDirectory()
        path=Path(self.temp.name)/'test.sqlite'
        def db():
            c=sqlite3.connect(path); c.row_factory=sqlite3.Row; return c
        self.db=db
        with db() as c:
            c.executescript('''CREATE TABLE system_state(key TEXT PRIMARY KEY,value TEXT);
                CREATE TABLE social_posts(posted_at TEXT,make_status INTEGER);
                CREATE TABLE buzzing_quote_posts(sent_at TEXT,status TEXT);
                CREATE TABLE threads_posts(id INTEGER PRIMARY KEY,trend_id INTEGER,buffer_status INTEGER);''')
            plan.init(c)
            for n in range(4):
                c.execute("INSERT INTO social_schedule_log VALUES(?,'trend','sent','2026-09-01T00:00:00+00:00','fixture','')",(str(n),))

    def tearDown(self): self.temp.cleanup()

    def test_current_status_has_different_platform_policies(self):
        status=mix.status(self.db)
        self.assertEqual(status['policy_revision'],'x-news-only-20260912')
        self.assertEqual(status['platforms']['x']['normal_per_10'],10)
        self.assertEqual(status['platforms']['x']['character_per_10'],0)
        self.assertEqual(status['platforms']['x']['character_positions'],[])
        self.assertEqual(status['platforms']['threads']['normal_per_10'],8)
        self.assertEqual(status['platforms']['threads']['character_positions'],[5,10])
        self.assertEqual(status['platforms']['x']['accepted'],4)
        self.assertEqual(status['platforms']['x']['next_kind'],'trend')
        self.assertEqual(detective_posts.status(self.db)['character_state'],'disabled_on_x')

    def test_scheduled_reclaimed_slots_call_normal_news_not_character_sender(self):
        for hour,minute in ((12,30),(21,0)):
            now=datetime(2026,9,12,hour,minute,tzinfo=plan.JST)
            class Clock:
                @staticmethod
                def now(tz):return now.astimezone(tz)
            with patch.object(self.main,'db',self.db), patch.object(self.main,'LEGACY_SERVICE',False), patch.object(self.main,'datetime',Clock), patch.object(self.main,'auto_post_social',return_value={'sent':1,'fixture':True}) as normal, patch.object(detective_posts,'run',side_effect=AssertionError('no character send')):
                self.assertEqual(self.main.run_scheduled_social()['sent'],1)
                normal.assert_called_once()

    def test_quote_cannot_consume_reclaimed_fifth_turn(self):
        with patch.object(self.main,'db',self.db), patch.object(self.main,'LEGACY_SERVICE',False), patch.object(self.main,'YAHOO_QUOTE_AUTO_ENABLED',True), patch.object(self.main,'_buffer_pause',return_value=None), patch.object(self.main,'_ensure_quote_post_log',return_value=None), patch.object(self.main,'_auto_quote_yahoo_buzzing_now_unscheduled',side_effect=AssertionError('reclaimed turn is normal news')):
            self.assertEqual(self.main.auto_quote_yahoo_buzzing_now()['reason'],'reserved_for_ordinary_news')
        with self.db() as c:
            self.assertEqual(mix.counts(c,'x')['accepted'],4)
            self.assertFalse(mix.pending(c,'x'))

    def test_no_candidate_does_not_use_character_as_fallback(self):
        now=datetime(2026,9,12,21,0,tzinfo=plan.JST)
        class Clock:
            @staticmethod
            def now(tz):return now.astimezone(tz)
        with patch.object(self.main,'db',self.db), patch.object(self.main,'LEGACY_SERVICE',False), patch.object(self.main,'datetime',Clock), patch.object(self.main,'auto_post_social',return_value={'sent':0,'reason':'no_eligible_candidate'}), patch.object(detective_posts,'run',side_effect=AssertionError('no fallback character')):
            self.assertEqual(self.main.run_scheduled_social()['reason'],'no_eligible_candidate')

    def test_threads_content_still_exists_but_x_is_blocked(self):
        with self.db() as c:
            now=datetime.now(timezone.utc)
            self.assertEqual(mix.character_content(c,'x',now)['reason'],'x_character_posts_disabled')
            result=mix.character_content(c,'threads',now)
            self.assertTrue(result['ok'])
            self.assertIn('/static/detective/v20260911/approved.jpg',result['image_url'])


if __name__=='__main__':unittest.main()
