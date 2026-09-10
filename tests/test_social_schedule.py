import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from app import social_schedule as p, detective_posts as d

class ScheduleTest(unittest.TestCase):
    def setUp(self):
        self.c=sqlite3.connect(':memory:')
        self.c.row_factory=sqlite3.Row
        self.c.executescript('''CREATE TABLE social_posts(posted_at TEXT,make_status INTEGER);
        CREATE TABLE buzzing_quote_posts(sent_at TEXT,status TEXT);
        CREATE TABLE system_state(key TEXT PRIMARY KEY,value TEXT);''')
        p.init(self.c)
        self.now=datetime(2026,9,11,3,30,tzinfo=timezone.utc) # 12:30 JST
    def test_jst_windows_and_no_catchup(self):
        self.assertIsNotNone(p.current_slot(self.now,'character'))
        self.assertIsNone(p.current_slot(self.now,'trend'))
        self.assertIsNone(p.current_slot(self.now+timedelta(minutes=30),'character'))
        self.assertIsNone(p.current_slot(self.now-timedelta(minutes=1),'character'))
        for h,m,k in p.SLOTS:
            t=datetime(2026,9,11,h,m,tzinfo=p.JST)
            self.assertEqual(p.current_slot(t,k)['kind'],k)
    def test_single_claim_shared_by_dispatchers(self):
        slot,reason=p.reserve(self.c,'character',self.now)
        self.assertEqual(reason,'ok')
        self.assertEqual(p.reserve(self.c,'character',self.now)[1],'slot_already_attempted')
        p.finish(self.c,slot,True,'test')
        self.assertEqual(p.reserve(self.c,'character',self.now)[1],'slot_already_attempted')
    def test_historical_and_character_combined_cap(self):
        for _ in range(8):self.c.execute('INSERT INTO social_posts VALUES(?,1)',((self.now-timedelta(hours=2)).isoformat(),))
        self.c.execute('INSERT INTO buzzing_quote_posts VALUES(?,?)',((self.now-timedelta(hours=3)).isoformat(),'sent'))
        self.c.execute("INSERT INTO social_schedule_log VALUES('previous','character','sent',?,'','')",((self.now-timedelta(hours=4)).isoformat(),))
        self.assertEqual(p.reserve(self.c,'character',self.now)[1],'combined_daily_cap')
    def test_quote_cooldown_applies_to_character(self):
        self.c.execute('INSERT INTO buzzing_quote_posts VALUES(?,?)',((self.now-timedelta(minutes=40)).isoformat(),'sent'))
        self.assertEqual(p.reserve(self.c,'character',self.now)[1],'combined_cooldown')
    def test_trial_texts_unique_disclosed_and_short(self):
        texts=[]
        for day in range(14):
            for hour in (12,21):
                t=p.character_text(day,{'start':self.now.replace(hour=hour)})
                self.assertIn('AIキャラクター',t)
                self.assertLessEqual(len(t)*2,280) # conservative double weight
                texts.append(t)
        self.assertEqual(len(set(texts)),28)
    def test_buffer_pause_never_sends(self):
        def sender(*args):self.fail('must not send')
        result=d.run(lambda:self.c,sender,lambda:{'reason':'buffer_rate_limited'},True,True,lambda c:None,10,60)
        self.assertEqual(result['reason'],'buffer_rate_limited')
    def test_character_send_and_trial_start(self):
        class Clock:
            @staticmethod
            def now(tz):return self.now
        sent=[]
        def send(text,url,mode):sent.append((text,url));return {'ok':True,'post_id':'fixture'}
        with patch.object(d,'datetime',Clock):
            result=d.run(lambda:self.c,send,lambda:None,True,True,lambda c:None,10,60)
            again=d.run(lambda:self.c,send,lambda:None,True,True,lambda c:None,10,60)
        self.assertEqual(result['sent'],1)
        self.assertEqual(again['sent'],0)
        self.assertEqual(len(sent),1)
        self.assertTrue(sent[0][1].endswith('/noon.jpg'))
        self.assertEqual(self.c.execute('SELECT value FROM system_state').fetchone()['value'],'2026-09-11')
    def test_trial_stops_after_fourteen_days(self):
        self.c.execute("INSERT INTO system_state VALUES('detective_trial_start','2026-08-01')")
        class Clock:
            @staticmethod
            def now(tz):return self.now
        with patch.object(d,'datetime',Clock):
            result=d.run(lambda:self.c,lambda *a:self.fail('expired'),lambda:None,True,True,lambda c:None,10,60)
        self.assertEqual(result['reason'],'character_trial_finished')

if __name__=='__main__': unittest.main()
