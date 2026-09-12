import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from app import social_schedule as p, detective_posts as d


class ScheduleTest(unittest.TestCase):
    def setUp(self):
        self.c=sqlite3.connect(':memory:')
        self.c.row_factory=sqlite3.Row
        self.c.executescript('''CREATE TABLE social_posts(posted_at TEXT,make_status INTEGER);
        CREATE TABLE buzzing_quote_posts(sent_at TEXT,status TEXT);
        CREATE TABLE system_state(key TEXT PRIMARY KEY,value TEXT);''')
        p.init(self.c)
        self.now=datetime(2026,9,12,3,30,tzinfo=timezone.utc)

    def tearDown(self): self.c.close()

    def test_all_ten_jst_windows_are_available_for_news_without_catchup(self):
        self.assertEqual([(h,m) for h,m,k in p.SLOTS], [(7,0),(9,0),(11,0),(12,30),(14,0),(16,0),(18,0),(19,30),(21,0),(22,30)])
        self.assertEqual(len(p.SLOTS),10)
        for h,m,k in p.SLOTS:
            t=datetime(2026,9,12,h,m,tzinfo=p.JST)
            self.assertEqual(k,'trend')
            self.assertIsNotNone(p.current_slot(t,'trend'))
            self.assertIsNone(p.current_slot(t,'character'))
            self.assertIsNone(p.current_slot(t+timedelta(minutes=30),'trend'))
        self.assertEqual(p.status()['trend_slots'],10)
        self.assertEqual(p.status()['character_slots'],0)

    def test_single_claim_shared_by_dispatchers(self):
        slot,reason=p.reserve(self.c,'trend',self.now)
        self.assertEqual(reason,'ok')
        self.assertEqual(p.reserve(self.c,'trend',self.now)[1],'slot_already_attempted')
        p.finish(self.c,slot,True,'test')
        self.assertEqual(p.reserve(self.c,'trend',self.now)[1],'slot_already_attempted')

    def test_historical_character_posts_still_count_toward_daily_cap(self):
        for _ in range(8):self.c.execute('INSERT INTO social_posts VALUES(?,1)',((self.now-timedelta(hours=2)).isoformat(),))
        self.c.execute('INSERT INTO buzzing_quote_posts VALUES(?,?)',((self.now-timedelta(hours=3)).isoformat(),'sent'))
        self.c.execute("INSERT INTO social_schedule_log VALUES('previous','character','sent',?,'','')",((self.now-timedelta(hours=4)).isoformat(),))
        self.assertEqual(p.reserve(self.c,'trend',self.now)[1],'combined_daily_cap')

    def test_quote_cooldown_applies_to_reassigned_slot(self):
        self.c.execute('INSERT INTO buzzing_quote_posts VALUES(?,?)',((self.now-timedelta(minutes=40)).isoformat(),'sent'))
        self.assertEqual(p.reserve(self.c,'trend',self.now)[1],'combined_cooldown')

    def test_threads_trial_captions_remain_unique_and_short(self):
        texts=[]
        for day in range(14):
            for hour in (12,21):
                t=p.character_text(day,{'start':self.now.replace(hour=hour)})
                self.assertIn('SNS捜査官｜BUZZ NOW',t)
                self.assertNotIn('公式AIキャラクター',t)
                self.assertNotIn('#AIキャラクター',t)
                self.assertLessEqual(len(t)*2,280)
                texts.append(t)
        self.assertEqual(len(set(texts)),28)

    def test_direct_x_character_sender_never_calls_database_or_buffer(self):
        def forbidden(*args):self.fail('disabled X sender must not perform I/O')
        for mixed in (False,True):
            result=d.run(forbidden,forbidden,forbidden,True,True,forbidden,10,60,mixed=mixed)
            self.assertEqual(result,{'sent':0,'reason':'x_character_posts_disabled'})

    def test_x_character_reservation_is_blocked_in_all_modes(self):
        for mixed in (False,True):
            self.assertEqual(p.reserve(self.c,'character',self.now,mixed=mixed)[1],'x_character_posts_disabled')
        self.assertEqual(self.c.execute('SELECT COUNT(*) FROM social_schedule_log').fetchone()[0],0)

    def test_no_change_to_window_or_minimum_cooldown(self):
        self.assertEqual(p.WINDOW_MINUTES,30)
        self.c.execute("INSERT INTO social_schedule_log VALUES('last','character','sent',?,'','')",((self.now-timedelta(minutes=59)).isoformat(),))
        self.assertEqual(p.reserve(self.c,'trend',self.now,cooldown_minutes=0)[1],'combined_cooldown')


if __name__=='__main__': unittest.main()
