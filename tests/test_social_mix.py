# X-only news policy applied: 2026-09-12.
import os
os.environ['DEMO_MODE'] = 'false'
os.environ['REAL_DATA_MODE'] = 'false'
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from app import social_schedule as plan, social_mix as mix


class MixTest(unittest.TestCase):
    def setUp(self):
        self.c = sqlite3.connect(':memory:')
        self.c.row_factory = sqlite3.Row
        self.c.executescript('''CREATE TABLE system_state(key TEXT PRIMARY KEY,value TEXT);
            CREATE TABLE social_posts(posted_at TEXT,make_status INTEGER);
            CREATE TABLE buzzing_quote_posts(sent_at TEXT,status TEXT);
            CREATE TABLE threads_posts(id INTEGER PRIMARY KEY,trend_id INTEGER,buffer_status INTEGER);
        ''')
        plan.init(self.c)
        self.now = datetime(2026,9,11,2,0,tzinfo=timezone.utc)

    def tearDown(self):
        self.c.close()

    def accepted(self, platform, kind, n):
        if platform == 'x':
            self.c.execute("INSERT INTO social_schedule_log VALUES(?,?,'sent',?,'fixture','')", (str(n), kind, (self.now-timedelta(days=2)).isoformat()))
        else:
            self.c.execute('INSERT INTO threads_posts VALUES(?,?,1)', (n, -1 if kind=='character' else n+1))

    def test_x_news_only_and_threads_two_per_ten(self):
        for platform in ('x', 'threads'):
            sequence = []
            for n in range(1, 31):
                kind = mix.next_kind(self.c, platform, self.now)
                sequence.append(kind)
                self.accepted(platform, kind, n)
            expected = [] if platform == 'x' else [5, 10, 15, 20, 25, 30]
            self.assertEqual([i+1 for i, k in enumerate(sequence) if k == 'character'], expected)
            for start in range(21):
                self.assertEqual(sequence[start:start+10].count('character'), 0 if platform == 'x' else 2)

    def test_independent_counters(self):
        for n in range(1,5): self.accepted('x','trend',n)
        self.assertEqual(mix.next_kind(self.c,'x',self.now),'trend')
        self.assertEqual(mix.next_kind(self.c,'threads',self.now),'trend')

    def test_skips_and_failures_do_not_advance(self):
        for n in range(1,5): self.accepted('x','trend',n)
        self.c.execute("INSERT INTO social_schedule_log VALUES('skip','character','skipped',?,'','')", (self.now.isoformat(),))
        self.assertEqual(mix.next_kind(self.c,'x',self.now),'trend')
        self.c.execute('INSERT INTO threads_posts VALUES(1,42,2)')
        self.assertEqual(mix.counts(self.c,'threads')['accepted'],0)

    def test_unknown_submissions_block(self):
        self.c.execute("INSERT INTO social_schedule_log VALUES('unknown','character','reserved',?,'','')", (self.now.isoformat(),))
        self.assertTrue(mix.pending(self.c,'x'))
        self.c.execute('INSERT INTO threads_posts VALUES(1,-1,-3)')
        self.assertTrue(mix.pending(self.c,'threads'))

    def test_old_unconfigured_threads_failure_is_not_new_uncertainty(self):
        self.c.execute('INSERT INTO threads_posts VALUES(1,42,0)')
        self.assertFalse(mix.pending(self.c,'threads'))

    def test_fifth_x_turn_accepts_news(self):
        for n in range(1, 5): self.accepted('x', 'trend', n)
        self.assertTrue(mix.ordinary_news_turn(self.c))
        slot, reason = plan.reserve(self.c, 'trend', self.now, mixed=True)
        self.assertEqual(reason, 'ok')
        self.assertEqual(slot['kind'], 'trend')
        self.assertEqual(plan.reserve(self.c, 'trend', self.now, mixed=True)[1], 'slot_already_attempted')

    def test_character_cannot_take_reassigned_news_turn(self):
        for n in range(1, 5): self.accepted('x', 'trend', n)
        self.assertEqual(plan.reserve(self.c, 'character', self.now, mixed=True)[1], 'x_character_posts_disabled')
        self.assertEqual(plan.reserve(self.c, 'trend', self.now, mixed=True)[1], 'ok')

    def test_threads_trial_is_not_extended_by_x_change(self):
        mix.start_trial(self.c, 'x', self.now-timedelta(days=14))
        self.assertEqual(mix.next_kind(self.c, 'x', self.now), 'trend')
        self.assertEqual(mix.character_content(self.c, 'x', self.now)['reason'], 'x_character_posts_disabled')
        self.assertTrue(mix.character_content(self.c, 'threads', self.now)['ok'])
        mix.start_trial(self.c, 'threads', self.now-timedelta(days=14))
        self.assertEqual(mix.character_content(self.c, 'threads', self.now)['reason'], 'character_trial_finished')

    def test_existing_asset_and_simplified_caption(self):
        for platform in ('threads',):
            content=mix.character_content(self.c,platform,self.now)
            self.assertTrue(content['ok'])
            self.assertIn('SNS捜査官｜BUZZ NOW',content['text'])
            self.assertNotIn('公式AIキャラクター',content['text'])
            self.assertNotIn('#AIキャラクター',content['text'])
            self.assertTrue(content['image_url'].endswith('/approved.jpg'))
            self.assertLessEqual(len(content['text'])*2,280)


class ThreadsIntegrationTest(unittest.TestCase):
    def setUp(self):
        import app.main as main
        self.main=main
        self.temp=tempfile.TemporaryDirectory()
        self.file=Path(self.temp.name)/'threads.sqlite'
        def db():
            c=sqlite3.connect(self.file)
            c.row_factory=sqlite3.Row
            return c
        self.db=db
        self.now=datetime.now(timezone.utc)
        with db() as c:
            c.execute('CREATE TABLE system_state(key TEXT PRIMARY KEY,value TEXT)')
            c.execute('CREATE TABLE social_images(trend_id INTEGER)')
            main._ensure_threads_tables(c)
            for n in range(4):
                c.execute("INSERT INTO threads_posts(trend_id,keyword,post_text,buffer_status,posted_at) VALUES(?,?,?,1,?)", (n+1,'existing','existing',(self.now-timedelta(hours=5+n)).isoformat()))
        self.patches=[patch.object(main,'db',db), patch.object(main,'THREADS_AUTO_ENABLED',True),
            patch.object(main,'BUFFER_API_KEY','test-only-not-a-credential'), patch.object(main,'_buffer_pause',lambda:None),
            patch.object(main,'THREADS_DAILY_CAP',5), patch.object(main,'THREADS_GLOBAL_COOLDOWN_MINUTES',180),
            patch.dict(os.environ,{'RENDER_SERVICE_ID':main.THREADS_SERVICE_ID})]
        for p in self.patches:p.start()

    def tearDown(self):
        for p in reversed(self.patches):p.stop()
        self.temp.cleanup()

    def test_fifth_is_character_and_sent_to_threads_not_x(self):
        with patch.object(self.main,'_send_to_buffer_channel',return_value={'ok':True,'post_id':'fixture'}) as sender, patch.object(self.main,'_social_candidate_rows',side_effect=AssertionError('news must not replace character')):
            result=self.main.auto_post_threads()
        self.assertEqual(result['sent'],1)
        self.assertEqual(result['kind'],'character')
        self.assertEqual(sender.call_args.args[0],self.main.BUFFER_THREADS_CHANNEL_ID)
        self.assertIn('SNS捜査官｜BUZZ NOW',sender.call_args.args[1])
        self.assertNotIn('公式AIキャラクター',sender.call_args.args[1])
        self.assertTrue(sender.call_args.args[2].endswith('/approved.jpg'))
        with self.db() as c:
            self.assertEqual(mix.counts(c,'threads'),{'accepted':5,'character_accepted':1})
            self.assertIsNotNone(c.execute("SELECT value FROM system_state WHERE key='detective_threads_trial_start'").fetchone())

    def test_cap_includes_character(self):
        with patch.object(self.main,'THREADS_DAILY_CAP',4), patch.object(self.main,'_send_to_buffer_channel',side_effect=AssertionError('cap')):
            self.assertEqual(self.main.auto_post_threads()['reason'],'daily_cap')

    def test_cooldown_includes_character(self):
        with self.db() as c:
            c.execute('UPDATE threads_posts SET posted_at=? WHERE id=4',(self.now.isoformat(),))
        with patch.object(self.main,'_send_to_buffer_channel',side_effect=AssertionError('cooldown')):
            self.assertEqual(self.main.auto_post_threads()['reason'],'global_cooldown')

    def test_unknown_does_not_retry_or_advance(self):
        with patch.object(self.main,'_send_to_buffer_channel',return_value={'ok':False,'reason':'timeout'}) as sender:
            self.assertEqual(self.main.auto_post_threads()['sent'],0)
            self.assertEqual(self.main.auto_post_threads()['reason'],'uncertain_submission_requires_review')
            self.assertEqual(sender.call_count,1)
        with self.db() as c:
            self.assertEqual(mix.counts(c,'threads')['accepted'],4)
            self.assertTrue(mix.pending(c,'threads'))

    def test_known_rate_limit_does_not_advance(self):
        with patch.object(self.main,'_send_to_buffer_channel',return_value={'ok':False,'status_code':429}):
            self.assertEqual(self.main.auto_post_threads()['sent'],0)
        with self.db() as c:
            self.assertEqual(mix.next_kind(c,'threads',self.now),'character')
            self.assertFalse(mix.pending(c,'threads'))
            self.assertIsNone(c.execute("SELECT value FROM system_state WHERE key='detective_threads_trial_start'").fetchone())


if __name__=='__main__':unittest.main()
