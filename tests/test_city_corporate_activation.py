import os
import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch
from app import city_corporate_activation as a
from app import city_corporate_social as s


def valid_channel():
    return {'ok':True,'data':{'channel':{'id':a.EXPECTED_CHANNEL,'name':a.EXPECTED_HANDLE,'service':'twitter','isDisconnected':False,'isLocked':False,'isQueuePaused':False}}}


class ActivationTests(unittest.TestCase):
    def setUp(self):
        a._cache.clear()
        a._last_run.clear()

    def test_disabled_performs_no_calls(self):
        graphql=Mock();sender=Mock()
        with patch.dict(os.environ,{'CITY_CORP_X_ENABLED':'false'}):
            self.assertEqual(a.run(None,sender,graphql)['sent'],0)
        graphql.assert_not_called();sender.assert_not_called()

    def test_old_channel_cannot_be_used(self):
        graphql=Mock()
        r=a.channel_check(graphql,'6a9a680a065799be4686e3d9')
        self.assertFalse(r['ok']);graphql.assert_not_called()

    def test_exact_channel_identity_and_cache(self):
        graphql=Mock(return_value=valid_channel())
        self.assertTrue(a.channel_check(graphql,a.EXPECTED_CHANNEL)['ok'])
        self.assertTrue(a.channel_check(graphql,a.EXPECTED_CHANNEL)['ok'])
        graphql.assert_called_once()

    def test_wrong_handle_service_and_flags_block(self):
        for key,value in [('id','other'),('name','buzz_now_of'),('service','threads'),('isDisconnected',True),('isLocked',True),('isQueuePaused',True),('isLocked',None)]:
            with self.subTest(key=key):
                a._cache.clear();r=valid_channel();r['data']['channel'][key]=value
                self.assertFalse(a.channel_check(Mock(return_value=r),a.EXPECTED_CHANNEL)['ok'])

    def test_lookup_failure_does_not_leak_secrets(self):
        r=a.channel_check(Mock(side_effect=ValueError('secret-key')),a.EXPECTED_CHANNEL)
        self.assertFalse(r['ok']);self.assertNotIn('secret',str(r))

    def test_weighted_limit_and_links(self):
        self.assertEqual(a.weighted_length('A'*280),280)
        self.assertEqual(a.weighted_length('あ'*140),280)
        self.assertEqual(a.weighted_length('https://open-close-map.onrender.com/store/42'),23)
        self.assertEqual(a.weighted_length('https://buzz-now-1.onrender.com/corporate/company/abc-123'),23)
        self.assertIsNone(a.fit_text('あ'*141))

    def test_optional_hashtags_only_removed(self):
        body='あ'*128+'\nhttps://open-close-map.onrender.com/store/42'
        self.assertEqual(a.fit_text(body+'\n#閉店'),body)
        self.assertEqual(a.fit_text(body),body)
        self.assertIsNone(a.fit_text('正式名称'*100+'\n#閉店'))

    def test_status_is_safe_and_has_six_slots(self):
        with patch.dict(os.environ,{'CITY_CORP_X_ENABLED':'true','CITY_CORP_X_BUFFER_CHANNEL_ID':a.EXPECTED_CHANNEL,'OPEN_CLOSE_DATABASE_URL':'postgres://secret'}):
            r=a.status()
        self.assertEqual(r['handle'],'machi_to_kigyo')
        self.assertEqual(len(r['slots']),6)
        self.assertNotIn('postgres://secret',str(r))


class DurableStateTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=Path(self.tmp.name)/'test.db'
        with sqlite3.connect(self.path) as c:
            c.execute('CREATE TABLE system_state(key TEXT PRIMARY KEY,value TEXT)')
        self.env=patch.dict(os.environ,{'CITY_CORP_X_ENABLED':'true','CITY_CORP_X_BUFFER_CHANNEL_ID':a.EXPECTED_CHANNEL,'CITY_CORP_X_DAILY_CAP':'6'})
        self.env.start()
        self.row={'id':42,'name':'検証用商店','status':'closing','prefecture':'北海道','city':'札幌市','close_date':date(2026,9,30)}
        self.now=datetime(2026,9,11,23,15,tzinfo=timezone.utc)

    def tearDown(self):
        self.env.stop();self.tmp.cleanup()

    @contextmanager
    def db(self):
        c=sqlite3.connect(self.path);c.row_factory=sqlite3.Row
        try:
            yield c;c.commit()
        finally:c.close()

    def candidate(self,dsn,seen):
        return None if 'close:42' in seen else ('close:42',self.row)

    def test_no_second_send_for_same_slot(self):
        sender=Mock(return_value={'ok':True,'post_id':'buffer-test'})
        with patch.object(s,'_close_candidate',side_effect=self.candidate):
            self.assertEqual(s.run(self.db,sender,self.now)['sent'],1)
            self.assertEqual(s.run(self.db,sender,self.now)['sent'],0)
        sender.assert_called_once()

    def test_uncertain_post_is_not_retried_in_later_slot(self):
        sender=Mock(side_effect=TimeoutError('secret'))
        with patch.object(s,'_close_candidate',side_effect=self.candidate):
            self.assertEqual(s.run(self.db,sender,self.now)['sent'],0)
            later=datetime(2026,9,12,4,0,tzinfo=timezone.utc)
            self.assertEqual(s.run(self.db,sender,later)['reason'],'no_fresh_candidate')
        sender.assert_called_once()

    def test_configured_daily_cap_applied(self):
        with self.db() as c:
            s._state_set(c,'slot:2026-09-12T07:00','sent')
        with patch.dict(os.environ,{'CITY_CORP_X_DAILY_CAP':'1'}):
            self.assertEqual(s.run(self.db,Mock(),self.now)['reason'],'daily_cap_reached')

if __name__=='__main__':unittest.main()
