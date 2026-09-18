import os
import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch
from app import x_editorial_policy as p
from app import city_corporate_social as s
from app.city_corporate_activation import fit_text

TODAY=date(2026,9,18)


def risk():
    return {'id':'rtest','kind':'risk','company':'検証用株式会社','reported_date':TODAY,
            'payload':{'source_url':'https://www.caa.go.jp/notice/test',
                       'source_name':'消費者庁','stage':'景品表示法に基づく措置命令'}}


def report_row(title='検証用社、不正会計の疑いを否定 破産関連の続報'):
    return {'id':'btest','kind':'bankruptcy','company':'検証用社','reported_date':TODAY,
            'payload':{'stage':'破産関連（報道）','news_reports':[
                {'title':title,'url':'https://n-seikei.jp/2026/09/test.html',
                 'publisher':'n-seikei.jp','published_date':'2026-09-18',
                 'match_basis':'会社名・所在地一致'}]}}


class EvidenceTests(unittest.TestCase):
    def test_administrative_action_is_never_labeled_bankrupt_or_guilty(self):
        row=risk(); chosen=p.select_corporate([row],set(),TODAY)
        self.assertEqual(chosen[0],'risk:rtest')
        text=p.corporate_text(chosen[1])
        self.assertIn('措置命令',text)
        self.assertIn('消費者庁',text)
        self.assertNotIn('倒産',text);self.assertNotIn('不正発覚',text)
        self.assertEqual(fit_text(text),text)

    def test_official_hostname_must_match_not_be_embedded(self):
        row=risk();row['payload']['source_url']='https://www.caa.go.jp.evil.example/test'
        self.assertIsNone(p.corporate_evidence(row,TODAY))

    def test_unmatched_company_stale_and_future_reports_are_excluded(self):
        for field,value in [('match_basis','未確認'),('title','別の会社で不正会計'),
                            ('published_date','2026-09-10'),('published_date','2026-09-19'),
                            ('publisher','different.example')]:
            row=report_row();row['payload']['news_reports'][0][field]=value
            with self.subTest(field=field,value=value):
                self.assertIsNone(p.corporate_evidence(row,TODAY))

    def test_denial_and_allegation_are_preserved_in_full(self):
        row=report_row(); chosen=p.select_corporate([row],set(),TODAY)
        text=p.corporate_text(chosen[1])
        self.assertIn('不正会計の疑いを否定',text)
        self.assertIn('企業報道',text)
        self.assertIsNotNone(fit_text(text))

    def test_ordinary_failure_does_not_create_scandal(self):
        row=report_row('検証用社、原材料高で破産申請準備')
        self.assertIsNone(p.select_corporate([row],set(),TODAY))

    def test_past_report_date_and_sent_source_are_excluded(self):
        row=risk();row['reported_date']=date(2026,9,15)
        self.assertIsNone(p.select_corporate([row],set(),TODAY))
        self.assertIsNone(p.select_corporate([risk()],{'risk:rtest'},TODAY))

    def test_long_headline_cannot_lose_trailing_denial(self):
        self.assertIsNone(p.corporate_evidence(report_row('検証用社、不正会計'+('の報道'*25)+'を否定'),TODAY))


class SendingTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'state.db'
        with sqlite3.connect(self.path) as c:c.execute('CREATE TABLE system_state(key TEXT PRIMARY KEY,value TEXT)')
        self.env=patch.dict(os.environ,{'CITY_CORP_X_ENABLED':'true','CITY_CORP_X_BUFFER_CHANNEL_ID':'dedicated','CITY_CORP_X_DAILY_CAP':'6'})
        self.env.start()

    def tearDown(self):
        self.env.stop();self.tmp.cleanup()

    @contextmanager
    def db(self):
        c=sqlite3.connect(self.path);c.row_factory=sqlite3.Row
        try:yield c;c.commit()
        finally:c.close()

    def candidate(self,dsn,seen,today):
        return p.select_corporate([risk()],seen,today)

    def test_incident_uses_existing_close_slot_and_durable_dedup(self):
        sender=Mock(return_value={'ok':True,'post_id':'receipt'})
        # 13:00 JST is normally a closure slot; no extra send slot is added.
        now=datetime(2026,9,18,4,0,tzinfo=timezone.utc)
        with patch.object(s,'_incident_candidate',side_effect=self.candidate),patch.object(s,'_close_candidate',return_value=None) as ordinary:
            self.assertEqual(s.run(self.db,sender,now)['kind'],'incident')
            self.assertEqual(s.run(self.db,sender,now)['reason'],'slot_already_attempted')
            later=datetime(2026,9,18,6,30,tzinfo=timezone.utc)
            self.assertEqual(s.run(self.db,sender,later)['reason'],'no_fresh_candidate')
        sender.assert_called_once()
        self.assertIn('/corporate/company/rtest',sender.call_args.args[1])
        self.assertNotIn('/store/',sender.call_args.args[1])

    def test_uncertain_incident_is_not_retried_and_no_digest_is_created(self):
        sender=Mock(side_effect=TimeoutError())
        now=datetime(2026,9,18,9,15,tzinfo=timezone.utc)
        with patch.object(s,'_incident_candidate',side_effect=self.candidate),patch.object(s.digest,'candidate') as digest,patch.object(s,'_close_candidate',return_value=None):
            self.assertEqual(s.run(self.db,sender,now)['reason'],'submission_uncertain')
            self.assertEqual(s.run(self.db,sender,datetime(2026,9,18,12,15,tzinfo=timezone.utc))['reason'],'no_fresh_candidate')
        digest.assert_not_called();sender.assert_called_once()


if __name__=='__main__':unittest.main()
