import os
import unittest
from datetime import date, datetime, timezone
from unittest.mock import patch
from app import city_corporate_social as s

class CityCorporateSocialTests(unittest.TestCase):
    def test_disabled_by_default(self):
        with patch.dict(os.environ, {'CITY_CORP_X_ENABLED':'false'}, clear=False):
            self.assertEqual(s.run(None,None)['reason'],'CITY_CORP_X_ENABLED=false')

    def test_channel_required(self):
        env={'CITY_CORP_X_ENABLED':'true','CITY_CORP_X_BUFFER_CHANNEL_ID':''}
        with patch.dict(os.environ,env,clear=False):
            self.assertEqual(s.run(None,None)['reason'],'dedicated_buffer_channel_missing')

    def test_slots_are_six_and_alternate_sources(self):
        self.assertEqual(len(s.SLOTS),6)
        self.assertEqual(sum(1 for *_,k in s.SLOTS if k=='close'),4)
        self.assertEqual(sum(1 for *_,k in s.SLOTS if k=='bankruptcy'),2)

    def test_close_copy_is_factual_and_links_to_store(self):
        text=s.close_text({'id':42,'name':'テスト商店','status':'closing','prefecture':'北海道','city':'札幌市','close_date':date(2026,9,30)})
        self.assertIn('【閉店予定｜北海道 札幌市】',text)
        self.assertIn('閉店日：2026/09/30',text)
        self.assertIn('https://open-close-map.onrender.com/store/42',text)
        self.assertNotIn('原因',text)

    def test_bankruptcy_copy_uses_published_stage_only(self):
        text=s.bankruptcy_text({'id':7,'company':'株式会社テスト','prefecture':'東京都','industry':'小売','reported_date':date(2026,9,11),'payload':{'stage':'破産手続開始決定'}})
        self.assertIn('【倒産速報｜東京都】',text)
        self.assertIn('破産手続開始決定',text)
        self.assertIn('https://buzz-now-1.onrender.com/corporate/company/7',text)
        self.assertNotIn('推定',text)

    def test_current_slot_uses_jst(self):
        # 2026-09-10 23:15 UTC == 2026-09-11 08:15 JST
        slot=s.current_slot(datetime(2026,9,10,23,15,tzinfo=timezone.utc))
        self.assertEqual(slot['kind'],'close')
        self.assertEqual(slot['key'],'2026-09-11T08:15')

if __name__=='__main__':unittest.main()
