import unittest
from unittest.mock import patch, MagicMock
from services.open_close.collectors import seo_pages
from datetime import date
from services.open_close.collectors.public_quality import public_store, valid_store_name, valid_facility
from services.open_close.collectors.text_rules import extract_floor

class PublicQualityTest(unittest.TestCase):
    def test_rendered_store_review_and_normal_indexability(self):
        for name, pending in [('＜', True), ('実在店名サンプル', False)]:
            with self.subTest(name=name):
                row = (2347, name, 'opening', '居酒屋', '北海道', '帯広市',
                       '', None, None, None, date(2026, 7, 28), None,
                       'https://example.com/source', '掲載元', None, 80, None)
                connection = MagicMock()
                cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
                cursor.fetchone.side_effect = [row, (0, 0, 0, 0, 0)]
                cursor.fetchall.return_value = []
                with patch.object(seo_pages, '_connect', return_value=connection):
                    rendered = seo_pages.render_store('unused', 'https://example.com', 2347)
                self.assertIn('href="https://example.com/source"', rendered)
                if pending:
                    self.assertIn('content="noindex,follow"', rendered)
                    self.assertIn('<dd>店舗名未確認</dd>', rendered)
                    self.assertIn('<dt>状況</dt><dd>店舗情報</dd>', rendered)
                    self.assertNotIn('2026年7月28日', rendered)
                else:
                    self.assertIn('content="index,follow,max-image-preview:large"', rendered)
                    self.assertIn('<dd>実在店名サンプル</dd>', rendered)
                    self.assertIn('2026年7月28日', rendered)

    def test_symbol_only_names_are_pending_without_changing_source(self):
        for name in ['＜', '<', '＞', '...', '「」', '＆', '　★　', '🏬']:
            with self.subTest(name=name):
                original = dict(id=2347, name=name, status='opening',
                                category='居酒屋', open_date=date(2026, 7, 28),
                                source_url='https://example.com/source')
                safe = public_store(original)
                self.assertTrue(safe['quality_pending'])
                self.assertEqual(safe['name'], '店舗名未確認')
                for field in ('status', 'category', 'open_date', 'close_date'):
                    self.assertIsNone(safe[field])
                self.assertEqual(safe['source_url'], original['source_url'])
                self.assertEqual(original['name'], name)
                self.assertEqual(original['status'], 'opening')
                self.assertEqual(original['open_date'], date(2026, 7, 28))

    def test_short_and_punctuated_real_names_are_preserved(self):
        for name in ['一', '123', '８番らーめん', '% ARABICA', 'A&W',
                     "50’s DINER ROCA", 'ノースコンチネント 赤レンガ店']:
            with self.subTest(name=name):
                safe = public_store(dict(name=name, status='open'))
                self.assertFalse(safe['quality_pending'])
                self.assertEqual(safe['name'], name)
                self.assertEqual(safe['status'], 'open')

    def test_bad_headline_does_not_publish_attached_facts(self):
        original = dict(name='さわやか」静岡市駿河区の「イオンセントラルスクエア静岡」に新店舗', category='スーパー', floor='0F', facility_name='に新店', open_date=date(2027,6,1))
        safe = public_store(original)
        self.assertEqual(safe['name'], '店舗名未確認')
        self.assertIsNone(safe['open_date'])
        self.assertIsNone(safe['category'])
        self.assertEqual(original['category'], 'スーパー')
    def test_valid_store_is_preserved(self):
        original = dict(name='さわやか 静岡池田店', category='飲食', floor='Ｂ１Ｆ', facility_name='イオンセントラルスクエア静岡', open_date=date(2026,9,10))
        safe = public_store(original)
        self.assertEqual(safe['name'], original['name'])
        self.assertEqual(safe['open_date'], original['open_date'])
        self.assertEqual(safe['floor'], 'B1F')
        self.assertFalse(safe['quality_pending'])
    def test_floor_boundaries(self):
        for value in ['0F', '100F', '10FLOOR', 'B0F']:
            self.assertIsNone(extract_floor(value), value)
        for value, expected in [('地下1階','地下1階'),('施設 ２Ｆ','2F'),('ビル 10階','10階')]:
            self.assertEqual(extract_floor(value), expected)
    def test_facility_fragment(self):
        self.assertIsNone(valid_facility('に新店'))
        self.assertEqual(valid_facility('新宿センタービル'), '新宿センタービル')

if __name__ == '__main__': unittest.main()
