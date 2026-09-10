import unittest
from datetime import date
from services.open_close.collectors.public_quality import public_store, valid_store_name, valid_facility
from services.open_close.collectors.text_rules import extract_floor

class PublicQualityTest(unittest.TestCase):
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

