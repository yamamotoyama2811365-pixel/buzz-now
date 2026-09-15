"""Region extraction and publication regression checks; no production DB writes."""
import unittest
from unittest.mock import MagicMock, patch
from urllib.parse import quote
from xml.etree import ElementTree
from services.open_close.collectors.text_rules import detect_city, city_conflicts_with_prefecture
from services.open_close.collectors import seo_pages

class GeographyTest(unittest.TestCase):
    def test_prefecture_is_not_part_of_city(self):
        cases = {
            '北海道札幌市中央区北一条西': '札幌市中央区',
            '北海道帯広市稲田町': '帯広市',
            '北海道函館市本町': '函館市',
            '東京都港区芝公園': '港区',
            '東京都町田市原町田': '町田市',
            '三重県四日市市西浦': '四日市市',
            '千葉県市川市': '市川市',
            '石川県野々市市': '野々市市',
            '富山県中新川郡上市町': '中新川郡上市町',
            '奈良県吉野郡下市町': '吉野郡下市町',
            '札幌市中央区': '札幌市中央区',
            'おたからやが北海道室蘭市に開店': '室蘭市',
            '北海道の新店\n住所：北海道札幌市中央区': '札幌市中央区',
        }
        for value, expected in cases.items():
            with self.subTest(value=value):
                self.assertEqual(detect_city(value), expected)

    def test_mixed_prefectures_need_review_not_combination(self):
        self.assertIsNone(detect_city('北海道の店と東京都港区の店'))
        self.assertIsNone(detect_city('東京都港区と北海道札幌市'))
        self.assertIsNone(detect_city('地名未確認'))
        self.assertIsNone(detect_city(None))

    def test_only_explicit_conflicts_are_flagged(self):
        for city in ['東京都港区', '大阪府大阪市中央区', '神奈川県横浜市港北区']:
            self.assertTrue(city_conflicts_with_prefecture('北海道', city))
        for city in ['北海道札幌市中央区', '札幌市中央区', '港区', '', None]:
            self.assertFalse(city_conflicts_with_prefecture('北海道', city))
        self.assertFalse(city_conflicts_with_prefecture('東京都', '東京都港区'))

    def test_conflicting_area_is_review_page_without_querying_db(self):
        with patch.object(seo_pages, '_connect') as connect:
            page = seo_pages.render_area('unused', 'https://example.com', '北海道', '東京都港区')
        connect.assert_not_called()
        self.assertIn('地域情報を確認中です', page)
        self.assertIn('content="noindex,follow"', page)
        self.assertNotIn('北海道東京都港区の開店', page)

    def test_prefecture_navigation_omits_conflicting_cities_only(self):
        connection = MagicMock()
        cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [(3, 2, 1), (3,)]
        cursor.fetchall.side_effect = [
            [], [('札幌市中央区', 2), ('東京都港区', 1)], [], []
        ]
        with patch.object(seo_pages, '_connect', return_value=connection):
            page = seo_pages.render_area('unused', 'https://example.com', '北海道')
        self.assertIn(quote('札幌市中央区', safe=''), page)
        self.assertNotIn(quote('東京都港区', safe=''), page)
        self.assertIn('content="index,follow,max-image-preview:large"', page)

    def test_sitemap_omits_conflict_but_preserves_valid_urls(self):
        connection = MagicMock()
        cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
        # Keep this mock aligned with the sitemap's current aggregate columns:
        # prefecture/category rows return total/open/close counts, and city rows
        # return prefecture, city, total, open, close.
        cursor.fetchall.side_effect = [
            [('北海道', 2, 1, 1), ('東京都', 1, 1, 0)],
            [
                ('北海道', '札幌市中央区', 2, 1, 1),
                ('北海道', '東京都港区', 1, 0, 1),
                ('東京都', '東京都港区', 2, 1, 1),
            ],
            [('飲食', 3, 2, 1)],
            [(68, None)],
        ]
        with patch.object(seo_pages, '_connect', return_value=connection):
            xml = seo_pages.sitemap_xml('unused', 'https://example.com')
        urls = [n.text for n in ElementTree.fromstring(xml).findall('.//{*}loc')]
        def area(pref, city):
            return 'https://example.com/area/' + quote(pref, safe='') + '/' + quote(city, safe='')
        self.assertNotIn(area('北海道', '東京都港区'), urls)
        self.assertIn(area('北海道', '札幌市中央区'), urls)
        self.assertIn(area('東京都', '東京都港区'), urls)
        self.assertIn('https://example.com/store/68', urls)

if __name__ == '__main__':
    unittest.main()
