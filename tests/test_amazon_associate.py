"""Offline checks: do not click or fetch Amazon affiliate links."""
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlsplit, parse_qs
import unittest
from jinja2 import Environment, FileSystemLoader, select_autoescape

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_URL = 'https://www.amazon.co.jp/b?_encoding=UTF8&node=10393976051&pf_rd_p=4c9d0136-a2fe-438c-ad8d-3e7287fee606&pf_rd_r=ASK5R47JTXX8BVXS7V1Z&linkCode=ll2&tag=buzznow-22&linkId=d6cbbbd3827b868f42e55079be888b18&ref_=as_li_ss_tl'
INCLUDE = '{% include "_amazon_associate.html" %}'
CSS = '<link rel="stylesheet" href="/static/amazon-associate.css?v=20260911">'


class Parser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.elements = []
    def handle_starttag(self, tag, attrs):
        self.elements.append((tag, dict(attrs)))


class AmazonAssociateTest(unittest.TestCase):
    def setUp(self):
        self.env = Environment(loader=FileSystemLoader(ROOT / 'templates'), autoescape=select_autoescape(['html']))
        self.html = self.env.get_template('_amazon_associate.html').render()
        self.parser = Parser()
        self.parser.feed(self.html)
        self.links = [attrs for tag, attrs in self.parser.elements if tag == 'a']

    def test_exact_owner_link_and_tag_are_preserved(self):
        self.assertEqual(len(self.links), 1)
        self.assertEqual(self.links[0]['href'], EXPECTED_URL)
        parsed = urlsplit(self.links[0]['href'])
        self.assertEqual((parsed.scheme, parsed.hostname, parsed.path), ('https', 'www.amazon.co.jp', '/b'))
        self.assertEqual(parse_qs(parsed.query)['tag'], ['buzznow-22'])
        self.assertEqual(parse_qs(parsed.query)['node'], ['10393976051'])

    def test_ad_label_and_required_disclosure_are_visible(self):
        self.assertIn('広告・PR｜Amazonアソシエイト', self.html)
        self.assertIn('Amazonのアソシエイトとして、BUZZ NOWは適格販売により収入を得ています。', self.html)
        self.assertNotIn('display:none', self.html)
        self.assertNotIn(' hidden', self.html)

    def test_link_is_marked_and_origin_is_not_hidden(self):
        attrs = self.links[0]
        self.assertEqual(set(attrs['rel'].split()), {'sponsored', 'nofollow', 'noopener'})
        self.assertEqual(attrs['referrerpolicy'], 'strict-origin-when-cross-origin')
        self.assertEqual(attrs['target'], '_blank')
        self.assertIn('新しいタブ', attrs['aria-label'])

    def test_no_remote_resources_or_auto_clicks(self):
        tags = {tag for tag, attrs in self.parser.elements}
        self.assertFalse(tags & {'script', 'iframe', 'img', 'object', 'form'})
        for tag, attrs in self.parser.elements:
            self.assertFalse(any(name.startswith('on') for name in attrs))
        self.assertNotIn('fetch(', self.html)
        self.assertNotIn('window.open', self.html)

    def test_no_prices_rates_or_blanket_promotion_claims(self):
        for text in ('全商品', 'ポイント2倍', '開催中', '最安値', '円', '%', '％'):
            self.assertNotIn(text, self.html)
        self.assertIn('開催状況', self.html)
        self.assertIn('エントリーの要否', self.html)
        self.assertIn('商品価格と送料の合計', self.html)

    def test_integrated_templates_compile_and_have_one_card(self):
        for name in ('index.html', 'trend.html'):
            self.env.get_template(name)
            raw = (ROOT / 'templates' / name).read_text()
            self.assertEqual(raw.count(INCLUDE), 1)
            self.assertEqual(raw.count(CSS), 1)
            self.assertLess(raw.index(CSS), raw.index('</head>'))
        trend = (ROOT / 'templates/trend.html').read_text()
        self.assertGreater(trend.index(INCLUDE), trend.index('  </article>\n'))
        home = (ROOT / 'templates/index.html').read_text()
        self.assertGreater(home.index(INCLUDE), home.index('id="ranking"'))

    def test_other_sites_and_social_landing_are_not_changed(self):
        self.assertNotIn(INCLUDE, (ROOT / 'templates/x_gate.html').read_text())
        for root in (ROOT / 'services', ROOT / 'templates'):
            for path in root.rglob('*.html'):
                if path.name not in ('index.html', 'trend.html', '_amazon_associate.html'):
                    self.assertNotIn(INCLUDE, path.read_text(), str(path))

    def test_responsive_scoped_css(self):
        css = (ROOT / 'static/amazon-associate.css').read_text()
        self.assertIn('@media(max-width:680px)', css)
        self.assertIn(':focus-visible', css)
        self.assertNotIn('http', css)
        self.assertNotIn('url(', css)
        self.assertNotIn('position:fixed', css)


if __name__ == '__main__':
    unittest.main()
