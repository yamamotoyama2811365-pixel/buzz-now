import hashlib
from io import BytesIO
from pathlib import Path
import unittest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient
from PIL import Image
from jinja2 import Environment, FileSystemLoader
from app.site_icons import router

ROOT = Path(__file__).resolve().parents[1]


class SiteIconsTest(unittest.TestCase):
    def setUp(self):
        app = FastAPI()
        app.include_router(router)
        self.client = TestClient(app)

    def test_approved_master_checksum_and_dimensions(self):
        data = (ROOT/'static/buzz-now-icon.png').read_bytes()
        self.assertEqual(hashlib.sha256(data).hexdigest(), 'b0c28a9751b39ec57a99d93d8b022bb66b1a5b78d58546091295f54586c90bda')
        with Image.open(BytesIO(data)) as image:
            self.assertEqual(image.size, (192,192))
            self.assertEqual(image.format, 'PNG')

    def test_favicon_get_is_real_multi_size_ico(self):
        response = self.client.get('/favicon.ico')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['content-type'], 'image/vnd.microsoft.icon')
        with Image.open(BytesIO(response.content)) as image:
            self.assertEqual(image.format, 'ICO')
            self.assertEqual(image.ico.sizes(), {(16,16),(32,32),(48,48),(64,64)})

    def test_touch_icon_is_180px_and_opaque(self):
        response = self.client.get('/apple-touch-icon.png')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['content-type'], 'image/png')
        with Image.open(BytesIO(response.content)) as image:
            self.assertEqual(image.size, (180,180))
            self.assertEqual(image.mode, 'RGB')

    def test_head_and_cache_and_no_cookies(self):
        for path in ('/favicon.ico','/apple-touch-icon.png'):
            get = self.client.get(path)
            head = self.client.head(path)
            self.assertEqual(head.status_code, 200)
            self.assertEqual(head.content, b'')
            self.assertEqual(int(head.headers['content-length']), len(get.content))
            self.assertIn('max-age=86400', head.headers['cache-control'])
            self.assertNotIn('set-cookie', head.headers)
            self.assertNotIn('x-robots-tag', head.headers)

    def test_every_buzz_template_includes_icons_once(self):
        for name in ('index.html','trend.html','x_gate.html'):
            text = (ROOT/'templates'/name).read_text()
            self.assertEqual(text.count('{% include "_site_icons.html" %}'),1)
            self.assertLess(text.index('_site_icons.html'),text.index('</head>'))

    def test_rendered_icon_links_are_local_and_stable(self):
        env = Environment(loader=FileSystemLoader(ROOT/'templates'))
        html = env.get_template('_site_icons.html').render()
        self.assertIn('href="/favicon.ico"',html)
        self.assertIn('sizes="192x192"',html)
        self.assertIn('href="/apple-touch-icon.png"',html)
        self.assertNotIn('http:',html)
        self.assertNotIn('<script',html)

    def test_main_serves_icons_without_database_or_background_jobs(self):
        import app.main as main
        # No TestClient context manager: startup/lifespan jobs are not run.
        client = TestClient(main.app)
        with patch.object(main,'db',side_effect=AssertionError('Icon must not access DB')):
            for path in ('/favicon.ico','/apple-touch-icon.png'):
                self.assertEqual(client.get(path).status_code,200)
                self.assertEqual(client.head(path).status_code,200)


if __name__ == '__main__':
    unittest.main()
