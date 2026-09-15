"""Switch magazine X cards from script-dependent blockquotes to direct official X embeds.

No X post text/media is copied into BUZZ NOW. We only build the official
platform.twitter.com embed URL from a previously validated numeric tweet id.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / 'templates/_magazine_stream.html'
CSS = ROOT / 'static/magazine.css'
TESTS = ROOT / 'tests/test_magazine_mode.py'

OLD = '''      <blockquote class="twitter-tweet" data-theme="dark" data-dnt="true" data-conversation="none">\n        <a href="{{ post.tweet_url }}" target="_blank" rel="nofollow noopener noreferrer">Xの公開投稿を見る ↗</a>\n      </blockquote>\n'''
NEW = '''      <div class="bn-x-embed-wrap">\n        <iframe\n          class="bn-x-embed"\n          src="https://platform.twitter.com/embed/Tweet.html?id={{ post.tweet_id }}&theme=dark&dnt=true"\n          title="X公開投稿: {{ post.keyword }}"\n          loading="lazy"\n          scrolling="no"\n          frameborder="0"\n          allowtransparency="true"\n          referrerpolicy="no-referrer-when-downgrade">\n        </iframe>\n      </div>\n      <a class="bn-x-fallback" href="{{ post.tweet_url }}" target="_blank" rel="nofollow noopener noreferrer">Xで元の投稿を開く ↗</a>\n'''


def apply():
    html = TEMPLATE.read_text()
    if OLD in html:
        html = html.replace(OLD, NEW, 1)
    elif 'platform.twitter.com/embed/Tweet.html?id={{ post.tweet_id }}' not in html:
        raise ValueError('X embed template anchor not found')
    html = html.replace('  <script async src="https://platform.twitter.com/widgets.js" charset="utf-8"></script>\n', '')
    TEMPLATE.write_text(html)

    css = CSS.read_text()
    marker = '.bn-x-quote-card .twitter-tweet{margin:0 auto!important;max-width:100%!important}'
    replacement = '.bn-x-embed-wrap{width:100%;min-height:380px;border-radius:12px;overflow:hidden;background:#000}.bn-x-embed{display:block;width:100%;height:520px;max-height:70vh;border:0;background:#000}.bn-x-fallback{display:inline-flex;margin:10px 6px 2px;color:#dce6ef;font-size:12px;font-weight:700;text-underline-offset:3px}'
    if marker in css:
        css = css.replace(marker, replacement, 1)
    elif '.bn-x-embed-wrap{' not in css:
        raise ValueError('Magazine X CSS anchor not found')
    CSS.write_text(css)

    tests = TESTS.read_text()
    tests = tests.replace("self.assertIn('platform.twitter.com/widgets.js',html)\n        self.assertIn('{{ post.tweet_url }}',html)", "self.assertIn('platform.twitter.com/embed/Tweet.html?id={{ post.tweet_id }}',html)\n        self.assertIn('{{ post.tweet_url }}',html)\n        self.assertNotIn('platform.twitter.com/widgets.js',html)")
    TESTS.write_text(tests)


if __name__ == '__main__':
    apply()
    print('Installed direct official X iframe embeds')
