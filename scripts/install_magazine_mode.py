"""Idempotently wire magazine-mode helpers into BUZZ NOW.

No DB writes or network calls. Intended for a feature-branch CI checkout.
"""
from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT / 'app/main.py'
TREND = ROOT / 'templates/trend.html'
MARKER = '# Magazine mode v1: S-style circulation without copying third-party content.'


def once(text, old, new):
    if text.count(old) != 1:
        raise ValueError('missing/ambiguous anchor: ' + old[:100])
    return text.replace(old, new, 1)


def install_main():
    text = MAIN.read_text()
    if MARKER not in text:
        text = once(text, 'from app import social_tracking\n', 'from app import social_tracking\nfrom app import magazine\n' + MARKER + '\n')

    old_post = '''    return f"BUZZ NOW｜話題をチェック\\n{reason}\\n背景・出典を確認 ↓\\n{url}"'''
    new_post = '''    headline = reason[len("関連報道："):] if reason.startswith("関連報道：") else f"「{row['keyword']}」が話題。なぜ今？"\n    return f"{headline}\\n{url}"'''
    if old_post in text:
        text = once(text, old_post, new_post)
    elif 'headline = reason[len("関連報道："):]' not in text:
        raise ValueError('social post format anchor missing')

    anchor_rows = '''        related_rows = _trend_related_rows(c, trend["id"], trend["keyword"], trend["category"], 5)\n        top_now_rows = _trend_top_now_rows(c, 5)\n        prebuzz_rows = _trend_prebuzz_rows(c, 5)'''
    if 'viral_posts = magazine.viral_posts' not in text:
        replacement = anchor_rows + '''\n        viral_posts = magazine.viral_posts(c, trend["keyword"], 4)\n        magazine_rows = magazine.internal_buzz_rows(c, trend["id"], 8)\n        buzz_points = magazine.three_points(trend, briefing, editorial_brief)'''
        text = once(text, anchor_rows, replacement)

    context_anchor = '''            "prebuzz_rows": prebuzz_rows,'''
    if '"viral_posts": viral_posts' not in text:
        text = once(text, context_anchor, context_anchor + '''\n            "viral_posts": viral_posts,\n            "magazine_rows": magazine_rows,\n            "buzz_points": buzz_points,''')

    ast.parse(text)
    MAIN.write_text(text)


def install_template():
    text = TREND.read_text()
    css = '<link rel="stylesheet" href="/static/magazine.css?v=20260915">'
    if css not in text:
        text = once(text, '</head>', css + '\n</head>')
    points = '{% include "_magazine_points.html" %}'
    h1 = '''    <h1>{{ trend['keyword'] }}はなぜ話題？<br>確認できた関連ニュース</h1>'''
    if points not in text:
        text = once(text, h1, h1 + '\n    ' + points)
    stream = '{% include "_magazine_stream.html" %}'
    ad = '    <aside class="section a8-advertisement" aria-label="広告">'
    if stream not in text:
        search_from = text.find('class="section news-briefing"')
        pos = text.find(ad, search_from)
        if search_from < 0 or pos < 0:
            raise ValueError('first article ad block not found after news briefing')
        text = text[:pos] + '    ' + stream + '\n\n' + text[pos:]
    if text.count(points) != 1 or text.count(stream) != 1 or text.count(css) != 1:
        raise ValueError('duplicate magazine integration')
    TREND.write_text(text)


if __name__ == '__main__':
    install_main()
    install_template()
    print('Magazine mode integrated: compact X posts, 3POINT, X embeds, internal circulation')
