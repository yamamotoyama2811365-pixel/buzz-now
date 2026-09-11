"""Narrow, idempotent template integration. Does not access a DB or Amazon."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INCLUDE = '{% include "_amazon_associate.html" %}'
CSS = '<link rel="stylesheet" href="/static/amazon-associate.css?v=20260911">'


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise ValueError('Missing or ambiguous integration anchor: ' + old[:90])
    return text.replace(old, new, 1)


def install():
    changes = {}
    for name in ('index.html', 'trend.html'):
        path = ROOT / 'templates' / name
        text = path.read_text()
        if CSS not in text:
            text = replace_once(text, '</head>', CSS + '\n</head>')
        if INCLUDE not in text:
            if name == 'index.html':
                anchor = '    <section class="money-section">'
                text = replace_once(text, anchor, '    ' + INCLUDE + '\n\n' + anchor)
            else:
                anchor = '  </article>\n</div>'
                text = replace_once(text, anchor, '  </article>\n  ' + INCLUDE + '\n</div>')
        if text.count(INCLUDE) != 1 or text.count(CSS) != 1:
            raise ValueError('Duplicate Amazon insertion: ' + name)
        changes[path] = text
    for path, text in changes.items():
        if path.read_text() != text:
            path.write_text(text)
    print('Amazon affiliate card integrated once in homepage and after trend article')


if __name__ == '__main__':
    install()
