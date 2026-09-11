"""Idempotent source integration. Run on a feature branch, review diff, then test."""
import ast
from pathlib import Path

root = Path(__file__).resolve().parents[1]
main = root / 'app/main.py'
text = main.read_text()
marker = '# First-party visitor analytics (independent of modeled traffic metrics).'
addition = '''\n\n# First-party visitor analytics (independent of modeled traffic metrics).
from app.visitor_analytics import install_visitor_analytics
install_visitor_analytics(app, db, is_legacy=LEGACY_SERVICE)
'''
if marker not in text:
    ast.parse(text + addition)
    main.write_text(text + addition)
for name in ('index.html', 'trend.html'):
    target = root / 'templates' / name
    html = target.read_text()
    if '/static/visitor-analytics.js' not in html:
        if html.count('</head>') != 1:
            raise ValueError(f'Unexpected template structure: {name}')
        html = html.replace('</head>', '<script defer src="/static/visitor-analytics.js?v=1"></script>\n</head>', 1)
        target.write_text(html)
