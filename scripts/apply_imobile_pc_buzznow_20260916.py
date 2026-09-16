from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]


def replace(path, old, new):
    p=ROOT/path
    text=p.read_text()
    if new in text:
        return False
    if old not in text:
        raise SystemExit(f'patch target missing: {path}: {old[:100]!r}')
    p.write_text(text.replace(old,new,1))
    return True

# Render the PC-only tag only for desktop user agents. Keep the owner-provided ad tag untouched.
replace(
    'app/main.py',
    'templates = Jinja2Templates(directory=BASE / "templates")\n',
    'templates = Jinja2Templates(directory=BASE / "templates")\n\n_PC_MOBILE_UA = re.compile(r"iphone|ipad|ipod|android|mobile|windows phone|blackberry|opera mini|opera mobi", re.I)\n\ndef _imobile_pc_enabled(request: Request) -> bool:\n    return not bool(_PC_MOBILE_UA.search(request.headers.get("user-agent", "")))\n'
)
replace(
    'app/main.py',
    '        "site_name": SITE_NAME,\n    })\n',
    '        "site_name": SITE_NAME,\n        "imobile_pc_enabled": _imobile_pc_enabled(request),\n    })\n'
)
# Trend detail has its own template context.
needle='        "canonical": canonical,\n    })\n'
replacement='        "canonical": canonical,\n        "imobile_pc_enabled": _imobile_pc_enabled(request),\n    })\n'
p=ROOT/'app/main.py'; text=p.read_text()
if replacement not in text:
    if needle not in text: raise SystemExit('trend context target missing')
    p.write_text(text.replace(needle,replacement,1))

# Top page: show one PC 300x250 after the introductory content.
replace(
    'templates/index.html',
    '    </section>\n\n<section class="summary-grid">',
    '    </section>\n\n    {% if imobile_pc_enabled %}\n    <aside class="section" aria-label="広告" style="text-align:center;margin:24px 0">\n      <p style="font-size:12px;color:#94a3b8;margin:0 0 10px">広告</p>\n      {% include "_imobile_pc_buzznow.html" %}\n    </aside>\n    {% endif %}\n\n<section class="summary-grid">'
)

# Article: prioritize i-mobile on desktop; retain existing A8 block only on non-PC traffic until SP i-mobile is approved.
p=ROOT/'templates/trend.html'; text=p.read_text()
start='    <aside class="section a8-advertisement" aria-label="広告">\n'
end='    </aside>\n\n    {{ next_links(related_rows or top_now_rows, \'あわせて読みたい話題\', \'article_top\', 3) }}'
if '{% include "_imobile_pc_buzznow.html" %}' not in text:
    i=text.find(start)
    j=text.find(end,i)
    if i<0 or j<0: raise SystemExit('trend ad block target missing')
    old=text[i:j+len('    </aside>\n')]
    new='''    {% if imobile_pc_enabled %}\n    <aside class="section a8-advertisement" aria-label="広告">\n      <p style="font-size:12px;color:#b8c3d1;margin:0 0 10px">広告</p>\n      <div class="a8-ad-grid"><div class="a8-ad-unit">\n        {% include "_imobile_pc_buzznow.html" %}\n      </div></div>\n    </aside>\n    {% else %}\n'''+old+'''    {% endif %}\n'''
    p.write_text(text[:i]+new+text[j+len('    </aside>\n'):])
