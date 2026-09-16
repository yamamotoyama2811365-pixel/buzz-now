from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def patch(path: str, old: str, new: str):
    p = ROOT / path
    text = p.read_text()
    if new in text:
        return False
    if old not in text:
        raise SystemExit(f"patch target missing: {path}")
    p.write_text(text.replace(old, new, 1))
    return True

index_block = '''  {% if imobile_pc_enabled %}\n  <aside aria-label="広告" style="text-align:center;margin:18px auto 20px;min-height:90px;overflow:hidden">\n    <p style="font-size:12px;color:#94a3b8;margin:0 0 8px">広告</p>\n    {% include "_imobile_pc_buzznow_top.html" %}\n  </aside>\n  {% endif %}\n\n'''
patch(
    "templates/index.html",
    "  </header>\n\n  <nav class=\"top-tabs\" aria-label=\"ランキング切替\">",
    "  </header>\n\n" + index_block + "  <nav class=\"top-tabs\" aria-label=\"ランキング切替\">",
)

trend_block = '''  {% if imobile_pc_enabled %}\n  <aside aria-label="広告" style="text-align:center;margin:18px auto 22px;min-height:90px;overflow:hidden">\n    <p style="font-size:12px;color:#b8c3d1;margin:0 0 8px">広告</p>\n    {% include "_imobile_pc_buzznow_top.html" %}\n  </aside>\n  {% endif %}\n'''
patch(
    "templates/trend.html",
    "  </header>\n\n  <article class=\"detail\">",
    "  </header>\n\n" + trend_block + "\n  <article class=\"detail\">",
)
