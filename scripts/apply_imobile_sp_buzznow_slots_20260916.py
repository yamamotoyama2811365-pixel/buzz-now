from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOP = '{% include "_imobile_sp_buzznow_top.html" %}'
RECT = '{% include "_imobile_sp_buzznow_rectangle.html" %}'
PC_TOP = '{% include "_imobile_pc_buzznow_top.html" %}'


def insert_after_pc_top(text: str) -> str:
    if TOP in text:
        return text
    pos = text.find(PC_TOP)
    if pos < 0:
        raise SystemExit('PC top marker not found')
    endif = text.find('{% endif %}', pos)
    if endif < 0:
        raise SystemExit('PC top endif not found')
    end = endif + len('{% endif %}')
    block = '\n\n  <!-- SP 325x50 inline; component self-gates to mobile only. -->\n  ' + TOP
    return text[:end] + block + text[end:]


def patch_index(path: Path):
    text = path.read_text()
    text = insert_after_pc_top(text)
    if RECT not in text:
        pc_mid = '    {% if imobile_pc_enabled %}\n    <aside class="section" aria-label="広告" style="text-align:center;margin:24px 0">'
        pos = text.find(pc_mid)
        if pos < 0:
            raise SystemExit('Index PC mid marker not found')
        block = '    <!-- SP 325x250 inline; separate spot for mid-content performance. -->\n    ' + RECT + '\n\n'
        text = text[:pos] + block + text[pos:]
    path.write_text(text)


def patch_trend(path: Path):
    text = path.read_text()
    text = insert_after_pc_top(text)
    if RECT not in text:
        marker = '    {% include "_magazine_points.html" %}'
        pos = text.find(marker)
        if pos < 0:
            raise SystemExit('Trend magazine marker not found')
        end = pos + len(marker)
        block = '\n\n    <!-- SP 325x250 inline after the article heading/summary. -->\n    ' + RECT
        text = text[:end] + block + text[end:]
    path.write_text(text)


patch_index(ROOT / 'templates/index.html')
patch_trend(ROOT / 'templates/trend.html')
