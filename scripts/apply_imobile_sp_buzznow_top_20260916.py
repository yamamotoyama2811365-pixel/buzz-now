from pathlib import Path

root=Path(__file__).resolve().parents[1]
marker='{% include "_imobile_sp_buzznow_top.html" %}'

files={
    root/'templates/index.html': (
        '  {% endif %}\n\n  <nav class="top-tabs" aria-label="ランキング切替">',
        '  {% endif %}\n\n  '+marker+'\n\n  <nav class="top-tabs" aria-label="ランキング切替">'
    ),
    root/'templates/trend.html': (
        '  {% endif %}\n\n  <article class="detail">',
        '  {% endif %}\n\n  '+marker+'\n\n  <article class="detail">'
    ),
}

for path,(old,new) in files.items():
    text=path.read_text()
    if marker in text:
        continue
    if old not in text:
        raise SystemExit(f'insertion point missing: {path}')
    path.write_text(text.replace(old,new,1))
