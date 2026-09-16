from pathlib import Path

root=Path(__file__).resolve().parents[1]
mid2='{% include "_imobile_sp_buzznow_rectangle_mid2.html" %}'
bottom='{% include "_imobile_sp_buzznow_rectangle_bottom.html" %}'

for rel in ('templates/index.html','templates/trend.html'):
    p=root/rel
    text=p.read_text()
    text=text.replace('<!-- SP_INLINE_SLOT_3_PENDING: separate approved 325x250 tag required -->', mid2)
    text=text.replace('<!-- SP_INLINE_SLOT_4_PENDING: separate approved 325x250 tag required -->', bottom)
    text=text.replace('SP 325x250 inline','SP 300x250 inline')
    text=text.replace('SP 325x50 inline','SP 320x50 inline')
    if mid2 not in text or bottom not in text:
        raise SystemExit(f'missing placement in {rel}')
    p.write_text(text)
# trigger 2026-09-16 16:22 JST
