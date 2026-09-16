from pathlib import Path

root=Path(__file__).resolve().parents[1]
include='{% include "_imobile_sp_buzznow_overlay.html" %}'

# Home: place the overlay include immediately before the fixed bottom navigation.
p=root/'templates/index.html'
text=p.read_text()
if include not in text:
    marker='  <nav class="bottom-nav">'
    if marker not in text: raise SystemExit('index bottom-nav marker missing')
    text=text.replace(marker, '  '+include+'\n\n'+marker, 1)
    p.write_text(text)

# Trend detail: place the overlay at the end of the normal page, before optional affiliate scripts.
p=root/'templates/trend.html'
text=p.read_text()
if include not in text:
    marker='\n{% if affiliate_enabled and affiliate_script_url %}'
    if marker not in text: raise SystemExit('trend footer marker missing')
    text=text.replace(marker, '\n'+include+'\n'+marker, 1)
    p.write_text(text)
