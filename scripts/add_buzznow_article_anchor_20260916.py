from pathlib import Path
p=Path(__file__).resolve().parents[1]/'templates/trend.html'
text=p.read_text()
old='<article class="detail">'
new='<article class="detail" id="buzznow-article-body">'
if new not in text:
    if old not in text:
        raise SystemExit('trend article marker missing')
    text=text.replace(old,new,1)
    p.write_text(text)
