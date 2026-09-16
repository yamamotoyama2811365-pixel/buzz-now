from pathlib import Path

p=Path(__file__).resolve().parents[1]/'services/corporate/main.py'
text=p.read_text()

# Existing 300x250 slot installation remains idempotent.
if 'im-158851c14b0f4ab3935a3aab85be0d33' not in text:
    marker="STYLE=Path(__file__).with_name('style.css').read_text()\n"
    helper=r"""STYLE=Path(__file__).with_name('style.css').read_text()

IMOBILE_PC_HTML='''<div id="im-158851c14b0f4ab3935a3aab85be0d33">
  <script async src="https://imp-adedge.i-mobile.co.jp/script/v1/spot.js?20220104"></script>
  <script>(window.adsbyimobile=window.adsbyimobile||[]).push({pid:85420,mid:596383,asid:1944823,type:"banner",display:"inline",elementid:"im-158851c14b0f4ab3935a3aab85be0d33"})</script>
</div>'''

def imobile_pc_tag():
    payload=json.dumps(IMOBILE_PC_HTML,ensure_ascii=False).replace('</script>','<\\/script>')
    return ('<script>(function(){var ua=navigator.userAgent||"";'
            'if(/iphone|ipad|ipod|android|mobile|windows phone|blackberry|opera mini|opera mobi/i.test(ua))return;'
            'if(!window.matchMedia("(min-width: 769px)").matches)return;'
            'document.write("<aside aria-label=\\"広告\\" style=\\"text-align:center;margin:28px auto;min-height:250px\\"><div style=\\"font-size:12px;opacity:.62;margin-bottom:8px\\">広告</div>"+'+payload+'+"</aside>");})();</script>')
"""
    if marker not in text: raise SystemExit('STYLE marker missing')
    text=text.replace(marker,helper,1)
    old="+'</main><footer><strong>企業倒産・新規法人情報サイト</strong>"
    new="+'</main>'+imobile_pc_tag()+'<footer><strong>企業倒産・新規法人情報サイト</strong>"
    if old not in text: raise SystemExit('shell insertion point missing')
    text=text.replace(old,new,1)

# 728x90 leaderboard immediately below the site header, PC only.
if 'im-d3359a305e3843298606fa646370358f' not in text:
    marker='def db():\n'
    top_helper=r'''IMOBILE_PC_TOP_HTML=''' + repr('''<div id="im-d3359a305e3843298606fa646370358f">
  <script async src="https://imp-adedge.i-mobile.co.jp/script/v1/spot.js?20220104"></script>
  <script>(window.adsbyimobile=window.adsbyimobile||[]).push({pid:85420,mid:596383,asid:1944835,type:"banner",display:"inline",elementid:"im-d3359a305e3843298606fa646370358f"})</script>
</div>''') + r'''

def imobile_pc_top_tag():
    payload=json.dumps(IMOBILE_PC_TOP_HTML,ensure_ascii=False).replace('</script>','<\\/script>')
    return ('<script>(function(){var ua=navigator.userAgent||"";'
            'if(/iphone|ipad|ipod|android|mobile|windows phone|blackberry|opera mini|opera mobi/i.test(ua))return;'
            'if(!window.matchMedia("(min-width: 769px)").matches)return;'
            'document.write("<aside aria-label=\\"広告\\" style=\\"text-align:center;margin:16px auto 22px;min-height:90px;overflow:hidden\\"><div style=\\"font-size:12px;opacity:.62;margin-bottom:6px\\">広告</div>"+'+payload+'+"</aside>");})();</script>')

'''
    if marker not in text: raise SystemExit('db marker missing')
    text=text.replace(marker,top_helper+marker,1)
    old="</nav></div></header><main>'+body+'</main>'+imobile_pc_tag()"
    new="</nav></div></header>'+imobile_pc_top_tag()+'<main>'+body+'</main>'+imobile_pc_tag()"
    if old not in text: raise SystemExit('corporate header insertion point missing')
    text=text.replace(old,new,1)

p.write_text(text)
