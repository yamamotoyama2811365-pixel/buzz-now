from pathlib import Path

p=Path(__file__).resolve().parents[1]/'services/corporate/main.py'
text=p.read_text()
if 'im-158851c14b0f4ab3935a3aab85be0d33' not in text:
    marker="STYLE=Path(__file__).with_name('style.css').read_text()\n"
    helper=r'''STYLE=Path(__file__).with_name('style.css').read_text()

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
'''
    if marker not in text: raise SystemExit('STYLE marker missing')
    text=text.replace(marker,helper,1)
    old="+'</main><footer><strong>企業倒産・新規法人情報サイト</strong>"
    new="+'</main>'+imobile_pc_tag()+'<footer><strong>企業倒産・新規法人情報サイト</strong>"
    if old not in text: raise SystemExit('shell insertion point missing')
    text=text.replace(old,new,1)
    p.write_text(text)
