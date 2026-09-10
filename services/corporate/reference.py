"""Rendering of source-backed but unconfirmed company information."""
from html import escape as e


def render_reference(profile, correction_url):
    if not profile or profile.get('verification_status')!='reference':return ''
    html='<section class="panel side"><h2>ウェブから収集した参考情報（未確定）</h2>'
    html+='<p class="notice">※社名と住所などを照合して情報を補完しています。本人・法人への確認は行っていません。<a href="'+e(correction_url,quote=True)+'">修正依頼はこちら</a></p>'
    html+='<dl class="facts"><dt>業種・事業（参考）</dt><dd>'+e('、'.join(profile.get('business_tags') or profile.get('industries',[])) or '記載を取得できませんでした')+'</dd>'
    for d in profile.get('details',[]):
        html+='<dt>'+e(d['label'])+'（参考）</dt><dd>'+e(d['value'])+' <a class="source" href="'+e(d['source_url'],quote=True)+'" target="_blank" rel="noopener noreferrer">出典 ↗</a></dd>'
    html+='<dt>照合の範囲</dt><dd>'+e(profile['match_basis'])+'</dd><dt>取得日</dt><dd>'+e(profile['checked_at'][:10])+'</dd></dl>'
    html+='<p class="small">取得元に掲載されていた情報です。現在の代表者・営業状況や、倒産時点との一致は未確認です。</p>'
    html+='<div class="links">'+''.join('<a class="source" href="'+e(url,quote=True)+'" target="_blank" rel="noopener noreferrer">情報元 '+str(i+1)+' ↗</a>' for i,url in enumerate(profile['evidence_urls']))+'</div></section>'
    return html
