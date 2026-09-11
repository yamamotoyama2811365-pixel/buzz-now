"""Rendering of source-backed but unconfirmed company information."""
from html import escape as e
from .profile_fields import usable_value, representatives, website


def render_reference(profile, correction_url):
    if not profile:return ''
    html='<section class="panel side"><h2>ウェブから収集した参考情報（未確定）</h2>'
    html+='<p class="notice">※社名・住所などから自動照合した参考情報です。誤りがあれば申告してください。本人・法人への確認は行っていません。<a href="'+e(correction_url,quote=True)+'">修正依頼はこちら</a></p>'
    html+='<dl class="facts"><dt>代表者（参考）</dt><dd>'
    reps=representatives(profile)
    html+=('、'.join(e(d['value'])+' <a class="source" href="'+e(d['source_url'],quote=True)+'" target="_blank" rel="noopener noreferrer">出典 ↗</a>' for d in reps) or '未取得（確認中）')+'</dd>'
    target=website(profile)
    html+='<dt>ウェブサイトURL（候補）</dt><dd>'
    html+=('<a class="source" href="'+e(target,quote=True)+'" target="_blank" rel="noopener noreferrer">'+e(target)+' ↗</a>' if target else '未取得（確認中）')+'</dd>'
    if target and profile.get('website_source_url'):
        html+='<dt>URLの情報元</dt><dd><a href="'+e(profile['website_source_url'],quote=True)+'" target="_blank" rel="noopener noreferrer">URLの記載・照合元を確認 ↗</a></dd>'
    tags=[x for x in (profile.get('business_tags') or profile.get('industries',[])) if usable_value(x)]
    html+='<dt>業種・事業（参考）</dt><dd>'+e('、'.join(tags) or '未取得（確認中）')+'</dd>'
    for d in profile.get('details',[]):
        if d['label']=='代表者' or not usable_value(d['value']):continue
        html+='<dt>'+e(d['label'])+'（参考）</dt><dd>'+e(d['value'])+' <a class="source" href="'+e(d['source_url'],quote=True)+'" target="_blank" rel="noopener noreferrer">出典 ↗</a></dd>'
    html+='<dt>照合の範囲</dt><dd>'+e(profile['match_basis'].replace('報道元の公式サイトリンク','報道元リンク'))+'</dd><dt>取得日</dt><dd>'+e(profile['checked_at'][:10])+'</dd></dl>'
    html+='<p class="small">取得元に掲載されていた情報です。現在の代表者・営業状況や、倒産時点との一致は未確認です。</p>'
    html+='<div class="links">'+''.join('<a class="source" href="'+e(url,quote=True)+'" target="_blank" rel="noopener noreferrer">情報元 '+str(i+1)+' ↗</a>' for i,url in enumerate(profile['evidence_urls']))+'</div></section>'
    return html
