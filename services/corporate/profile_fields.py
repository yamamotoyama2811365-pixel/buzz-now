"""Shared checks for extracted profile values, including previously saved values."""
import re
import unicodedata
from copy import deepcopy


def usable_value(value):
    if not isinstance(value,str):return False
    value=unicodedata.normalize('NFKC',value).strip()
    return bool(value and len(value)<=100 and re.search(r'[\w一-龥]',value)
                and not re.search(r'ログイン|会員|登録して|情報を見る|非公開|未公開|未確認|不明|記載なし|個人情報|[＊*]{2,}',value))


def representatives(profile):
    return [d for d in (profile or {}).get('details',[]) if d.get('label')=='代表者' and usable_value(d.get('value'))]


def website(profile):
    if not profile:return ''
    return profile.get('company_website_url') or (profile.get('website_url','') if profile.get('verification_status')!='reference' else '')


def complete(profile):
    return bool(website(profile) and representatives(profile))


def combine_profiles(primary,secondary):
    """Keep complementary, source-attributed fields for an already matched entity."""
    if not primary:return secondary
    if not secondary:return primary
    result=deepcopy(primary)
    if not website(result) and website(secondary):
        result['company_website_url']=website(secondary)
        result['website_source_url']=secondary.get('website_source_url') or secondary.get('website_url')
    if not representatives(result):
        result['details']=[d for d in result.get('details',[]) if d.get('label')!='代表者']+representatives(secondary)
    required=[result['website_source_url']] if result.get('website_source_url') else []
    required += [d['source_url'] for d in result.get('details',[])]
    result['evidence_urls']=list(dict.fromkeys(required+result.get('evidence_urls',[])+secondary.get('evidence_urls',[])))[:4]
    result['details']=[d for d in result.get('details',[]) if d['source_url'] in result['evidence_urls']][:6]
    return result
