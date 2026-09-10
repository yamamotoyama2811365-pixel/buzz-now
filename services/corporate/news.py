"""Cited reporting enrichment; no article text is persisted or republished."""
import re
import os
import unicodedata
from datetime import date, datetime, timezone
from urllib.parse import urlsplit
from html import escape
import requests
from bs4 import BeautifulSoup
from .enrich import Fetcher, public_url, norm
from .model import normalized_name, INDUSTRIES, CAUSES

VERSION = 2
# User-supplied discovery leads are still fetched and matched, never trusted as facts.
LEADS = {'bfb9524de141817310bcabbb4': ['https://sapporo-zakkan.seesaa.net/article/521485415.html']}
FIELDS = {'representative':'報道に記載の代表者', 'business':'報道に記載の事業',
          'stores':'報道に記載の店舗・ブランド', 'debt':'報道に記載の負債額',
          'employees':'報道に記載の従業員数', 'causes':'報道で言及された背景'}

class NewsFetcher(Fetcher):
    def page(self, url):
        for _ in range(4):
            url = public_url(url)
            if not self.allowed(url): raise ValueError('robots disallow')
            status, text, target = self.raw(url)
            if status in {301,302,303,307,308}:
                url = target
                continue
            if status != 200: raise ValueError('page unavailable')
            return target, BeautifulSoup(text, 'html.parser')
        raise ValueError('redirect limit')

def article(soup):
    clone = BeautifulSoup(str(soup), 'html.parser')
    for el in clone.select('script,style,nav,aside,footer,.related,.related-articles,.comments,#comments'):
        el.decompose()
    roots = clone.select('[itemprop="articleBody"], .article-body, .entry-body, #entry-body, .entry-more, #entry-more, .entry-content')
    if not roots:
        roots = clone.select('article')
    # A page without an identifiable article body is not a source for facts.
    return '\n'.join(x.get_text(' ',strip=True) for x in roots if not any(p in roots for p in x.parents))[:16000]

def headline(soup):
    meta = soup.select_one('meta[property="og:title"]')
    headings = soup.select('h1.entry-title, h2.entry-title, h3.entry-title, .article-title, h1')
    candidates = [meta.get('content','') if meta else '']+[h.get_text(' ',strip=True) for h in headings]+[soup.title.get_text(' ',strip=True) if soup.title else '']
    return next((x[:200] for x in candidates if x.strip()),'')

def published(soup):
    for el in soup.select('meta[property="article:published_time"],meta[name="date"],time[datetime]'):
        value = el.get('content') or el.get('datetime','')
        try: return date.fromisoformat(value[:10]).isoformat()
        except ValueError: pass
    for el in soup.select('.entry-date,.date,.posted,.entry-header'):
        m = re.search(r'(20\d{2})年\s*(\d{1,2})月\s*(\d{1,2})日',el.get_text(' ',strip=True))
        if m:
            try: return date(*map(int,m.groups())).isoformat()
            except ValueError: pass
    return ''

def canonical_address(value):
    value = unicodedata.normalize('NFKC',value)
    digits = dict(zip('一二三四五六七八九','123456789'))
    value = re.sub(r'[一二三四五六七八九](?=条|丁目)',lambda m:digits[m[0]],value)
    return norm(value)

def case_keys(text):
    text = unicodedata.normalize('NFKC',text)
    numbers = re.findall(r'(令和|平成)\s*(\d+)\s*年\s*\(\s*フ\s*\)\s*第?\s*(\d+)\s*号',text)
    courts = re.findall(r'([一-龥]{2,8})(?:地方裁判所|地裁)',text)
    return {(court,*number) for court in courts for number in numbers}

def match_report(row,title,body,source_body=''):
    name = normalized_name(row['company'])
    # The named subject must appear in the headline, not merely a related-story link.
    if len(name)<2 or name not in normalized_name(title): return ''
    if not re.search(r'破産|民事再生|特別清算',title): return ''
    if name not in normalized_name(body[:1800]): return ''
    number = row.get('corporate_number','')
    if number and re.search(r'(?<!\d)'+re.escape(number)+r'(?!\d)',body[:1800]):
        return '会社名・法人番号一致'
    address = row.get('address','')
    if address and not re.search(r'[*＊○〇…]',address):
        a = canonical_address(address)
        if len(re.findall(r'\d+',a))>=2 and a in canonical_address(body[:1800]):
            return '会社名・所在地一致'
    if source_body and len(case_keys(source_body))==1 and len(case_keys(body))==1 and case_keys(source_body) & case_keys(body):
        return '会社名・裁判所・事件番号一致'
    return ''

def facts(body):
    # Only short labels/names/numbers are retained. Never store article paragraphs.
    lead = unicodedata.normalize('NFKC',body[:2200])
    result = {}
    m = re.search(r'(?:代表取締役|代表者|社長)\s*[：:]?\s*([一-龥々ぁ-んァ-ヶ]{2,5}\s*[一-龥々ぁ-んァ-ヶ]{1,5})(?=[氏、,）)\s]|$)',lead)
    if m: result['representative'] = re.sub(r'\s+',' ',m[1]).strip()
    stores = re.findall(r'(?:レストラン|居酒屋|飲食店|店舗|ブランド|ホテル)\s*[「『]([^」』]{2,45})[」』]\s*(?:の)?(?:運営|経営|を運営|を経営)',lead)
    if stores: result['stores'] = '、'.join(dict.fromkeys(stores))[:100]
    for key,pattern in [('debt',r'負債(?:総額|額)?(?:は|が|:|：)?\s*((?:約|およそ)?[\d,.]+(?:億[\d,.]+万|億|万)?円)'),('employees',r'従業員(?:数)?(?:は|が|:|：)?\s*((?:約)?[\d,]+人)')]:
        m = re.search(pattern,lead)
        if m: result[key] = m[1]
    tags = []
    for label,pattern in [('デジタルマーケティング','デジタルマーケティング'),('広告事業','広告事業|広告代理|(?i:WEB)広告|広告運用'),('飲食店運営','居酒屋|レストラン|飲食店'),('宿泊施設運営','ホテル経営|旅館経営'),('建設業','建設業|土木工事'),('製造業','製造業'),('運送業','運送業|貨物運送'),('システム開発','システム開発')]:
        if re.search(pattern,lead): tags.append(label)
    if tags: result['business'] = '、'.join(tags)
    # Background terms are explicitly labeled as mentions, not causal conclusions.
    causes = [label for label,words in CAUSES.items() if any(word in lead for word in words)]
    if causes: result['causes'] = '、'.join(causes)
    return result

def parse_report(row,url,soup,source_body=''):
    title,body = headline(soup),article(soup)
    basis = match_report(row,title,body,source_body)
    if url==row.get('source_url') and normalized_name(row['company']) in normalized_name(title) and normalized_name(row['company']) in normalized_name(body[:1800]) and re.search('破産|民事再生|特別清算',title):
        basis='速報の出典記事'
    if not basis: return None
    day = published(soup) or (row['reported_date'] if basis=='速報の出典記事' else '')
    if day and abs((date.fromisoformat(day)-date.fromisoformat(row['reported_date'])).days)>60: return None
    return dict(url=public_url(url),title=title,publisher=urlsplit(url).hostname,
                published_date=day,checked_at=datetime.now(timezone.utc).isoformat(timespec='seconds'),
                match_basis=basis,fields=facts(title+'\n'+body))

def collect_reports(candidate,search_permitted=False):
    row = candidate['payload']; urls = list(LEADS.get(candidate['id'],[])); searched=False
    if search_permitted and os.getenv('BRAVE_SEARCH_API_KEY'):
        address=re.split(r'[*＊○〇…]',row.get('address',''))[0].strip()
        q = '"'+row['company']+'" '+(address or row.get('prefecture',''))+' 倒産 破産'
        r = requests.get('https://api.search.brave.com/res/v1/web/search',headers={'X-Subscription-Token':os.environ['BRAVE_SEARCH_API_KEY']},params={'q':q,'count':5,'country':'JP','search_lang':'jp'},timeout=15)
        r.raise_for_status(); searched=True
        urls += [x['url'] for x in r.json().get('web',{}).get('results',[]) if x.get('url')]
    fetcher = NewsFetcher(budget=65); source_body=''; reports=[]; errors=0
    try:
        final,soup = fetcher.page(row['source_url']); source_body = article(soup)
        primary=parse_report(row,final,soup)
        if primary and primary['fields']:reports.append(primary)
    except Exception: errors+=1
    for url in list(dict.fromkeys(urls))[:4]:
        if url==row['source_url']: continue
        try:
            final,soup = fetcher.page(url)
            item = parse_report(row,final,soup,source_body)
            if item: reports.append(item)
        except Exception: errors+=1
    return reports[:4],errors,searched

def validate_reports(reports):
    if not isinstance(reports,list) or len(reports)>4: raise ValueError('reports')
    for r in reports:
        public_url(r['url'])
        if len(r['url'])>2000: raise ValueError('url')
        for key,limit in [('title',200),('publisher',255),('checked_at',40),('published_date',10)]:
            if not isinstance(r[key],str) or len(r[key])>limit: raise ValueError(key)
        if r['published_date']: date.fromisoformat(r['published_date'])
        datetime.fromisoformat(r['checked_at'].replace('Z','+00:00'))
        if r['publisher']!=urlsplit(r['url']).hostname: raise ValueError('publisher')
        if r['match_basis'] not in {'速報の出典記事','会社名・法人番号一致','会社名・所在地一致','会社名・裁判所・事件番号一致'}: raise ValueError('match')
        if not isinstance(r['fields'],dict): raise ValueError('fields')
        for k,v in r['fields'].items():
            if k not in FIELDS or not isinstance(v,str) or len(v)>150: raise ValueError('field')
    return reports

def render_reports(reports):
    if not reports: return ''
    e = escape
    html = '<h2>関連する倒産ニュース</h2><p class="small">各記事の報道内容を整理しています。代表者・店舗との関係は報道時点の記載で、店舗の現在の営業状況を示すものではありません。</p>'
    for r in reports:
        html += '<section class="panel side"><h3><a class="source" target="_blank" rel="noopener noreferrer" href="'+e(r['url'],quote=True)+'">'+e(r['title'])+' ↗</a></h3>'
        html += '<p class="small">出典：'+e(r['publisher'])+' ｜ 掲載日：'+e(r['published_date'] or '未確認')+' ｜ 確認日：'+e(r['checked_at'][:10])+'</p>'
        html += '<dl class="facts">'+''.join('<dt>'+e(FIELDS[k])+'</dt><dd>'+e(v)+'</dd>' for k,v in r['fields'].items())+'</dl>'
        html += '<p class="small">照合：'+e(r['match_basis'])+'</p></section>'
    return html
