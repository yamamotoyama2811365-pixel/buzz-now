"""Small, asynchronous official-site enrichment. No search-page scraping."""
import ipaddress
import os
import re
import socket
import time
import unicodedata
from datetime import datetime, timezone
from urllib.parse import urljoin, urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser
import requests
from bs4 import BeautifulSoup
from .model import INDUSTRIES, normalized_name

UA='CorporateSignal/1.0 (+https://github.com/yamamotoyama2811365-pixel/-corporate-signal)'
BUSINESS=dict(INDUSTRIES)
BUSINESS['飲食業']=BUSINESS['飲食業']+['割烹','仕出し','ケータリング']
BUSINESS['運輸業']=BUSINESS['運輸業']+['物流業務']
BLOCKED={'n-seikei.jp','google.com','google.co.jp','facebook.com','instagram.com','x.com','twitter.com','youtube.com','tabelog.com','houjin-bangou.nta.go.jp'}

def public_url(url):
    p=urlsplit(url)
    if p.scheme not in {'http','https'} or not p.hostname or p.username or p.password or p.port not in {None,80,443}:raise ValueError('unsafe URL')
    host=p.hostname.lower()
    if host=='localhost' or '.' not in host:raise ValueError('unsafe host')
    try:
        if not ipaddress.ip_address(host).is_global:raise ValueError('private address')
    except ValueError:
        if re.fullmatch(r'[0-9.:]+',host):raise ValueError('invalid address')
    return urlunsplit((p.scheme,p.netloc,p.path or '/',p.query,''))

def candidate_url(url):
    url=public_url(url);host=urlsplit(url).hostname
    if any(host==h or host.endswith('.'+h) for h in BLOCKED):raise ValueError('not an official-site candidate')
    return url

def norm(text):
    text=unicodedata.normalize('NFKC',text).casefold()
    text=re.sub(r'(\d+)丁目',r'\1-',text)
    text=re.sub(r'(\d+)番地?(?:の)?',r'\1-',text)
    text=re.sub(r'(\d+)号',r'\1',text)
    return re.sub(r'[\s〒ー−‐–—・,、。]','',text)

def article_candidates(soup,source_url):
    """Only explicit official-site links inside the article, never ads/footer."""
    result=[]
    roots=soup.select('.entry-body, #entry-body, .entry-more, .article-body, .entry-content, [itemprop="articleBody"]')
    for root in roots:
        for a in root.select('a[href]'):
            label=a.get_text(' ',strip=True)
            context=a.parent.get_text(' ',strip=True) if a.parent else label
            if not re.search(r'公式(?:サイト|ホームページ|HP)|同社(?:サイト|ホームページ|HP)',label+' '+context[:180],re.I):continue
            try:url=candidate_url(urljoin(source_url,a['href']))
            except ValueError:continue
            if url not in result:result.append(url)
    return result[:3]

class Fetcher:
    def __init__(self,budget=45):
        self.s=requests.Session();self.s.trust_env=False
        self.s.headers.update({'User-Agent':UA})
        self.deadline=time.monotonic()+budget;self.robots={};self.last={};self.calls=0
    def raw(self,url):
        for _ in range(4):
            if time.monotonic()>self.deadline or self.calls>=12:raise TimeoutError('crawl budget')
            url=public_url(url);p=urlsplit(url)
            addresses=socket.getaddrinfo(p.hostname,p.port or (443 if p.scheme=='https' else 80),type=socket.SOCK_STREAM)
            if not addresses or any(not ipaddress.ip_address(x[4][0]).is_global for x in addresses):raise ValueError('private DNS')
            delay=1.0-(time.monotonic()-self.last.get(p.hostname,0))
            if delay>0:time.sleep(delay)
            self.last[p.hostname]=time.monotonic();self.calls+=1
            with self.s.get(url,timeout=(5,10),allow_redirects=False,stream=True) as r:
                if r.status_code in {301,302,303,307,308}:
                    # Redirect targets must pass their own robots check when HTML is fetched.
                    return r.status_code,'',urljoin(url,r.headers.get('Location',''))
                if r.status_code==404:return 404,'',url
                r.raise_for_status();data=bytearray()
                for chunk in r.iter_content(16384):
                    data.extend(chunk)
                    if len(data)>1_500_000 or time.monotonic()>self.deadline:raise ValueError('response limit')
                encoding=r.encoding if r.encoding and r.encoding.lower()!='iso-8859-1' else None
                if not encoding:
                    m=re.search(rb'charset=["\s\x27]*([a-zA-Z0-9_-]+)',bytes(data[:5000]))
                    encoding=m[1].decode() if m else 'utf-8'
                return r.status_code,bytes(data).decode(encoding,errors='replace'),url
        raise ValueError('redirect limit')
    def allowed(self,url):
        p=urlsplit(url);origin=p.scheme+'://'+p.netloc
        if origin not in self.robots:
            rp=RobotFileParser();status,text,_=self.raw(origin+'/robots.txt')
            # Missing robots means no rules. Unresolved redirects/errors defer collection.
            if status==404:rp.parse([])
            elif status==200:rp.parse(text.splitlines())
            else:raise ValueError('robots unavailable')
            self.robots[origin]=rp
        delay=self.robots[origin].crawl_delay(UA) or self.robots[origin].crawl_delay('*') or 0
        if delay>5:raise ValueError('crawl delay exceeds budget')
        if delay:time.sleep(delay)
        return self.robots[origin].can_fetch(UA,url)
    def page(self,url):
        for _ in range(4):
            url=candidate_url(url)
            if not self.allowed(url):raise ValueError('robots disallow')
            status,text,next_url=self.raw(url)
            if status in {301,302,303,307,308}:url=next_url;continue
            if status!=200:raise ValueError('page unavailable')
            return next_url,BeautifulSoup(text,'html.parser')
        raise ValueError('redirect limit')

def page_text(soup):
    clone=BeautifulSoup(str(soup),'html.parser')
    for n in clone.select('script,style,nav,aside'):n.decompose()
    return clone.get_text(' ',strip=True)[:24000]

def matching_basis(row,text,explicit=False):
    name=normalized_name(row['company'])
    if len(name)<2 or name not in normalized_name(text):return ''
    number=row.get('corporate_number','')
    if number and re.search(r'(?<!\d)'+re.escape(number)+r'(?!\d)',text):return '会社名・法人番号一致'
    address=norm(row.get('address',''))
    # A full street address must match; a prefecture/city alone is insufficient.
    if address and re.search(r'\d',address) and address in norm(text):return '会社名・所在地一致'
    # An explicit link in the cited report supplies independent attribution.
    pref=row.get('prefecture','')
    if explicit and pref and norm(pref) in norm(text):return '報道元の公式サイトリンク・会社名・都道府県一致'
    return ''

def profile_from_pages(row,pages,explicit=False):
    text=' '.join(page_text(soup) for _,soup in pages)
    basis=matching_basis(row,text,explicit)
    if not basis:return None
    # Use business sections instead of classifying every word on a company page.
    sections=[]
    for url,soup in pages:
        for el in soup.find_all(['h1','h2','h3','h4','dt','th','td']):
            label=el.get_text(' ',strip=True)
            if re.fullmatch(r'事業内容|事業案内|業務内容|サービス内容',re.sub(r'\s','',label)):
                if el.name in {'dt','th','td'}:
                    nxt=el.find_next_sibling(['dd','td']);section=nxt.get_text(' ',strip=True) if nxt else ''
                else:
                    parts=[]
                    for nxt in el.next_siblings:
                        if getattr(nxt,'name',None) in {'h1','h2','h3','h4'} and parts:break
                        parts.append(nxt.get_text(' ',strip=True) if hasattr(nxt,'get_text') else str(nxt))
                        if len(' '.join(parts))>800:break
                    section=' '.join(parts)
                if section:sections.append(section[:1200])
    # Titles describe the site's own offering, unlike footer/navigation keywords.
    titles=' '.join(soup.title.get_text(' ',strip=True) for _,soup in pages if soup.title)
    business=' '.join(sections) or titles
    industries=[k for k,words in BUSINESS.items() if any(w in business for w in words)]
    # Store short, original category labels, not copied website paragraphs.
    descriptions=[]
    for label,terms in [('割烹・日本料理',['割烹','日本料理']),('仕出し・ケータリング',['仕出し','ケータリング']),('物流業務の受託',['物流業務','物流アウトソーシング']),('卸売',['卸売']),('通信販売',['通信販売']),('システム開発',['システム開発','ソフトウェア開発'])]:
        if any(t in business for t in terms):descriptions.append(label)
    return dict(website_url=pages[0][0],evidence_urls=list(dict.fromkeys(url for url,_ in pages)),industries=industries,primary_industry=industries[0] if len(industries)==1 else '',business_tags=descriptions,match_basis=basis,checked_at=datetime.now(timezone.utc).isoformat(timespec='seconds'))

def search_candidates(row):
    key=os.getenv('BRAVE_SEARCH_API_KEY','')
    if not key:return []
    # Optional official search API. Never scrape search result pages or print the key.
    response=requests.get('https://api.search.brave.com/res/v1/web/search',headers={'X-Subscription-Token':key},params={'q':row['company']+' '+row.get('address','')+' 会社概要','count':5,'country':'JP','search_lang':'jp'},timeout=15)
    response.raise_for_status()
    urls=[]
    for item in response.json().get('web',{}).get('results',[]):
        try:urls.append(candidate_url(item['url']))
        except (KeyError,ValueError):pass
    return urls[:3]

def enrich(row):
    candidates=row.get('website_candidates',[])[:3]
    explicit=bool(candidates)
    if not candidates:candidates=search_candidates(row)
    fetcher=Fetcher()
    for url in candidates:
        try:
            root,soup=fetcher.page(url);pages=[(root,soup)]
            links=[]
            for a in soup.select('a[href]'):
                if not re.search('会社概要|企業情報|事業内容|事業案内|会社案内|店舗情報',a.get_text(' ',strip=True)):continue
                dest=urljoin(root,a['href'])
                if urlsplit(dest).hostname==urlsplit(root).hostname and dest!=root and dest not in links:links.append(dest)
            for dest in links[:2]:
                try:pages.append(fetcher.page(dest))
                except Exception:continue
            if not explicit:
                # A directory's matching listing is not proof that it is the official site.
                own_name=normalized_name(row['company'])
                footers=[n.get_text(' ',strip=True) for _,page in pages for n in page.select('footer, #footer, .footer')]
                if not any(own_name in normalized_name(t) and re.search(r'copyright|©|著作権',t,re.I) for t in footers):continue
            result=profile_from_pages(row,pages,explicit)
            if result:return result
        except Exception:continue
    return None
