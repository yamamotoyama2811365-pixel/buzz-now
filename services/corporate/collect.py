"""Bounded HTTP collection runs on GitHub Actions, outside the 512MB API server."""
import io
import hashlib
import re
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import requests
from bs4 import BeautifulSoup
from .model import parse_news, parse_registration
from .enrich import article_candidates
UA='CorporateSignal/1.0 (+https://github.com/yamamotoyama2811365-pixel/-corporate-signal)'
NTA='https://www.houjin-bangou.nta.go.jp/download/sabun/'

def session():
    s=requests.Session(); s.headers.update({'User-Agent':UA}); return s

def get(s,url,**kwargs):
    r=s.get(url,timeout=(10,30),**kwargs); r.raise_for_status()
    if len(r.content)>15_000_000: raise ValueError('source too large')
    r.encoding='utf-8'; return r

NEWS_WORDS=re.compile('破産|民事再生|特別清算')

def publication_date(soup,fallback=None):
    node=soup.select_one('meta[property="article:published_time"],meta[name="date"]')
    if node:fallback=node.get('content') or fallback
    if not fallback:
        node=soup.select_one('time[datetime],.published[title]')
        if node:fallback=node.get('datetime') or node.get('title')
    if not fallback:
        m=re.search(r'\[\s*(20\d{2})年\s*(\d{1,2})月\s*(\d{1,2})日',soup.get_text(' ',strip=True))
        if m:return f'{m[1]}-{int(m[2]):02d}-{int(m[3]):02d}'
    if fallback:
        try:return parsedate_to_datetime(fallback).date().isoformat()
        except (ValueError,TypeError):return datetime.fromisoformat(fallback.replace('Z','+00:00')).date().isoformat()
    return ''

def article_facts(soup,url,title,published):
    heading=soup.select_one('h1.entry-title') or soup.select_one('h1')
    if heading and NEWS_WORDS.search(heading.get_text()):title=heading.get_text(' ',strip=True)
    roots=soup.select('.entry-body,#entry-body,.article-body,[itemprop="articleBody"],.entry-content,.entry-more')
    for root in roots:
        for n in root.select('script,style,aside,nav'):n.decompose()
    description=' '.join(root.get_text(' ',strip=True) for root in roots)[:12000]
    metadata=soup.select_one('meta[name="description"]')
    if metadata:description=metadata.get('content','')+' '+description
    company_hint='';address=''
    # Only read facts inside the article, excluding recommendations and advertising.
    for root in roots:
        for tr in root.select('tr'):
            cells=tr.find_all(['th','td'],recursive=False)
            if len(cells)!=2:continue
            label=re.sub(r'\s','',cells[0].get_text(strip=True));value=cells[1].get_text(' ',strip=True)
            if label in {'法人名','会社名','商号','企業名'} and 2<=len(value)<=100:company_hint=value
            if label in {'所在地','住所','本社所在地'} and len(value)<=300:address=value
    row=parse_news(title,description,url,published,company_hint)
    if not row:return None
    if not address:
        m=re.search(r'「[^」]+」は[（(]([^）)]+)[）)]に所在',description)
        if m:address=re.split('、|,|法人番号|登記記録上|商業登記',m[1])[0].strip()
    row['address']=address[:300]
    # Do not attach one company's number to a multi-company report.
    numbers=set(re.findall(r'法人番号[：:\s]*([0-9]{13})(?![0-9])',description))
    if len(numbers)==1:row['corporate_number']=next(iter(numbers))
    row['website_candidates']=article_candidates(soup,url)
    return row

def collect_news(known=None):
    known=known or {};s=session();rows=Collection()
    rp=RobotFileParser();rp.parse(get(s,'https://n-seikei.jp/robots.txt').text.splitlines())
    candidates={};seen_pages=set();daily=[];last_request=0
    today=datetime.now(timezone(timedelta(hours=9))).date();cutoff=today-timedelta(days=29)
    def read(url):
        nonlocal last_request
        if urlparse(url).hostname not in {'n-seikei.jp','www.n-seikei.jp'} or not rp.can_fetch(UA,url):raise ValueError('source not allowed')
        delay=1.0-(time.monotonic()-last_request)
        if delay>0:time.sleep(delay)
        last_request=time.monotonic()
        return get(s,url)
    def links(soup,page):
        for link in soup.select('a[href]'):
            title=link.get_text(' ',strip=True);url=urljoin(page,link['href']).split('#')[0]
            if urlparse(url).hostname not in {'n-seikei.jp','www.n-seikei.jp'}:continue
            daily_match=re.search(r'/(20\d{2})/(\d{2})/(20\d{2})-(\d{2})(\d{2})-tousan\.html$',url)
            if daily_match:
                day=f'{daily_match[3]}-{daily_match[4]}-{daily_match[5]}'
                if cutoff.isoformat()<=day<=today.isoformat() and url not in daily:daily.append(url)
                continue
            if not re.search(r'/20\d{2}/\d{2}/[^/]+\.html$',url) or not NEWS_WORDS.search(title):continue
            container=link.find_parent(class_=re.compile(r'\b(?:entry|hentry|asset)\b'))
            listed_date=publication_date(container) if container else ''
            if listed_date and listed_date<cutoff.isoformat():continue
            # Date summaries are discovery indexes, never a company record.
            if '一覧' in title:
                if re.search('破産・小口倒産一覧',title) and url not in daily:daily.append(url)
                continue
            if re.search('倒産件数|過去最多|ランキング|予測',title):continue
            if len(title)>len(candidates.get(url,('',None,''))[0]):candidates[url]=(title,listed_date or None,'')
    pages=['https://n-seikei.jp/','https://n-seikei.jp/tousan/','https://n-seikei.jp/koguchi-hasan/']
    for url in pages:
        try:links(BeautifulSoup(read(url).text,'html.parser'),url);seen_pages.add(url)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code==429:raise
            rows.errors.append({'url':url,'reason':type(exc).__name__,'status':getattr(getattr(exc,'response',None),'status_code',None)})
        except Exception as exc:rows.errors.append({'url':url,'reason':type(exc).__name__,'status':getattr(getattr(exc,'response',None),'status_code',None)})
    # Read daily indexes to recover small-company articles omitted from the homepage.
    for index,url in enumerate(daily):
        if index>=35:break
        if url in seen_pages:continue
        try:links(BeautifulSoup(read(url).text,'html.parser'),url);seen_pages.add(url)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code==429:raise
            rows.errors.append({'url':url,'reason':type(exc).__name__,'status':getattr(getattr(exc,'response',None),'status_code',None)})
        except Exception as exc:rows.errors.append({'url':url,'reason':type(exc).__name__,'status':getattr(getattr(exc,'response',None),'status_code',None)})
    try:
        root=ET.fromstring(read('https://n-seikei.jp/rss.xml').content)
        items=root.findall('.//item') or root.findall('.//{http://purl.org/rss/1.0/}item')
        for item in items:
            value=lambda tag:item.findtext(tag) or item.findtext('{http://purl.org/rss/1.0/}'+tag) or ''
            title=value('title');url=value('link');pub=value('pubDate') or item.findtext('{http://purl.org/dc/elements/1.1/}date')
            if NEWS_WORDS.search(title) and '一覧' not in title:candidates.setdefault(url,(title,pub,''))
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code==429:raise
        rows.errors.append({'url':'rss.xml','reason':type(exc).__name__,'status':getattr(getattr(exc,'response',None),'status_code',None)})
    except Exception as exc:rows.errors.append({'url':'rss.xml','reason':type(exc).__name__,'status':getattr(getattr(exc,'response',None),'status_code',None)})
    if not candidates:raise ValueError('No news candidates')
    rows.candidates=len(candidates);attempted=0;deadline=time.monotonic()+420
    # Iterate ALL discovered URLs. The work budget defers overflow to subsequent runs.
    for url,(title,pub,_) in candidates.items():
        if pub:
            try:
                listed=publication_date(BeautifulSoup('','html.parser'),pub)
                if listed<cutoff.isoformat():rows.outside_window+=1;continue
            except (ValueError,TypeError):pass
        fingerprint='v4:'+hashlib.sha256(title.encode()).hexdigest()
        if known.get(url)==fingerprint:rows.skipped+=1;continue
        if attempted>=160 or time.monotonic()>deadline:rows.deferred+=1;continue
        attempted+=1
        try:
            soup=BeautifulSoup(read(url).text,'html.parser');published=publication_date(soup,pub)
            if not published:raise ValueError('missing publication date')
            if published<cutoff.isoformat() or published>today.isoformat():
                rows.outside_window+=1;continue
            row=article_facts(soup,url,title,published)
            if row:
                row['listing_fingerprint']=fingerprint;rows.append(row)
            else:rows.rejected.append({'url':url,'title':title})
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code==429:raise
            rows.errors.append({'url':url,'reason':type(exc).__name__,'status':getattr(getattr(exc,'response',None),'status_code',None)})
        except Exception as exc:rows.errors.append({'url':url,'reason':type(exc).__name__,'status':getattr(getattr(exc,'response',None),'status_code',None)})
    return rows

class Collection(list):
    def __init__(self):
        super().__init__(); self.updates=[]; self.skipped=0; self.file_ids=[]; self.errors=[]; self.rejected=[]; self.candidates=0; self.deferred=0; self.outside_window=0

def collect_registrations(known_files=None):
    known_files=set(known_files or [])
    s=session(); page=BeautifulSoup(get(s,NTA).text,'html.parser')
    form=page.select_one('form#appForm')
    if not form: raise ValueError('NTA download form changed')
    heading=form.select_one('#xml') or form.find(id=re.compile('xml'))
    if not heading: raise ValueError('NTA XML section missing')
    table=heading.find_next('table')
    links=table.select('a[onclick]')[:3]
    if not links: raise ValueError('NTA download list missing')
    rows=Collection()
    for link in links:
        file_id=re.search(r'doDownload\((\d+)\)',link['onclick']).group(1)
        rows.file_ids.append(file_id)
        if file_id in known_files:
            rows.skipped+=1
            continue
        fields={n['name']:n.get('value','') for n in form.select('input[name]')}
        fields.update(event='download',selDlFileNo=re.search(r'doDownload\((\d+)\)',link['onclick']).group(1))
        response=s.post(urljoin(NTA,form.get('action')),data=fields,timeout=(10,45)); response.raise_for_status()
        if len(response.content)>15_000_000: raise ValueError('NTA archive too large')
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            members=[m for m in archive.infolist() if m.filename.lower().endswith('.xml')]
            if not members: raise ValueError('No XML in NTA archive')
            for member in members:
                if member.file_size>40_000_000: raise ValueError('NTA XML too large')
                with archive.open(member) as fh:
                    for _,node in ET.iterparse(fh,events=('end',)):
                        if node.tag=='corporation':
                            getfield=lambda key:(node.findtext(key) or '').strip()
                            number=getfield('corporateNumber')
                            if re.fullmatch(r'\d{13}',number) and getfield('latest')!='0':
                                rows.updates.append(dict(corporate_number=number,company=getfield('name'),prefecture=getfield('prefectureName'),address=getfield('prefectureName')+getfield('cityName')+getfield('streetNumber'),published=not (getfield('hihyoji')=='1' or getfield('process')=='99' or getfield('closeDate'))))
                            row=parse_registration(node)
                            if row: rows.append(row)
                            node.clear()
        # Fresh token/cookie state for the next normal form submission.
        page=BeautifulSoup(get(s,NTA).text,'html.parser'); form=page.select_one('form#appForm')
    return rows
