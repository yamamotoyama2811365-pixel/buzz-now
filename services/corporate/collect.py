"""Bounded HTTP collection runs on GitHub Actions, outside the 512MB API server."""
import io
import hashlib
import re
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
import requests
from bs4 import BeautifulSoup
from .model import parse_news, parse_registration
UA='CorporateSignal/1.0 (+https://github.com/yamamotoyama2811365-pixel/-corporate-signal)'
NTA='https://www.houjin-bangou.nta.go.jp/download/sabun/'

def session():
    s=requests.Session(); s.headers.update({'User-Agent':UA}); return s

def get(s,url,**kwargs):
    r=s.get(url,timeout=(10,30),**kwargs); r.raise_for_status()
    if len(r.content)>15_000_000: raise ValueError('source too large')
    r.encoding='utf-8'; return r

def collect_news(known=None):
    known=known or {}
    s=session()
    robots=get(s,'https://n-seikei.jp/robots.txt').text
    rp=RobotFileParser(); rp.parse(robots.splitlines())
    feedurl='https://n-seikei.jp/rss.xml'
    if not rp.can_fetch(UA,feedurl): raise RuntimeError('source robots disallows feed')
    root=ET.fromstring(get(s,feedurl).content)
    rows=Collection()
    candidates={}
    # RSS may be stale: current homepage is an independent freshness source.
    home=BeautifulSoup(get(s,'https://n-seikei.jp/').text,'html.parser')
    for link in home.select('a[href]'):
        title=link.get_text(' ',strip=True); url=urljoin('https://n-seikei.jp/',link['href'])
        if re.search(r'/20\d{2}/\d{2}/',url) and re.search('破産|民事再生|特別清算',title):
            if len(title)>len(candidates.get(url,('',None,''))[0]): candidates[url]=(title,None,'')
    items=root.findall('.//item') or root.findall('.//{http://purl.org/rss/1.0/}item')
    if not items and not candidates: raise ValueError('No news candidates')
    for item in items[:80]:
        def value(tag):
            return item.findtext(tag) or item.findtext('{http://purl.org/rss/1.0/}'+tag) or ''
        title=value('title'); url=value('link')
        pub=value('pubDate') or item.findtext('{http://purl.org/dc/elements/1.1/}date')
        candidates.setdefault(url,(title,pub,value('description')))
    for url,(title,pub,description) in list(candidates.items())[:70]:
        if urlparse(url).hostname not in {'n-seikei.jp','www.n-seikei.jp'} or not rp.can_fetch(UA,url): continue
        if not re.search('破産|民事再生|特別清算',title) or '一覧' in title: continue
        fingerprint='v2:'+hashlib.sha256(title.encode()).hexdigest()
        if known.get(url)==fingerprint:
            rows.skipped+=1
            continue
        time.sleep(1)
        soup=BeautifulSoup(get(s,url).text,'html.parser')
        # Prefer the full article title to truncated list labels.
        h=soup.select_one('h1.entry-title') or soup.select_one('h1')
        if h and re.search('破産|民事再生|特別清算',h.get_text()): title=h.get_text(' ',strip=True)
        meta=soup.select_one('meta[property="article:published_time"]') or soup.select_one('meta[name="date"]')
        if meta: pub=meta.get('content') or pub
        if not pub:
            time_node=soup.select_one('time[datetime]') or soup.select_one('.published[title]')
            if time_node: pub=time_node.get('datetime') or time_node.get('title')
        if not pub:
            # Visible Japanese publication date, not the collection time.
            m=re.search(r'\[\s*(20\d{2})年\s*(\d{1,2})月\s*(\d{1,2})日',soup.get_text(' ',strip=True))
            if m: pub=f'{m[1]}-{int(m[2]):02d}-{int(m[3]):02d}'
        if not pub: continue
        try: dt=parsedate_to_datetime(pub)
        except ValueError: dt=datetime.fromisoformat(pub.replace('Z','+00:00'))
        body=soup.select_one('.entry-body') or soup.select_one('#entry-body') or soup.select_one('.article-body') or soup.select_one('[itemprop="articleBody"]') or soup.select_one('.entry-content')
        if body:
            for n in body.select('script,style,aside,nav'): n.decompose()
            description=body.get_text(' ',strip=True)[:7000]
        else:
            meta=soup.select_one('meta[name="description"]')
            description=meta.get('content','') if meta else BeautifulSoup(description,'html.parser').get_text(' ',strip=True)
        metadata=soup.select_one('meta[name="description"]')
        if metadata:description=metadata.get('content','')+' '+description
        more=soup.select_one('.entry-more')
        if more:description+=' '+more.get_text(' ',strip=True)[:6000]
        company_hint=''
        for tr in soup.select('tr'):
            cells=tr.find_all(['th','td'],recursive=False)
            if len(cells)==2 and cells[0].get_text(strip=True) in {'法人名','会社名','商号','企業名'}:
                value=cells[1].get_text(' ',strip=True)
                if 2<=len(value)<=100:company_hint=value;break
        row=parse_news(title,description,url,dt.date().isoformat(),company_hint)
        if row:
            row["listing_fingerprint"]=fingerprint
            rows.append(row)
        if len(rows)>=35: break
    return rows

class Collection(list):
    def __init__(self):
        super().__init__(); self.updates=[]; self.skipped=0; self.file_ids=[]

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
