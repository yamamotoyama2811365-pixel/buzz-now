import re
from difflib import SequenceMatcher
from urllib.parse import quote_plus, urljoin, urlparse
import httpx
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent":"Mozilla/5.0 (compatible; OpenCloseMap/1.0; +https://open-close-map.onrender.com)",
    "Accept-Language":"ja,en;q=0.8",
}

def clean_news_title(title: str):
    if not title:
        return ""
    t = re.sub(r'\s*[-｜|]\s*PR TIMES\s*$', '', title, flags=re.I)
    t = re.sub(r'\s*[-｜|]\s*[^-｜|]{1,60}$', '', t)
    return re.sub(r'\s+', ' ', t).strip()

def compact(s: str):
    return re.sub(r'[\s　「」『』【】\[\]（）()・,，。.!！?？:：\-ー_｜|]', '', s or '').lower()

def similarity(a: str, b: str):
    aa, bb = compact(a), compact(b)
    if not aa or not bb:
        return 0.0
    if aa in bb or bb in aa:
        return 0.93
    return SequenceMatcher(None, aa, bb).ratio()

def title_queries(title):
    target = clean_news_title(title)
    qs = []

    # Quoted store names are usually the best search key.
    for pat in [r'「([^」]{3,70})」', r'『([^』]{3,70})』']:
        for m in re.findall(pat, target):
            if m not in qs:
                qs.append(m)

    # Distinctive trailing phrase near "店"
    m = re.search(r'([A-Za-z0-9一-龥ぁ-んァ-ヶー・＆&\s]{4,60}店)', target)
    if m:
        v = re.sub(r'\s+',' ',m.group(1)).strip()
        if v not in qs:
            qs.append(v)

    # progressively shorter headline queries
    for n in (48, 34, 24):
        q = target[:n].strip()
        if q and q not in qs:
            qs.append(q)

    return qs[:6]

def _collect_links(html, base_url, target, href_filter=None):
    soup = BeautifulSoup(html[:2_000_000], "html.parser")
    candidates=[]
    seen=set()

    for a in soup.find_all("a", href=True):
        href=a.get("href","")
        full=urljoin(base_url,href)
        if href_filter and not href_filter(full):
            continue
        if full in seen:
            continue
        seen.add(full)

        txt=re.sub(r'\s+',' ',a.get_text(" ",strip=True))
        if not txt:
            parent=a.parent
            txt=re.sub(r'\s+',' ',parent.get_text(" ",strip=True)) if parent else ""

        score=similarity(target,txt)
        if score > 0.2:
            candidates.append({
                "url":full,
                "title":txt[:260],
                "score":round(score,4)
            })

    candidates.sort(key=lambda x:x["score"], reverse=True)
    return candidates

def prtimes_search(title: str):
    target=clean_news_title(title)
    if not target:
        return {"ok":False,"reason":"empty_title"}

    attempts=[]
    best_all=[]

    with httpx.Client(follow_redirects=True,timeout=12.0,headers=HEADERS) as client:
        for q in title_queries(title):
            url="https://prtimes.jp/main/action.php?page=searchkey&search_word="+quote_plus(q)
            try:
                r=client.get(url)
                attempts.append({"query":q,"status":r.status_code})
                if r.status_code != 200:
                    continue

                candidates=_collect_links(
                    r.text,
                    "https://prtimes.jp",
                    target,
                    href_filter=lambda u:"/main/html/rd/p/" in u
                )
                if candidates:
                    best_all.extend(candidates[:5])
                    best_all.sort(key=lambda x:x["score"],reverse=True)
                    if best_all[0]["score"] >= 0.68:
                        break
            except Exception as e:
                attempts.append({"query":q,"error":str(e)[:120]})

    if not best_all:
        return {"ok":False,"reason":"no_candidates","attempts":attempts}

    # de-dupe
    uniq={}
    for c in best_all:
        if c["url"] not in uniq or c["score"] > uniq[c["url"]]["score"]:
            uniq[c["url"]]=c
    best=sorted(uniq.values(),key=lambda x:x["score"],reverse=True)[0]

    if best["score"] < 0.50:
        return {
            "ok":False,
            "reason":"low_similarity",
            "best":best,
            "attempts":attempts
        }

    return {
        "ok":True,
        "method":"prtimes_search_multi",
        "url":best["url"],
        "matched_title":best["title"],
        "score":best["score"],
        "attempts":attempts
    }

def generic_recent_site_search(title, base_urls, href_filter=None, method="site_recent"):
    target=clean_news_title(title)
    all_candidates=[]
    attempts=[]

    with httpx.Client(follow_redirects=True,timeout=12.0,headers=HEADERS) as client:
        for url in base_urls:
            try:
                r=client.get(url)
                attempts.append({"url":url,"status":r.status_code})
                if r.status_code != 200:
                    continue
                candidates=_collect_links(r.text,url,target,href_filter=href_filter)
                all_candidates.extend(candidates[:8])
            except Exception as e:
                attempts.append({"url":url,"error":str(e)[:120]})

    if not all_candidates:
        return {"ok":False,"reason":"no_candidates","attempts":attempts}

    uniq={}
    for c in all_candidates:
        if c["url"] not in uniq or c["score"] > uniq[c["url"]]["score"]:
            uniq[c["url"]]=c
    best=sorted(uniq.values(),key=lambda x:x["score"],reverse=True)[0]

    if best["score"] < 0.45:
        return {"ok":False,"reason":"low_similarity","best":best,"attempts":attempts}

    return {
        "ok":True,
        "method":method,
        "url":best["url"],
        "matched_title":best["title"],
        "score":best["score"],
        "attempts":attempts
    }

def goguynet_search(title, publisher):
    # Publisher often comes as atsugi.goguynet.jp
    domain=(publisher or "").strip().lower()
    if "goguynet.jp" not in domain:
        domain="www.goguynet.jp"
    if not domain.startswith("http"):
        base="https://"+domain.rstrip("/")+"/"
    else:
        base=domain.rstrip("/")+"/"

    q=title_queries(title)[0] if title_queries(title) else clean_news_title(title)[:30]
    urls=[
        base,
        base+"?s="+quote_plus(q)
    ]
    return generic_recent_site_search(
        title,urls,
        href_filter=lambda u:"goguynet.jp/" in u and "/202" in u,
        method="goguynet_recent"
    )

def gatachira_search(title):
    q=title_queries(title)[0] if title_queries(title) else clean_news_title(title)[:30]
    urls=[
        "https://gatachira.com/",
        "https://gatachira.com/local_cat/open/",
        "https://gatachira.com/local_cat/open-close/",
        "https://gatachira.com/?s="+quote_plus(q)
    ]
    return generic_recent_site_search(
        title,urls,
        href_filter=lambda u:"gatachira.com/" in u and "/local/" in u,
        method="gatachira_recent"
    )

def ryutsuu_search(title):
    q=title_queries(title)[0] if title_queries(title) else clean_news_title(title)[:30]
    urls=[
        "https://www.ryutsuu.biz/",
        "https://www.ryutsuu.biz/news/",
        "https://www.ryutsuu.biz/?s="+quote_plus(q)
    ]
    return generic_recent_site_search(
        title,urls,
        href_filter=lambda u:"ryutsuu.biz/" in u and u.count("/") >= 4,
        method="ryutsuu_recent"
    )

def relocate_source(title: str, source_name: str = "", source_url: str = ""):
    hay=f"{title or ''} {source_name or ''}"
    upper=hay.upper()

    if "PR TIMES" in upper:
        return prtimes_search(title)

    if "GOGUYNET.JP" in upper or "号外NET" in hay:
        return goguynet_search(title,source_name)

    if "ガタチラ" in hay or "GATACHIRA" in upper:
        return gatachira_search(title)

    if "流通ニュース" in hay or "RYUTSUU" in upper:
        return ryutsuu_search(title)

    return {
        "ok":False,
        "reason":"unsupported_publisher",
        "publisher":source_name,
        "source_url":source_url
    }

