import json
import re
import httpx
from bs4 import BeautifulSoup
from .news_resolver import resolve_google_news_url
from .text_rules import (
    extract_address,extract_facility_name,extract_floor,extract_postal_code,
    detect_prefecture,detect_city,norm,address_matches_expected_prefecture,
    NEGATIVE_ADDRESS_LABELS,is_plausible_street_address,extract_best_store_name_from_title
)

HEADERS={
    "User-Agent":"Mozilla/5.0 (compatible; OpenCloseMap/1.0; +https://open-close-map.onrender.com)",
    "Accept-Language":"ja,en;q=0.8",
}

def _jsonld_candidates(soup):
    out=[]
    for tag in soup.find_all("script",attrs={"type":"application/ld+json"}):
        raw=tag.string or tag.get_text(" ",strip=True)
        if not raw:continue
        try:out.append(json.loads(raw))
        except Exception:continue
    return out

def _walk_jsonld(obj,found):
    if isinstance(obj,dict):
        addr=obj.get("address")
        name=obj.get("name")
        typ=obj.get("@type")

        if isinstance(addr,dict):
            pieces=[
                addr.get("postalCode"),addr.get("addressRegion"),
                addr.get("addressLocality"),addr.get("streetAddress")
            ]
            value=" ".join(str(x) for x in pieces if x)
            if value:
                found.append({
                    "address":value,
                    "name":str(name) if name else None,
                    "type":typ
                })
        elif isinstance(addr,str):
            found.append({
                "address":addr,
                "name":str(name) if name else None,
                "type":typ
            })

        for v in obj.values():_walk_jsonld(v,found)

    elif isinstance(obj,list):
        for x in obj:_walk_jsonld(x,found)

def _table_pairs(soup):
    pairs=[]
    for tr in soup.find_all("tr"):
        cells=tr.find_all(["th","td"])
        if len(cells)>=2:
            key=norm(cells[0].get_text(" ",strip=True)).strip("［］【】[]")
            val=norm(" ".join(c.get_text(" ",strip=True) for c in cells[1:]))
            if key and val:pairs.append((key,val))

    for dl in soup.find_all("dl"):
        for dt in dl.find_all("dt"):
            key=norm(dt.get_text(" ",strip=True)).strip("［］【】[]")
            dd=dt.find_next_sibling("dd")
            if dd:
                val=norm(dd.get_text(" ",strip=True))
                if key and val:pairs.append((key,val))
    return pairs

def _extract_same_block(text, expected_prefecture):
    """
    Extract address/postal/floor from same short block.
    """
    lines=[norm(x) for x in text.replace('\r','\n').split('\n') if norm(x)]

    for i,line in enumerate(lines):
        window="\n".join(lines[max(0,i-1):min(len(lines),i+3)])
        if any(k in window for k in NEGATIVE_ADDRESS_LABELS):
            continue

        if expected_prefecture and expected_prefecture in window:
            addr=extract_address(window,expected_prefecture)
            if addr:
                return {
                    "address":addr,
                    "postal_code":extract_postal_code(window),
                    "floor":extract_floor(window),
                    "block":window
                }
    return None

def fetch_article_facts(url, expected_prefecture=None, expected_store_name=None):
    result={
        "requested_url":url,
        "resolved_url":url,
        "address":None,
        "facility_name":None,
        "floor":None,
        "postal_code":None,
        "prefecture":None,
        "city":None,
        "page_title":None,
        "method":None,
        "quality":None,
    }

    if not url or not url.startswith(("http://","https://")):
        return result

    resolution=resolve_google_news_url(url)
    if resolution.get("ok") and resolution.get("url"):
        url=resolution["url"]
        result["resolved_url"]=url
        result["resolution_method"]=resolution.get("method")

    try:
        with httpx.Client(follow_redirects=True,timeout=12.0,headers=HEADERS) as client:
            r=client.get(url)
            result["resolved_url"]=str(r.url)
            ctype=(r.headers.get("content-type") or "").lower()
            if r.status_code!=200 or "text/html" not in ctype:
                return result
            html=r.text[:2_000_000]
    except Exception:
        return result

    soup=BeautifulSoup(html,"html.parser")
    result["page_title"]=soup.title.get_text(" ",strip=True) if soup.title else None

    # 1. structured pairs/table
    for key,val in _table_pairs(soup):
        if any(neg in key for neg in NEGATIVE_ADDRESS_LABELS):
            continue
        if key in ("店舗所在地","施設所在地","所在地","住所","会場","開催場所"):
            addr=extract_address(f"{key}\n{val}",expected_prefecture)
            if addr:
                result["address"]=addr
                result["postal_code"]=extract_postal_code(val)
                result["floor"]=extract_floor(val)
                result["method"]="table"
                result["quality"]="high"
                break

    # name from table
    for key,val in _table_pairs(soup):
        if key in ("店舗名","施設名","会場名","名称","名 称","ホテル名","店名"):
            result["facility_name"]=val[:100]
            break

    # 2. JSON-LD only if prefecture consistent
    if not result["address"]:
        candidates=[]
        for obj in _jsonld_candidates(soup):
            _walk_jsonld(obj,candidates)
        for c in candidates:
            raw=c.get("address") or ""
            pref=detect_prefecture(raw)
            if expected_prefecture and pref and pref!=expected_prefecture:
                continue
            addr=extract_address(raw,expected_prefecture)
            if addr:
                result["address"]=addr
                result["postal_code"]=extract_postal_code(raw)
                result["facility_name"]=result["facility_name"] or c.get("name")
                result["method"]="jsonld"
                result["quality"]="high"
                break

    # 3. visible text, but same short block only
    for tag in soup(["script","style","noscript","svg"]):
        tag.decompose()
    text=soup.get_text("\n",strip=True)
    text=re.sub(r'\n{3,}','\n\n',text)[:350_000]

    if not result["address"]:
        block=_extract_same_block(text,expected_prefecture)
        if block:
            result["address"]=block["address"]
            result["postal_code"]=block["postal_code"]
            result["floor"]=block["floor"]
            result["method"]="visible-block"
            result["quality"]="medium"

    if not result["facility_name"]:
        # Page title often contains the proper store name more reliably than body fragments.
        from_title = extract_best_store_name_from_title(result.get("page_title") or "")
        if from_title:
            result["facility_name"] = from_title
        else:
            result["facility_name"]=extract_facility_name(text,result["address"])

    # final consistency check
    if result["address"] and expected_prefecture:
        if not address_matches_expected_prefecture(result["address"], expected_prefecture):
            result["address"] = None
            result["postal_code"] = None
            result["floor"] = None
            result["quality"] = "rejected_prefecture_mismatch"

    # final street-address plausibility check
    if result["address"] and not is_plausible_street_address(result["address"]):
        # Keep facility name if available, but do not store a fake street address
        result["address"] = None
        result["postal_code"] = None
        result["floor"] = None
        result["quality"] = "rejected_not_street_address"

    result["prefecture"] = detect_prefecture(result["address"] or "")
    result["city"] = detect_city(result["address"] or "")

    return result

