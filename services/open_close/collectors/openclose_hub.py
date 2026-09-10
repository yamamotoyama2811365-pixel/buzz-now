import hashlib
import re
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from .text_rules import (
    detect_prefecture, detect_city, detect_category, extract_date,
    calculate_confidence, extract_address, extract_facility_name,
    extract_floor, extract_postal_code, is_plausible_street_address,
    extract_best_store_name_from_title, normalize_hub_store_name,
    is_good_hub_store_name, clean_hub_address, exact_event_date_from_text
)

HEADERS = {
    "User-Agent":"Mozilla/5.0 (compatible; OpenCloseMap/1.0; +https://open-close-map.onrender.com)",
    "Accept-Language":"ja,en;q=0.8",
}

# Curated discovery sources. We only take factual candidate fields + source URL.
HUB_SOURCES = [
    {
        "key":"kaiten_heiten_24_open",
        "name":"開店閉店.com",
        "kind":"openclose_directory",
        "status":"opening",
        "urls":[
            "https://kaiten-heiten-24.com/category/open/",
            "https://kaiten-heiten-24.com/category/open/page/2/",
            "https://kaiten-heiten-24.com/category/open/page/3/",
        ],
        "allowed_domains":["kaiten-heiten-24.com"],
        "confidence":82,
        "max_items":80,
    },
    {
        "key":"kaiten_heiten_24_close",
        "name":"開店閉店.com",
        "kind":"openclose_directory",
        "status":"closing",
        "urls":[
            "https://kaiten-heiten-24.com/category/close/",
            "https://kaiten-heiten-24.com/category/close/page/2/",
            "https://kaiten-heiten-24.com/category/close/page/3/",
        ],
        "allowed_domains":["kaiten-heiten-24.com"],
        "confidence":82,
        "max_items":80,
    },
    {
        "key":"shopship_open",
        "name":"ShopShip",
        "kind":"openclose_directory",
        "status":"opening",
        "urls":[
            "https://www.shopship.jp/open-close/category/shop-open/",
        ],
        "allowed_domains":["shopship.jp","www.shopship.jp"],
        "confidence":82,
        "max_items":60,
    },
    {
        "key":"shopship_close",
        "name":"ShopShip",
        "kind":"openclose_directory",
        "status":"closing",
        "urls":[
            "https://www.shopship.jp/open-close/category/shop-close/",
        ],
        "allowed_domains":["shopship.jp","www.shopship.jp"],
        "confidence":82,
        "max_items":60,
    },
    {
        "key":"open_close_japan",
        "name":"開店・閉店ポータル",
        "kind":"openclose_directory",
        "status":None,
        "urls":[
            "https://open-close-japan.com/",
        ],
        "allowed_domains":["open-close-japan.com","www.open-close-japan.com"],
        "confidence":78,
        "max_items":50,
    },
]

OPEN_MARKERS = ["【開店】","〖開店〗","［開店］","ニューオープン","新規オープン","OPEN"]
CLOSE_MARKERS = ["【閉店】","〖閉店〗","［閉店］","閉店"]

def fingerprint(title,url):
    return hashlib.sha256(f"{title.strip()}|{url.strip()}".encode()).hexdigest()

def _domain_ok(url, allowed):
    try:
        host=(urlparse(url).hostname or "").lower()
    except Exception:
        return False
    return any(host == d or host.endswith("." + d) for d in allowed)

def _clean_anchor_title(text):
    t=re.sub(r'\s+',' ',text or '').strip()

    # Dedicated sites often prepend article date/category metadata before marker.
    pos=[]
    for marker in ["【開店】","〖開店〗","［開店］","【閉店】","〖閉店〗","［閉店］"]:
        p=t.find(marker)
        if p >= 0:
            pos.append(p)
    if pos:
        t=t[min(pos):]

    t=re.sub(r'^(?:【|〖|［)(?:開店|閉店)(?:】|〗|］)\s*','',t).strip()

    # Remove a trailing article publication date accidentally included in card text.
    t=re.sub(r'\s+20\d{2}[/-]\d{1,2}[/-]\d{1,2}\s*$','',t)
    return t[:300].strip()

def _status_from_text(text, fallback=None):
    t=text or ""
    if any(m in t for m in CLOSE_MARKERS):
        return "closing"
    if any(m in t for m in OPEN_MARKERS):
        return "opening"
    return fallback

def _looks_like_article(text, url, source):
    if not text or len(text.strip()) < 3:
        return False
    if not _domain_ok(url, source["allowed_domains"]):
        return False

    bad_paths=[
        "/category/","/tag/","/author/","/page/","/privacy",
        "/contact","/jouhou","/about","/owner"
    ]
    path=(urlparse(url).path or "").lower()
    if any(x in path for x in bad_paths):
        return False

    t=text.strip()
    if source.get("status")=="opening":
        return any(m in t for m in OPEN_MARKERS) or "オープン" in t or "OPEN" in t
    if source.get("status")=="closing":
        return any(m in t for m in CLOSE_MARKERS)
    return (
        any(m in t for m in OPEN_MARKERS+CLOSE_MARKERS)
        or "オープン" in t
    )

def _published_from_text(text):
    t=text or ""
    for p in [
        r'(20\d{2})年(\d{1,2})月(\d{1,2})日',
        r'(20\d{2})[/-](\d{1,2})[/-](\d{1,2})'
    ]:
        m=re.search(p,t)
        if m:
            try:
                return datetime(
                    int(m.group(1)),int(m.group(2)),int(m.group(3)),
                    tzinfo=timezone.utc
                )
            except Exception:
                return None
    return None

def _extract_event_date_from_detail(text,status, page_title=None):
    combined=(page_title or "")+"\n"+(text or "")
    return exact_event_date_from_text(combined,status)

def _external_official_candidate(soup, source_domain):
    deny=[
        "facebook.com","instagram.com","x.com","twitter.com","youtube.com",
        "line.me","tiktok.com","google.com","maps.google","amazon.co.jp",
        "kaiten-heiten-24.com","kaiten-heiten.com","shopship.jp",
        "open-close-japan.com"
    ]

    candidates=[]
    for a in soup.find_all("a",href=True):
        href=a.get("href","").strip()
        if not href.startswith(("http://","https://")):
            continue
        host=(urlparse(href).hostname or "").lower()
        if not host:
            continue
        if source_domain and (host==source_domain or host.endswith("."+source_domain)):
            continue
        if any(d in host for d in deny):
            continue

        txt=re.sub(r'\s+',' ',a.get_text(" ",strip=True))
        score=0
        if any(k in txt for k in ["公式","ホームページ","HP","店舗サイト","公式サイト"]):
            score+=10
        if any(k in href.lower() for k in ["official","shop","store"]):
            score+=2
        candidates.append((score,href))

    if not candidates:
        return None
    candidates.sort(key=lambda x:x[0],reverse=True)
    # only use a clearly official-labeled external link
    return candidates[0][1] if candidates[0][0] >= 10 else None

def parse_detail_page(url, expected_status=None, expected_prefecture=None):
    result={
        "address":None,
        "facility_name":None,
        "floor":None,
        "postal_code":None,
        "prefecture":None,
        "city":None,
        "event_date":None,
        "official_url":None,
        "category":None,
        "page_title":None,
        "quality":None,
    }

    try:
        with httpx.Client(follow_redirects=True,timeout=12.0,headers=HEADERS) as client:
            r=client.get(url)
            if r.status_code!=200:
                result["quality"]=f"http_{r.status_code}"
                return result
            html=r.text[:2_000_000]
            final_url=str(r.url)
    except Exception as e:
        result["quality"]="fetch_failed"
        result["error"]=f"{type(e).__name__}: {e}"[:250]
        return result

    soup=BeautifulSoup(html,"html.parser")
    if soup.title:
        result["page_title"]=re.sub(r'\s+',' ',soup.title.get_text(" ",strip=True))

    for tag in soup(["script","style","noscript","svg"]):
        tag.decompose()
    text=soup.get_text("\n",strip=True)
    text=re.sub(r'\n{3,}','\n\n',text)[:350_000]

    addr=extract_address(text,expected_prefecture)
    if addr and is_plausible_street_address(addr):
        # only use floor if it occurs near the chosen address
        idx=text.find(addr)
        block=text[max(0,idx-120):idx+len(addr)+180] if idx>=0 else ""
        floor=extract_floor(block)
        addr=clean_hub_address(addr,floor)
        result["address"]=addr
        result["postal_code"]=extract_postal_code(block)
        result["floor"]=floor
        result["quality"]="structured_fact"
    else:
        result["quality"]="no_valid_address"

    h1_text=None
    h1=soup.find("h1")
    if h1:
        h1_text=_clean_anchor_title(h1.get_text(" ",strip=True))

    name=normalize_hub_store_name(
        h1_text or result["page_title"] or "",
        fallback=extract_best_store_name_from_title(result["page_title"] or "")
    )
    result["facility_name"]=name or extract_facility_name(text,result["address"])

    result["prefecture"]=detect_prefecture(result["address"] or text)
    result["city"]=detect_city(result["address"] or text)
    result["event_date"]=_extract_event_date_from_detail(text,expected_status,result["page_title"])

    # Category: store/facility name first, then article title/body.
    result["category"]=detect_category(
        "\n".join([
            result["facility_name"] or "",
            result["page_title"] or "",
            text[:25000]
        ]),
        store_name=result["facility_name"]
    )

    source_domain=(urlparse(final_url).hostname or "").lower()
    result["official_url"]=_external_official_candidate(soup,source_domain)
    return result

def _insert_candidate(cur, source, title, url, raw_card_text, published, discovery_channel='openclose_hub'):
    status=_status_from_text(raw_card_text,source.get("status"))
    if status not in ("opening","closing"):
        return False

    name=normalize_hub_store_name(title, fallback=_clean_anchor_title(title))
    if not name:
        return False

    text=raw_card_text+"\n"+title
    pref=detect_prefecture(text)
    city=detect_city(text)
    cat=detect_category(text,store_name=name)

    # Listing date is publication date, not necessarily opening/closing date.
    event=None
    conf=max(
        source.get("confidence",78),
        calculate_confidence(title,"",status,pref,city,event,cat,None,None)
    )

    cur.execute("""
        INSERT INTO discovery_items(
            fingerprint,title,source_name,source_url,published_at,
            detected_status,prefecture,city,confidence,raw_summary,
            store_name_candidate,event_date_candidate,category_candidate,
            publisher_name,publisher_home_url,discovery_channel
        )
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(fingerprint) DO UPDATE SET
            prefecture=COALESCE(EXCLUDED.prefecture,discovery_items.prefecture),
            city=COALESCE(EXCLUDED.city,discovery_items.city),
            confidence=GREATEST(discovery_items.confidence,EXCLUDED.confidence),
            store_name_candidate=COALESCE(EXCLUDED.store_name_candidate,discovery_items.store_name_candidate),
            category_candidate=COALESCE(EXCLUDED.category_candidate,discovery_items.category_candidate),
            publisher_name=COALESCE(EXCLUDED.publisher_name,discovery_items.publisher_name),
            publisher_home_url=COALESCE(EXCLUDED.publisher_home_url,discovery_items.publisher_home_url),
            discovery_channel=COALESCE(discovery_items.discovery_channel,EXCLUDED.discovery_channel)
        RETURNING (xmax=0)
    """,(
        fingerprint(title,url),title,source["name"],url,published,status,pref,city,conf,
        raw_card_text[:1800],name,event,cat,source["name"],
        (
            (source.get("urls") or [source.get("base_url") or url])[0]
        ),
        discovery_channel
    ))
    return bool(cur.fetchone()[0])

def collect_openclose_hub(database_url):
    import psycopg

    fetched_pages=0
    found_links=0
    inserted=0
    duplicates=0
    source_stats=[]

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            with httpx.Client(follow_redirects=True,timeout=12.0,headers=HEADERS) as client:
                for source in HUB_SOURCES:
                    seen_urls=set()
                    src_found=src_inserted=src_dup=0
                    errors=[]

                    for list_url in source["urls"]:
                        try:
                            r=client.get(list_url)
                            fetched_pages+=1
                            if r.status_code != 200:
                                errors.append(f"{list_url}: http_{r.status_code}")
                                continue
                            soup=BeautifulSoup(r.text[:2_000_000],"html.parser")
                        except Exception as e:
                            errors.append(f"{list_url}: {type(e).__name__}")
                            continue

                        for a in soup.find_all("a",href=True):
                            raw=re.sub(r'\s+',' ',a.get_text(" ",strip=True))
                            full=urljoin(str(r.url),a.get("href",""))
                            if full in seen_urls:
                                continue
                            if not _looks_like_article(raw,full,source):
                                continue

                            seen_urls.add(full)
                            found_links+=1
                            src_found+=1
                            title=_clean_anchor_title(raw)
                            published=_published_from_text(raw)

                            try:
                                is_new=_insert_candidate(
                                    cur,source,title,full,raw,published
                                )
                                if is_new:
                                    inserted+=1
                                    src_inserted+=1
                                else:
                                    duplicates+=1
                                    src_dup+=1
                            except Exception as e:
                                errors.append(f"insert:{type(e).__name__}")
                                continue

                            if src_found >= source.get("max_items",60):
                                break

                        if src_found >= source.get("max_items",60):
                            break

                    source_stats.append({
                        "source":source["name"],
                        "key":source["key"],
                        "found":src_found,
                        "inserted":src_inserted,
                        "duplicates":src_dup,
                        "errors":errors[:5],
                    })

    return {
        "fetched_pages":fetched_pages,
        "found_links":found_links,
        "inserted":inserted,
        "duplicates":duplicates,
        "sources":source_stats
    }

def enrich_hub_candidates(database_url,limit=20):
    import psycopg

    scanned=enriched=address_found=official_found=store_updates=0
    parsed_address_found=parsed_official_found=0
    results=[]
    enriched_ids=[]

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,title,source_url,detected_status,prefecture,city,
                    store_name_candidate,confidence,raw_summary,source_name
                FROM discovery_items
                WHERE discovery_channel IN('openclose_hub','openclose_hub_backfill')
                  AND processed=FALSE
                  AND COALESCE(rescue_status,'') NOT IN(
                      'hub_enriched','hub_needs_review','hub_promoted','excluded_event'
                  )
                ORDER BY
                    CASE WHEN discovery_channel='openclose_hub' THEN 0 ELSE 1 END,
                    CASE WHEN address_candidate IS NULL THEN 0 ELSE 1 END,
                    id DESC
                LIMIT %s
            """,(limit,))
            rows=cur.fetchall()

    for did,title,url,status,pref,city,name,confidence,summary,source_name in rows:
        scanned+=1
        try:
            facts=parse_detail_page(
                url,
                expected_status=status,
                expected_prefecture=pref
            )
            addr=facts.get("address")
            facility=facts.get("facility_name")
            floor=facts.get("floor")
            postal=facts.get("postal_code")
            event=facts.get("event_date")
            official=facts.get("official_url")
            detail_category=facts.get("category")

            clean_name=normalize_hub_store_name(
                title,
                fallback=facility or name
            )
            if clean_name:
                name=clean_name
            pref2=pref or facts.get("prefecture")
            city2=city or facts.get("city")

            if addr:
                parsed_address_found+=1
            if official:
                parsed_official_found+=1

            category2=detail_category or detect_category(
                "\n".join([name or "",title or "",summary or ""]),
                store_name=name
            )

            new_conf=calculate_confidence(
                title,"",status,pref2,city2,event,category2,addr,facility
            )
            conf=max(int(confidence or 0),int(new_conf or 0))
            if official:
                conf=min(98,conf+5)

            with psycopg.connect(database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE discovery_items
                        SET store_name_candidate=COALESCE(%s::text,store_name_candidate),
                            address_candidate=COALESCE(%s::text,address_candidate),
                            facility_name_candidate=COALESCE(%s::text,facility_name_candidate),
                            floor_candidate=COALESCE(%s::text,floor_candidate),
                            postal_code_candidate=COALESCE(%s::text,postal_code_candidate),
                            event_date_candidate=COALESCE(%s,event_date_candidate),
                            prefecture=COALESCE(prefecture,%s::text),
                            city=COALESCE(city,%s::text),
                            official_url_candidate=COALESCE(%s::text,official_url_candidate),
                            category_candidate=COALESCE(%s::text,category_candidate),
                            confidence=GREATEST(confidence,%s),
                            rescue_status='hub_enriched',
                            rescue_error=NULL
                        WHERE id=%s
                    """,(
                        name,addr,facility,floor,postal,event,pref2,city2,official,category2,conf,did
                    ))

                    if addr:
                        address_found+=1
                    if official:
                        official_found+=1

                    # update existing matching store only; promotion is separate
                    params=[
                        addr,facility,floor,postal,official,
                        category2,category2,conf,name
                    ]
                    extra=""
                    if pref2:
                        extra+=" AND COALESCE(prefecture,'')=COALESCE(%s,'')"
                        params.append(pref2)

                    cur.execute(f"""
                        UPDATE stores
                        SET address=COALESCE(address,%s::text),
                            facility_name=COALESCE(facility_name,%s::text),
                            floor=COALESCE(floor,%s::text),
                            postal_code=COALESCE(postal_code,%s::text),
                            official_url=COALESCE(official_url,%s::text),
                            category=CASE
                                WHEN %s::text IS NULL THEN category
                                WHEN category IS NULL OR category IN(
                                    '未分類','業種未分類','小売','飲食店'
                                ) THEN %s::text
                                ELSE category
                            END,
                            confidence=GREATEST(confidence,%s),
                            updated_at=NOW()
                        WHERE name=%s
                        {extra}
                    """,params)
                    store_updates+=cur.rowcount

            enriched+=1
            enriched_ids.append(did)
            results.append({
                "discovery_id":did,
                "name":name,
                "address":addr,
                "floor":floor,
                "official_url":official,
                "event_date":event.isoformat() if event else None,
                "quality":facts.get("quality"),
            })
        except Exception as e:
            results.append({
                "discovery_id":did,
                "name":name,
                "error":f"{type(e).__name__}: {e}"[:400]
            })

    return {
        "scanned":scanned,
        "enriched":enriched,
        "parsed_address_found":parsed_address_found,
        "address_found":address_found,
        "parsed_official_found":parsed_official_found,
        "official_found":official_found,
        "store_updates":store_updates,
        "enriched_ids":enriched_ids,
        "results":results
    }

def _hub_candidate_is_publishable(name, address, official_url, event_date, prefecture, city):
    if not is_good_hub_store_name(name):
        return False, "invalid_store_name"

    if address and is_plausible_street_address(address):
        return True, "valid_address"

    if official_url:
        return True, "official_url"

    if event_date and prefecture and city:
        return True, "dated_local_record"

    return False, "insufficient_verified_facts"

def promote_hub_candidates(database_url, discovery_ids):
    """
    Promote ONLY the IDs enriched by the current hub cycle.
    Never touches Google News or unrelated unprocessed discoveries.
    """
    import psycopg

    ids=[int(x) for x in (discovery_ids or [])]
    if not ids:
        return {"checked":0,"promoted":0,"updated":0,"needs_review":0,"items":[]}

    checked=promoted=updated=needs_review=0
    items=[]

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,title,store_name_candidate,detected_status,category_candidate,
                    prefecture,city,event_date_candidate,source_url,source_name,
                    confidence,address_candidate,facility_name_candidate,
                    floor_candidate,postal_code_candidate,official_url_candidate
                FROM discovery_items
                WHERE discovery_channel IN('openclose_hub','openclose_hub_backfill')
                  AND id = ANY(%s)
                  AND rescue_status='hub_enriched'
            """,(ids,))

            for row in cur.fetchall():
                (
                    did,title,name,status,category,pref,city,event,url,source_name,
                    confidence,address,facility,floor,postal,official
                )=row
                checked+=1

                name=normalize_hub_store_name(title,fallback=name or facility)
                address=clean_hub_address(address,floor)

                ok,reason=_hub_candidate_is_publishable(
                    name,address,official,event,pref,city
                )

                if not ok:
                    needs_review+=1
                    cur.execute("""
                        UPDATE discovery_items
                        SET processed=TRUE,
                            rescue_status='hub_needs_review',
                            rescue_error=%s
                        WHERE id=%s
                    """,(reason,did))
                    items.append({
                        "discovery_id":did,
                        "name":name,
                        "action":"needs_review",
                        "reason":reason
                    })
                    continue

                db_status="opening" if status=="opening" else "closing"
                open_date=event if db_status=="opening" else None
                close_date=event if db_status=="closing" else None

                # Source URL is the strongest exact identity for hub articles.
                cur.execute("SELECT id FROM stores WHERE source_url=%s LIMIT 1",(url,))
                existing=cur.fetchone()

                if not existing:
                    cur.execute("""
                        SELECT id FROM stores
                        WHERE name=%s
                          AND COALESCE(prefecture,'')=COALESCE(%s,'')
                          AND COALESCE(city,'')=COALESCE(%s,'')
                          AND COALESCE(open_date,DATE '1900-01-01')=COALESCE(%s,DATE '1900-01-01')
                          AND COALESCE(close_date,DATE '1900-01-01')=COALESCE(%s,DATE '1900-01-01')
                        LIMIT 1
                    """,(name,pref,city,open_date,close_date))
                    existing=cur.fetchone()

                if existing:
                    cur.execute("""
                        UPDATE stores
                        SET name=%s,
                            status=%s,
                            category=COALESCE(%s,category),
                            prefecture=COALESCE(%s,prefecture),
                            city=COALESCE(%s,city),
                            address=%s,
                            facility_name=COALESCE(%s,facility_name),
                            floor=%s,
                            postal_code=%s,
                            open_date=%s,
                            close_date=%s,
                            source_url=%s,
                            source_name=%s,
                            official_url=COALESCE(%s,official_url),
                            confidence=GREATEST(confidence,%s),
                            last_verified_at=NOW(),
                            updated_at=NOW()
                        WHERE id=%s
                    """,(
                        name,db_status,category,pref,city,address,facility,floor,postal,
                        open_date,close_date,url,source_name,official,confidence,existing[0]
                    ))
                    updated+=1
                    store_id=existing[0]
                    action="updated"
                else:
                    cur.execute("""
                        INSERT INTO stores(
                            name,status,category,prefecture,city,address,facility_name,
                            floor,postal_code,open_date,close_date,source_url,source_name,
                            official_url,confidence,last_verified_at
                        )
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        RETURNING id
                    """,(
                        name,db_status,category,pref,city,address,facility,floor,postal,
                        open_date,close_date,url,source_name,official,confidence
                    ))
                    store_id=cur.fetchone()[0]
                    promoted+=1
                    action="promoted"

                cur.execute("""
                    UPDATE discovery_items
                    SET store_name_candidate=%s,
                        address_candidate=%s,
                        processed=TRUE,
                        rescue_status='hub_promoted',
                        rescue_error=NULL
                    WHERE id=%s
                """,(name,address,did))

                items.append({
                    "discovery_id":did,
                    "store_id":store_id,
                    "name":name,
                    "action":action,
                    "quality_reason":reason
                })

    return {
        "checked":checked,
        "promoted":promoted,
        "updated":updated,
        "needs_review":needs_review,
        "items":items
    }

def reset_and_hide_hub_promotions(database_url):
    """
    Safety repair for v1.3.0:
    Hide stores promoted from hub before detailed verification, then reset hub
    discoveries so they can be re-enriched with the stricter parser.
    """
    import psycopg

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE stores s
                SET status='excluded',updated_at=NOW()
                FROM discovery_items d
                WHERE d.discovery_channel IN('openclose_hub','openclose_hub_backfill')
                  AND s.source_url=d.source_url
                  AND COALESCE(s.status,'') <> 'excluded'
            """)
            hidden=cur.rowcount

            cur.execute("""
                UPDATE discovery_items
                SET processed=FALSE,
                    address_candidate=NULL,
                    facility_name_candidate=NULL,
                    floor_candidate=NULL,
                    postal_code_candidate=NULL,
                    event_date_candidate=NULL,
                    official_url_candidate=NULL,
                    rescue_status=NULL,
                    rescue_error=NULL
                WHERE discovery_channel IN('openclose_hub','openclose_hub_backfill')
            """)
            reset=cur.rowcount

    return {"hidden_stores":hidden,"reset_discoveries":reset}

