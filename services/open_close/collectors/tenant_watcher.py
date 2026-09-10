import hashlib
import re
from datetime import datetime, timezone
from urllib.parse import quote_plus

import feedparser
import httpx

from .sources import google_news_rss
from .text_rules import (
    TENANT_KEYWORDS, detect_prefecture, detect_city, extract_address,
    extract_postal_code, extract_facility_name
)

def _norm(value):
    s=str(value or "")
    s=s.replace("〒","")
    s=re.sub(r"\s+","",s)
    s=s.replace("−","-").replace("ー","-").replace("―","-")
    s=re.sub(r"[‐‑‒–—―ー]", "-", s)
    s=re.sub(r"[^\w一-龥ぁ-んァ-ヶ0-9-]", "", s)
    return s.lower()

def _fingerprint(title,url):
    return hashlib.sha256(f"{title.strip()}|{url.strip()}".encode()).hexdigest()

def _address_signature(address):
    """
    Conservative signature for matching a listing/news result back to a closed store.
    """
    n=_norm(address)
    if not n:
        return []
    tokens=[n]
    # Also keep the numeric street section if available.
    nums=re.findall(r"\d+(?:-\d+){1,3}", n)
    tokens.extend(x for x in nums if len(x)>=3)
    return list(dict.fromkeys(tokens))

def _result_matches_store(text, store):
    """
    Avoid claiming a tenant match only because a search result was returned.
    Require the address, facility name, or city + street-number signature.
    """
    t=_norm(text)
    address=store.get("address") or ""
    facility=store.get("facility_name") or ""
    city=store.get("city") or ""

    full=_norm(address)
    if full and full in t:
        return True, 92, "exact_address"

    fac=_norm(facility)
    if fac and len(fac)>=4 and fac in t:
        return True, 84, "facility"

    city_n=_norm(city)
    for token in _address_signature(address)[1:]:
        if city_n and city_n in t and token in t:
            return True, 82, "city_street"

    return False, 0, None

def _upsert_listing(cur, store_id, source_name, source_url, confidence, match_method, query_text=None):
    cur.execute("""
        INSERT INTO tenant_listings(
            store_id,source_name,source_url,status,last_verified_at,
            confidence,match_method,query_text
        )
        VALUES(%s,%s,%s,'detected',NOW(),%s,%s,%s)
        ON CONFLICT(store_id,source_url) DO UPDATE SET
            source_name=COALESCE(EXCLUDED.source_name,tenant_listings.source_name),
            status='detected',
            last_verified_at=NOW(),
            confidence=GREATEST(COALESCE(tenant_listings.confidence,0),EXCLUDED.confidence),
            match_method=COALESCE(EXCLUDED.match_method,tenant_listings.match_method),
            query_text=COALESCE(EXCLUDED.query_text,tenant_listings.query_text),
            updated_at=NOW()
        RETURNING (xmax=0)
    """,(store_id,source_name,source_url,confidence,match_method,query_text))
    return bool(cur.fetchone()[0])

def promote_existing_tenant_candidates(database_url, limit=120):
    """
    Match already-collected tenant/居抜き/貸店舗 discovery items to closed stores.
    Only address/facility-conservative matches are promoted.
    """
    import psycopg

    matched=inserted=checked=0
    details=[]

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,title,raw_summary,source_name,
                    COALESCE(resolved_source_url,source_url),
                    address_candidate,facility_name_candidate,prefecture,city
                FROM discovery_items
                WHERE detected_status='tenant'
                  AND COALESCE(rescue_status,'') <> 'tenant_checked'
                ORDER BY id DESC
                LIMIT %s
            """,(limit,))
            candidates=cur.fetchall()

            cur.execute("""
                SELECT id,name,address,facility_name,prefecture,city
                FROM stores
                WHERE status IN('closing','closed')
                  AND address IS NOT NULL AND address<>''
                ORDER BY updated_at DESC NULLS LAST,id DESC
                LIMIT 5000
            """)
            stores=[
                {
                    "id":r[0],"name":r[1],"address":r[2],"facility_name":r[3],
                    "prefecture":r[4],"city":r[5]
                }
                for r in cur.fetchall()
            ]

            for did,title,summary,source_name,url,c_addr,c_fac,c_pref,c_city in candidates:
                checked+=1
                text="\n".join(x or "" for x in [title,summary,c_addr,c_fac])
                best=None
                for store in stores:
                    if c_pref and store["prefecture"] and c_pref!=store["prefecture"]:
                        continue
                    if c_city and store["city"] and c_city!=store["city"]:
                        continue
                    ok,confidence,method=_result_matches_store(text,store)
                    if ok and (best is None or confidence>best[1]):
                        best=(store,confidence,method)

                if best:
                    store,confidence,method=best
                    is_new=_upsert_listing(
                        cur,store["id"],source_name or "公開情報",url,
                        confidence,method
                    )
                    matched+=1
                    inserted+=1 if is_new else 0
                    details.append({
                        "store_id":store["id"],"store_name":store["name"],
                        "source":source_name,"confidence":confidence,
                        "match_method":method
                    })

                cur.execute("""
                    UPDATE discovery_items
                    SET rescue_status='tenant_checked',
                        rescue_attempted_at=NOW()
                    WHERE id=%s
                """,(did,))

    return {
        "checked":checked,"matched":matched,"inserted":inserted,
        "items":details[:30]
    }

def scan_closed_store_tenant_news(database_url, limit_stores=5, per_store=12):
    """
    Free tenant watcher:
    - Selects least-recently-checked closed stores with an address.
    - Queries Google News RSS using the exact address/facility and tenant keywords.
    - Requires an address/facility signature before recording a listing candidate.
    This does not claim exhaustive property-portal coverage.
    """
    import psycopg

    stores_checked=feeds_checked=results_seen=matched=inserted=0
    items=[]

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT s.id,s.name,s.address,s.facility_name,s.prefecture,s.city
                FROM stores s
                LEFT JOIN tenant_watch_state tw ON tw.store_id=s.id
                WHERE s.status IN('closing','closed')
                  AND s.address IS NOT NULL AND s.address<>''
                ORDER BY tw.last_checked_at ASC NULLS FIRST,s.updated_at DESC NULLS LAST,s.id DESC
                LIMIT %s
            """,(limit_stores,))
            stores=[
                {
                    "id":r[0],"name":r[1],"address":r[2],"facility_name":r[3],
                    "prefecture":r[4],"city":r[5]
                }
                for r in cur.fetchall()
            ]

            for store in stores:
                stores_checked+=1
                queries=[
                    f'"{store["address"]}" ("テナント募集" OR "貸店舗" OR "居抜き" OR "空き店舗")'
                ]
                if store.get("facility_name"):
                    queries.append(
                        f'"{store["facility_name"]}" ("テナント募集" OR "貸店舗" OR "居抜き" OR "空き店舗")'
                    )

                error=None
                query_count=0
                for query in queries[:2]:
                    query_count+=1
                    feeds_checked+=1
                    try:
                        feed=feedparser.parse(google_news_rss(query))
                        entries=feed.entries[:per_store]
                    except Exception as e:
                        error=f"{type(e).__name__}: {e}"[:240]
                        continue

                    for entry in entries:
                        results_seen+=1
                        title=(entry.get("title") or "").strip()
                        url=(entry.get("link") or "").strip()
                        summary=(entry.get("summary") or "").strip()
                        if not title or not url:
                            continue
                        combined=title+"\n"+summary
                        if not any(k in combined for k in TENANT_KEYWORDS):
                            continue

                        ok,confidence,method=_result_matches_store(combined,store)
                        if not ok:
                            continue

                        src_obj=entry.get("source") or {}
                        source_name=None
                        try:
                            source_name=(src_obj.get("title") or "").strip() or None
                        except Exception:
                            pass
                        source_name=source_name or "Google News掲載元"

                        is_new=_upsert_listing(
                            cur,store["id"],source_name,url,
                            confidence,method,query_text=query
                        )
                        matched+=1
                        inserted+=1 if is_new else 0
                        items.append({
                            "store_id":store["id"],"store_name":store["name"],
                            "source":source_name,"confidence":confidence,
                            "match_method":method
                        })

                cur.execute("""
                    INSERT INTO tenant_watch_state(
                        store_id,last_checked_at,query_count,last_error,updated_at
                    )
                    VALUES(%s,NOW(),%s,%s,NOW())
                    ON CONFLICT(store_id) DO UPDATE SET
                        last_checked_at=NOW(),
                        query_count=tenant_watch_state.query_count+EXCLUDED.query_count,
                        last_error=EXCLUDED.last_error,
                        updated_at=NOW()
                """,(store["id"],query_count,error))

    return {
        "stores_checked":stores_checked,
        "feeds_checked":feeds_checked,
        "results_seen":results_seen,
        "matched":matched,
        "inserted":inserted,
        "items":items[:30]
    }


def verify_tenant_listings(database_url, limit=20):
    """
    Recheck previously detected listing/source URLs.
    Only explicit 404/410 is treated as unavailable; network errors never imply a contract.
    """
    import psycopg

    checked=active=unavailable=errors=0
    details=[]

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id,store_id,source_name,source_url,status
                FROM tenant_listings
                ORDER BY last_verified_at ASC NULLS FIRST,id ASC
                LIMIT %s
            """,(limit,))
            rows=cur.fetchall()

            with httpx.Client(
                follow_redirects=True,timeout=10.0,
                headers={"User-Agent":"Mozilla/5.0 (compatible; OpenCloseMap/1.0)"}
            ) as client:
                for lid,store_id,source_name,url,status in rows:
                    checked+=1
                    try:
                        r=client.get(url)
                        if r.status_code in (404,410):
                            cur.execute("""
                                UPDATE tenant_listings
                                SET status='unavailable',last_verified_at=NOW(),updated_at=NOW()
                                WHERE id=%s
                            """,(lid,))
                            unavailable+=1
                            details.append({"id":lid,"store_id":store_id,"status":"unavailable"})
                        elif 200 <= r.status_code < 400:
                            cur.execute("""
                                UPDATE tenant_listings
                                SET status='detected',last_verified_at=NOW(),updated_at=NOW()
                                WHERE id=%s
                            """,(lid,))
                            active+=1
                        else:
                            errors+=1
                    except Exception:
                        errors+=1

    return {
        "checked":checked,"active":active,"unavailable":unavailable,
        "errors":errors,"items":details[:20]
    }

def tenant_watch_status(database_url):
    import psycopg
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(*) FILTER(WHERE status='detected'),
                    COUNT(DISTINCT store_id) FILTER(WHERE status='detected'),
                    MAX(last_verified_at)
                FROM tenant_listings
            """)
            listings,stores,last_verified=cur.fetchone()

            cur.execute("""
                SELECT COUNT(*),MAX(last_checked_at)
                FROM tenant_watch_state
            """)
            checked_stores,last_checked=cur.fetchone()

    return {
        "active_listings":listings or 0,
        "stores_with_listing":stores or 0,
        "stores_checked":checked_stores or 0,
        "last_verified_at":last_verified.isoformat() if last_verified else None,
        "last_watch_at":last_checked.isoformat() if last_checked else None
    }

