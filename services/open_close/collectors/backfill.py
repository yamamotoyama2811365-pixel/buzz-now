import re
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from .openclose_hub import (
    HEADERS,
    _looks_like_article,
    _clean_anchor_title,
    _published_from_text,
    _insert_candidate,
)

BACKFILL_SOURCES = [
    {
        "key":"kaiten_heiten_24_open",
        "name":"開店閉店.com",
        "status":"opening",
        "base_url":"https://kaiten-heiten-24.com/category/open/",
        "page_url":"https://kaiten-heiten-24.com/category/open/page/{page}/",
        "allowed_domains":["kaiten-heiten-24.com"],
        "confidence":82,
        "max_items":60,
    },
    {
        "key":"kaiten_heiten_24_close",
        "name":"開店閉店.com",
        "status":"closing",
        "base_url":"https://kaiten-heiten-24.com/category/close/",
        "page_url":"https://kaiten-heiten-24.com/category/close/page/{page}/",
        "allowed_domains":["kaiten-heiten-24.com"],
        "confidence":82,
        "max_items":60,
    },
]

def _ensure_state(cur):
    for source in BACKFILL_SOURCES:
        cur.execute("""
            INSERT INTO source_backfill_state(
                source_key,next_page,completed,pages_fetched,items_inserted,updated_at
            )
            VALUES(%s,4,FALSE,0,0,NOW())
            ON CONFLICT(source_key) DO NOTHING
        """,(source["key"],))

def _discover_last_page(client, source):
    try:
        r=client.get(source["base_url"])
        if r.status_code != 200:
            return None, f"http_{r.status_code}"

        soup=BeautifulSoup(r.text[:1_500_000],"html.parser")
        nums=[1]
        for a in soup.find_all("a",href=True):
            href=a.get("href","")
            m=re.search(r'/page/(\d+)/?',href)
            if m:
                try:
                    nums.append(int(m.group(1)))
                except Exception:
                    pass

        return max(nums), None
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"[:240]

def _fetch_backfill_page(cur, client, source, page):
    url=source["page_url"].format(page=page)

    try:
        r=client.get(url)
    except Exception as e:
        return {
            "ok":False,
            "page":page,
            "url":url,
            "error":f"{type(e).__name__}: {e}"[:240],
            "found":0,
            "inserted":0,
            "duplicates":0,
            "terminal":False,
        }

    # WordPress normally returns 404 when pagination is past the end.
    if r.status_code == 404:
        return {
            "ok":True,
            "page":page,
            "url":url,
            "found":0,
            "inserted":0,
            "duplicates":0,
            "terminal":True,
            "reason":"http_404_end"
        }

    if r.status_code != 200:
        return {
            "ok":False,
            "page":page,
            "url":url,
            "error":f"http_{r.status_code}",
            "found":0,
            "inserted":0,
            "duplicates":0,
            "terminal":False,
        }

    # Guard against a pagination redirect back to page 1.
    final_url=str(r.url)
    requested_marker=f"/page/{page}/"
    if page > 1 and requested_marker not in final_url:
        return {
            "ok":True,
            "page":page,
            "url":url,
            "found":0,
            "inserted":0,
            "duplicates":0,
            "terminal":True,
            "reason":"redirected_past_end"
        }

    soup=BeautifulSoup(r.text[:2_000_000],"html.parser")
    seen=set()
    found=inserted=duplicates=0
    errors=[]

    for a in soup.find_all("a",href=True):
        raw=re.sub(r'\s+',' ',a.get_text(" ",strip=True))
        full=urljoin(final_url,a.get("href",""))

        if full in seen:
            continue
        if not _looks_like_article(raw,full,source):
            continue

        seen.add(full)
        found+=1

        title=_clean_anchor_title(raw)
        published=_published_from_text(raw)

        try:
            is_new=_insert_candidate(
                cur,source,title,full,raw,published,
                discovery_channel="openclose_hub_backfill"
            )
            if is_new:
                inserted+=1
            else:
                duplicates+=1
        except Exception as e:
            errors.append(f"{type(e).__name__}: {e}"[:200])

        if found >= source.get("max_items",60):
            break

    # A valid historical page should contain articles. A 200 page with none is
    # treated as terminal only after we know the pagination range.
    return {
        "ok":True,
        "page":page,
        "url":url,
        "found":found,
        "inserted":inserted,
        "duplicates":duplicates,
        "terminal":False,
        "errors":errors[:5]
    }

def collect_backfill_step(database_url):
    """
    Fetch exactly ONE historical page per run.
    The source with the oldest last_run_at is chosen, so opening/closing alternate.
    """
    import psycopg

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            _ensure_state(cur)

            cur.execute("""
                SELECT source_key,next_page,last_page,completed,last_run_at
                FROM source_backfill_state
                WHERE completed=FALSE
                ORDER BY last_run_at NULLS FIRST, source_key
                LIMIT 1
            """)
            state=cur.fetchone()

            if not state:
                return {
                    "completed":True,
                    "message":"All configured backfill sources are complete.",
                    "step":None
                }

            source_key,next_page,last_page,completed,last_run_at=state
            source=next((x for x in BACKFILL_SOURCES if x["key"]==source_key),None)
            if not source:
                cur.execute("""
                    UPDATE source_backfill_state
                    SET completed=TRUE,last_error='source_config_missing',updated_at=NOW()
                    WHERE source_key=%s
                """,(source_key,))
                return {
                    "completed":False,
                    "step":{
                        "source_key":source_key,
                        "error":"source_config_missing"
                    }
                }

            with httpx.Client(
                follow_redirects=True,timeout=12.0,headers=HEADERS
            ) as client:
                # Discover/refresh the terminal page number if needed.
                if last_page is None:
                    discovered,disc_error=_discover_last_page(client,source)
                    if discovered:
                        last_page=discovered
                        cur.execute("""
                            UPDATE source_backfill_state
                            SET last_page=%s,last_error=NULL,updated_at=NOW()
                            WHERE source_key=%s
                        """,(last_page,source_key))
                    elif disc_error:
                        cur.execute("""
                            UPDATE source_backfill_state
                            SET last_error=%s,updated_at=NOW()
                            WHERE source_key=%s
                        """,(disc_error,source_key))

                if last_page is not None and next_page > last_page:
                    cur.execute("""
                        UPDATE source_backfill_state
                        SET completed=TRUE,last_run_at=NOW(),last_error=NULL,updated_at=NOW()
                        WHERE source_key=%s
                    """,(source_key,))
                    return {
                        "completed":False,
                        "step":{
                            "source_key":source_key,
                            "source_name":source["name"],
                            "status":source["status"],
                            "page":next_page,
                            "last_page":last_page,
                            "terminal":True,
                            "reason":"cursor_past_last_page"
                        }
                    }

                result=_fetch_backfill_page(cur,client,source,next_page)

            if not result.get("ok"):
                cur.execute("""
                    UPDATE source_backfill_state
                    SET last_run_at=NOW(),
                        last_error=%s,
                        updated_at=NOW()
                    WHERE source_key=%s
                """,(result.get("error"),source_key))
            elif result.get("terminal"):
                cur.execute("""
                    UPDATE source_backfill_state
                    SET completed=TRUE,
                        last_run_at=NOW(),
                        last_error=NULL,
                        updated_at=NOW()
                    WHERE source_key=%s
                """,(source_key,))
            else:
                new_next=next_page+1
                is_complete=bool(last_page is not None and new_next > last_page)

                cur.execute("""
                    UPDATE source_backfill_state
                    SET next_page=%s,
                        completed=%s,
                        last_run_at=NOW(),
                        pages_fetched=pages_fetched+1,
                        items_inserted=items_inserted+%s,
                        last_error=%s,
                        updated_at=NOW()
                    WHERE source_key=%s
                """,(
                    new_next,
                    is_complete,
                    int(result.get("inserted") or 0),
                    "; ".join(result.get("errors") or [])[:500] or None,
                    source_key
                ))

            result.update({
                "source_key":source_key,
                "source_name":source["name"],
                "status":source["status"],
                "last_page":last_page,
            })

            return {
                "completed":False,
                "step":result
            }

def get_backfill_status(database_url):
    import psycopg

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            _ensure_state(cur)
            cur.execute("""
                SELECT
                    source_key,next_page,last_page,completed,last_run_at,
                    pages_fetched,items_inserted,last_error,updated_at
                FROM source_backfill_state
                ORDER BY source_key
            """)
            rows=cur.fetchall()

    items=[]
    for r in rows:
        items.append({
            "source_key":r[0],
            "next_page":r[1],
            "last_page":r[2],
            "completed":r[3],
            "last_run_at":r[4].isoformat() if r[4] else None,
            "pages_fetched":r[5],
            "items_inserted":r[6],
            "last_error":r[7],
            "updated_at":r[8].isoformat() if r[8] else None,
        })

    return {
        "all_completed":all(x["completed"] for x in items) if items else False,
        "items":items
    }

