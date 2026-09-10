import hashlib
from datetime import datetime, timezone

import feedparser

from .sources import google_news_rss
from .source_health import ensure_source_run_table
from .text_rules import (
    PREFECTURES,
    classify_title,
    detect_prefecture,
    detect_city,
    detect_category,
    extract_date,
    clean_store_name,
    calculate_confidence,
    extract_address,
    extract_facility_name,
    extract_floor,
    extract_postal_code,
    extract_source_name_from_title,
)


CLOSE_QUERY_TERMS = '"閉店" OR "営業終了" OR "閉館" OR "撤退"'
DEFAULT_BATCH_SIZE = 8
DEFAULT_LIMIT_PER_PREFECTURE = 18


def _fingerprint(title, url):
    return hashlib.sha256(
        f"{title.strip()}|{url.strip()}".encode("utf-8")
    ).hexdigest()


def _publisher(entry):
    src_obj = entry.get("source") or {}
    name = None
    home = None

    try:
        name = (src_obj.get("title") or "").strip() or None
        home = (src_obj.get("href") or "").strip() or None
    except Exception:
        pass

    title = (entry.get("title") or "").strip()
    name = name or extract_source_name_from_title(title) or "Google News"

    return name, home


def _latest_scan_and_closing_counts(database_url):
    """
    Never-scanned prefectures are chosen first.
    Among never-scanned prefectures, the areas with fewer existing closing records
    are prioritized. After one nationwide lap, the oldest-scanned areas come first.
    """
    import psycopg

    closing_counts = {p: 0 for p in PREFECTURES}
    latest_scan = {p: None for p in PREFECTURES}

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT prefecture, COUNT(*)
                FROM stores
                WHERE status IN('closing','closed')
                  AND prefecture IS NOT NULL
                GROUP BY prefecture
            """)
            for pref, count in cur.fetchall():
                if pref in closing_counts:
                    closing_counts[pref] = int(count or 0)

            cur.execute("""
                SELECT DISTINCT ON(source_key)
                    source_key, run_at
                FROM collector_source_runs
                WHERE family='pref_close'
                ORDER BY source_key, run_at DESC, id DESC
            """)
            for key, run_at in cur.fetchall():
                if not key or not str(key).startswith("pref_close:"):
                    continue
                pref = str(key).split(":", 1)[1]
                if pref in latest_scan:
                    latest_scan[pref] = run_at

    return latest_scan, closing_counts


def choose_prefectures(database_url, batch_size=DEFAULT_BATCH_SIZE):
    latest_scan, closing_counts = _latest_scan_and_closing_counts(database_url)

    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)

    ranked = sorted(
        PREFECTURES,
        key=lambda pref: (
            latest_scan[pref] is not None,
            latest_scan[pref] or epoch,
            closing_counts[pref],
            PREFECTURES.index(pref),
        )
    )

    return ranked[:max(1, min(int(batch_size or 8), len(PREFECTURES)))]


def _record_run(cur, pref, fetched, inserted, duplicates, rejected, errors):
    error_text = "; ".join(str(x)[:300] for x in errors if x)[:1800] or None
    error_count = len([x for x in errors if x])
    ok = not (error_count > 0 and int(fetched or 0) == 0)

    cur.execute("""
        INSERT INTO collector_source_runs(
            family, source_key, source_name,
            fetched, inserted, duplicates, rejected,
            error_count, error_text, ok
        )
        VALUES('pref_close',%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """,(
        f"pref_close:{pref}",
        f"{pref} 閉店収集",
        int(fetched or 0),
        int(inserted or 0),
        int(duplicates or 0),
        int(rejected or 0),
        error_count,
        error_text,
        ok,
    ))


def collect_prefecture_closings(
    database_url,
    batch_size=DEFAULT_BATCH_SIZE,
    limit_per_prefecture=DEFAULT_LIMIT_PER_PREFECTURE,
):
    """
    Rotate through Japan's 47 prefectures.
    Eight prefectures per hourly run means one nationwide lap in about six runs.

    The collector is deliberately closing-only. It supplements the general feeds
    so opening-heavy publishers cannot crowd closing information out.
    """
    import psycopg

    ensure_source_run_table(database_url)

    targets = choose_prefectures(database_url, batch_size=batch_size)
    items = []

    total_fetched = 0
    total_inserted = 0
    total_duplicates = 0
    total_rejected = 0

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            for pref in targets:
                fetched = inserted = duplicates = rejected = 0
                errors = []

                query = (
                    f'"{pref}" ({CLOSE_QUERY_TERMS}) '
                    f'(店舗 OR 店 OR 商業施設 OR ショップ OR スーパー OR 飲食店)'
                )
                url = google_news_rss(query)

                try:
                    feed = feedparser.parse(url)

                    if getattr(feed, "bozo", False) and not feed.entries:
                        errors.append(
                            str(getattr(feed, "bozo_exception", "feed_error"))[:180]
                        )

                    for entry in feed.entries[:int(limit_per_prefecture)]:
                        fetched += 1
                        total_fetched += 1

                        title = (entry.get("title") or "").strip()
                        source_url = (entry.get("link") or "").strip()
                        summary = (entry.get("summary") or "").strip()

                        if not title or not source_url:
                            rejected += 1
                            total_rejected += 1
                            continue

                        text = title + "\n" + summary
                        status, base = classify_title(title)

                        if status is None:
                            status, base = classify_title(text)

                        # This collector is strictly for closing information.
                        if status != "closing":
                            rejected += 1
                            total_rejected += 1
                            continue

                        detected_pref = detect_prefecture(text)

                        # If the article clearly names another prefecture, reject it.
                        if detected_pref and detected_pref != pref:
                            rejected += 1
                            total_rejected += 1
                            continue

                        candidate_pref = detected_pref or pref
                        city = detect_city(text)
                        event = extract_date(text)
                        name = clean_store_name(title)
                        category = detect_category(text, store_name=name)
                        address = extract_address(text)
                        facility = extract_facility_name(text, address)
                        floor = extract_floor(text)
                        postal = extract_postal_code(text)
                        publisher_name, publisher_home = _publisher(entry)

                        confidence = max(
                            74,
                            int(base or 0),
                            int(calculate_confidence(
                                title,
                                summary,
                                "closing",
                                candidate_pref,
                                city,
                                event,
                                category,
                                address,
                                facility,
                            ) or 0),
                        )

                        # Explicit prefecture text is stronger than a query-only match.
                        if detected_pref == pref:
                            confidence = min(100, confidence + 4)

                        published = None
                        if entry.get("published_parsed"):
                            p = entry["published_parsed"]
                            published = datetime(
                                p.tm_year, p.tm_mon, p.tm_mday,
                                p.tm_hour, p.tm_min, p.tm_sec,
                                tzinfo=timezone.utc,
                            )

                        try:
                            cur.execute("""
                                INSERT INTO discovery_items(
                                    fingerprint,title,source_name,source_url,published_at,
                                    detected_status,prefecture,city,confidence,raw_summary,
                                    store_name_candidate,event_date_candidate,category_candidate,
                                    address_candidate,facility_name_candidate,floor_candidate,
                                    postal_code_candidate,publisher_name,publisher_home_url,
                                    discovery_channel
                                )
                                VALUES(
                                    %s,%s,%s,%s,%s,
                                    'closing',%s,%s,%s,%s,
                                    %s,%s,%s,
                                    %s,%s,%s,
                                    %s,%s,%s,
                                    'rss_prefecture_closing'
                                )
                                ON CONFLICT(fingerprint) DO UPDATE SET
                                    prefecture=COALESCE(discovery_items.prefecture,EXCLUDED.prefecture),
                                    city=COALESCE(discovery_items.city,EXCLUDED.city),
                                    confidence=GREATEST(discovery_items.confidence,EXCLUDED.confidence),
                                    category_candidate=COALESCE(discovery_items.category_candidate,EXCLUDED.category_candidate),
                                    address_candidate=COALESCE(discovery_items.address_candidate,EXCLUDED.address_candidate),
                                    facility_name_candidate=COALESCE(discovery_items.facility_name_candidate,EXCLUDED.facility_name_candidate),
                                    floor_candidate=COALESCE(discovery_items.floor_candidate,EXCLUDED.floor_candidate),
                                    postal_code_candidate=COALESCE(discovery_items.postal_code_candidate,EXCLUDED.postal_code_candidate),
                                    publisher_name=COALESCE(discovery_items.publisher_name,EXCLUDED.publisher_name),
                                    publisher_home_url=COALESCE(discovery_items.publisher_home_url,EXCLUDED.publisher_home_url)
                                RETURNING (xmax=0)
                            """,(
                                _fingerprint(title, source_url),
                                title,
                                publisher_name,
                                source_url,
                                published,
                                candidate_pref,
                                city,
                                confidence,
                                summary[:2000] if summary else None,
                                name,
                                event,
                                category,
                                address,
                                facility,
                                floor,
                                postal,
                                publisher_name,
                                publisher_home,
                            ))

                            if cur.fetchone()[0]:
                                inserted += 1
                                total_inserted += 1
                            else:
                                duplicates += 1
                                total_duplicates += 1

                        except Exception as e:
                            errors.append(f"{type(e).__name__}: {e}"[:180])

                except Exception as e:
                    errors.append(f"{type(e).__name__}: {e}"[:180])

                _record_run(
                    cur,
                    pref,
                    fetched,
                    inserted,
                    duplicates,
                    rejected,
                    errors,
                )

                items.append({
                    "prefecture": pref,
                    "fetched": fetched,
                    "inserted": inserted,
                    "duplicates": duplicates,
                    "rejected": rejected,
                    "errors": errors[:3],
                })

    return {
        "targets": targets,
        "batch_size": len(targets),
        "limit_per_prefecture": int(limit_per_prefecture),
        "fetched": total_fetched,
        "inserted": total_inserted,
        "duplicates": total_duplicates,
        "rejected": total_rejected,
        "items": items,
    }


def get_prefecture_coverage(database_url):
    """
    Coverage view for all 47 prefectures.

    Published store counts and unprocessed discovery counts are kept separate so
    a large candidate backlog cannot be mistaken for published data.
    """
    import psycopg

    ensure_source_run_table(database_url)

    store_stats = {}
    discovery_stats = {}
    last_scan = {}

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    prefecture,
                    COUNT(*) FILTER(
                        WHERE status IN('opening','open')
                    ) AS opening_total,
                    COUNT(*) FILTER(
                        WHERE status IN('closing','closed')
                    ) AS closing_total,
                    COUNT(*) FILTER(
                        WHERE status IN('opening','open')
                          AND open_date IS NOT NULL
                          AND open_date >= CURRENT_DATE - INTERVAL '365 days'
                    ) AS opening_365d,
                    COUNT(*) FILTER(
                        WHERE status IN('closing','closed')
                          AND close_date IS NOT NULL
                          AND close_date >= CURRENT_DATE - INTERVAL '365 days'
                    ) AS closing_365d
                FROM stores
                WHERE prefecture IS NOT NULL
                GROUP BY prefecture
            """)
            for r in cur.fetchall():
                store_stats[r[0]] = {
                    "opening_total": int(r[1] or 0),
                    "closing_total": int(r[2] or 0),
                    "opening_365d": int(r[3] or 0),
                    "closing_365d": int(r[4] or 0),
                }

            cur.execute("""
                SELECT
                    prefecture,
                    COUNT(*) FILTER(
                        WHERE detected_status='opening' AND processed=FALSE
                    ) AS opening_pending,
                    COUNT(*) FILTER(
                        WHERE detected_status='closing' AND processed=FALSE
                    ) AS closing_pending,
                    COUNT(*) FILTER(
                        WHERE detected_status='closing'
                          AND processed=FALSE
                          AND discovery_channel='rss_prefecture_closing'
                    ) AS regional_closing_pending
                FROM discovery_items
                WHERE prefecture IS NOT NULL
                GROUP BY prefecture
            """)
            for r in cur.fetchall():
                discovery_stats[r[0]] = {
                    "opening_pending": int(r[1] or 0),
                    "closing_pending": int(r[2] or 0),
                    "regional_closing_pending": int(r[3] or 0),
                }

            cur.execute("""
                SELECT DISTINCT ON(source_key)
                    source_key, run_at, fetched, inserted, duplicates, error_count
                FROM collector_source_runs
                WHERE family='pref_close'
                ORDER BY source_key, run_at DESC, id DESC
            """)
            for key, run_at, fetched, inserted, duplicates, error_count in cur.fetchall():
                if not key or not str(key).startswith("pref_close:"):
                    continue
                pref = str(key).split(":", 1)[1]
                last_scan[pref] = {
                    "run_at": run_at.isoformat() if run_at else None,
                    "fetched": int(fetched or 0),
                    "inserted": int(inserted or 0),
                    "duplicates": int(duplicates or 0),
                    "error_count": int(error_count or 0),
                }

    items = []

    for pref in PREFECTURES:
        s = store_stats.get(pref, {
            "opening_total": 0,
            "closing_total": 0,
            "opening_365d": 0,
            "closing_365d": 0,
        })
        d = discovery_stats.get(pref, {
            "opening_pending": 0,
            "closing_pending": 0,
            "regional_closing_pending": 0,
        })

        recent_total = s["opening_365d"] + s["closing_365d"]
        closing_share = (
            round(s["closing_365d"] / recent_total, 3)
            if recent_total else 0.0
        )

        # Operational flag only: this does not claim the real-world closing rate.
        needs_more_closing = (
            s["closing_365d"] < 5
            or (
                s["opening_365d"] >= 10
                and s["closing_365d"] < s["opening_365d"] * 0.35
            )
        )

        items.append({
            "prefecture": pref,
            **s,
            **d,
            "closing_share_365d": closing_share,
            "needs_more_closing_collection": bool(needs_more_closing),
            "last_closing_scan": last_scan.get(pref),
        })

    items.sort(key=lambda x: (
        not x["needs_more_closing_collection"],
        x["closing_365d"],
        x["prefecture"],
    ))

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "prefectures": len(items),
        "needs_more_closing_collection": sum(
            1 for x in items if x["needs_more_closing_collection"]
        ),
        "items": items,
    }

