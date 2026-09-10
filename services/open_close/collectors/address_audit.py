from .article_enricher import fetch_article_facts

def classify_audit_row(row):
    (
        store_id, store_name, store_address, facility_name,
        source_url, source_name, prefecture, city,
        discovery_id, discovery_source_url, resolved_source_url,
        address_candidate, facility_candidate, processed
    ) = row

    result = {
        "store_id": store_id,
        "store_name": store_name,
        "source_name": source_name,
        "source_url": source_url,
        "prefecture": prefecture,
        "city": city,
        "store_address": store_address,
        "facility_name": facility_name,
        "discovery_id": discovery_id,
        "discovery_source_url": discovery_source_url,
        "resolved_source_url": resolved_source_url,
        "address_candidate": address_candidate,
        "facility_candidate": facility_candidate,
        "processed": processed,
        "status": None,
        "reason": None,
    }

    if store_address:
        result["status"] = "success"
        result["reason"] = "store_has_address"
        return result

    if address_candidate:
        result["status"] = "needs_sync"
        result["reason"] = "candidate_has_address_but_store_not_updated"
        return result

    url = resolved_source_url or discovery_source_url or source_url
    if not url:
        result["status"] = "no_source"
        result["reason"] = "no_source_url"
        return result

    facts = fetch_article_facts(url)
    result["audit_fetch"] = {
        "requested_url": facts.get("requested_url"),
        "resolved_url": facts.get("resolved_url"),
        "page_title": facts.get("page_title"),
        "method": facts.get("method"),
        "address": facts.get("address"),
        "facility_name": facts.get("facility_name"),
    }

    resolved = facts.get("resolved_url") or url
    if "news.google.com" in resolved:
        result["status"] = "unresolved_news_url"
        result["reason"] = "still_on_google_news_url"
    elif facts.get("address"):
        result["status"] = "found_on_recheck"
        result["reason"] = "address_found_during_audit"
    elif facts.get("page_title"):
        result["status"] = "page_reached_no_address"
        result["reason"] = "page_reached_but_address_not_detected"
    else:
        result["status"] = "fetch_failed"
        result["reason"] = "source_page_could_not_be_read"

    return result


def audit_addresses(database_url, limit=50):
    import psycopg

    items = []
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    s.id,
                    s.name,
                    s.address,
                    s.facility_name,
                    s.source_url,
                    s.source_name,
                    s.prefecture,
                    s.city,
                    d.id,
                    d.source_url,
                    d.resolved_source_url,
                    d.address_candidate,
                    d.facility_name_candidate,
                    d.processed
                FROM stores s
                LEFT JOIN LATERAL (
                    SELECT *
                    FROM discovery_items d
                    WHERE d.store_name_candidate = s.name
                      AND (
                        COALESCE(d.prefecture,'') = COALESCE(s.prefecture,'')
                        OR d.prefecture IS NULL
                        OR s.prefecture IS NULL
                      )
                    ORDER BY d.id DESC
                    LIMIT 1
                ) d ON TRUE
                ORDER BY
                    CASE WHEN s.address IS NULL OR s.address='' THEN 0 ELSE 1 END,
                    s.id DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()

    for row in rows:
        items.append(classify_audit_row(row))

    summary = {}
    for item in items:
        summary[item["status"]] = summary.get(item["status"], 0) + 1

    return {
        "count": len(items),
        "summary": summary,
        "items": items
    }

