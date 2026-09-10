from .source_relocator import relocate_source
from .article_enricher import fetch_article_facts
from .text_rules import (
    calculate_confidence,
    extract_source_name_from_title,
    is_likely_non_store_event,
    is_likely_aggregate_store_article,
    extract_best_store_name_from_title
)

def rescue_sources(database_url,batch_size=5):
    import psycopg

    scanned=relocated=address_found=store_updated=unsupported=failed=rejected=excluded_event=0
    results=[]

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    s.id,s.name,s.prefecture,s.city,s.address,
                    d.id,d.title,d.source_name,d.source_url,d.resolved_source_url,
                    d.raw_summary,d.detected_status,d.category_candidate,
                    d.event_date_candidate,d.confidence
                FROM stores s
                JOIN LATERAL(
                    SELECT *
                    FROM discovery_items d
                    WHERE d.store_name_candidate=s.name
                      AND(
                        COALESCE(d.prefecture,'')=COALESCE(s.prefecture,'')
                        OR d.prefecture IS NULL OR s.prefecture IS NULL
                      )
                    ORDER BY
                        CASE WHEN d.rescue_attempted_at IS NULL THEN 0 ELSE 1 END,
                        d.id DESC
                    LIMIT 1
                ) d ON TRUE
                WHERE (s.address IS NULL OR s.address='')
                  AND COALESCE(s.status,'') <> 'excluded'
                  AND d.rescue_attempted_at IS NULL
                ORDER BY s.id DESC
                LIMIT %s
            """,(batch_size,))
            rows=cur.fetchall()

    for row in rows:
        (
            store_id,store_name,pref,city,store_address,
            did,title,source_name,source_url,resolved_source_url,
            summary,detected_status,category,event_date,confidence
        )=row
        scanned+=1

        item={
            "store_id":store_id,
            "store_name":store_name,
            "title":title,
        }

        try:
            if is_likely_non_store_event(title,summary or "") or is_likely_aggregate_store_article(title,summary or ""):
                excluded_event += 1
                item["excluded"] = True
                item["reason"] = "non_store_event"
                with psycopg.connect(database_url) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE discovery_items
                            SET rescue_attempted_at=NOW(),
                                rescue_status='excluded_event',
                                rescue_error=NULL,
                                processed=TRUE
                            WHERE id=%s
                        """,(did,))
                        cur.execute("""
                            UPDATE stores
                            SET status='excluded', updated_at=NOW()
                            WHERE id=%s
                        """,(store_id,))
                results.append(item)
                continue

            better_name = extract_best_store_name_from_title(title)
            if better_name:
                item["better_store_name"] = better_name

            real_publisher=extract_source_name_from_title(title) or source_name
            item["publisher"]=real_publisher

            relocation=relocate_source(title,real_publisher,source_url)
            item["relocation"]=relocation

            if not relocation.get("ok"):
                reason=relocation.get("reason") or "relocation_failed"
                if reason=="unsupported_publisher":
                    unsupported+=1
                    rescue_status="unsupported_publisher"
                else:
                    failed+=1
                    rescue_status="relocation_failed"

                with psycopg.connect(database_url) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE discovery_items
                            SET rescue_attempted_at=NOW(),
                                rescue_status=%s,
                                rescue_error=%s
                            WHERE id=%s
                        """,(rescue_status,str(reason)[:500],did))
                results.append(item)
                continue

            relocated+=1
            real_url=relocation.get("url")
            facts=fetch_article_facts(
                real_url,
                expected_prefecture=pref,
                expected_store_name=better_name or store_name
            )

            addr=facts.get("address")
            facility=facts.get("facility_name")
            floor=facts.get("floor")
            postal=facts.get("postal_code")

            item["facts"]={
                "address":addr,
                "facility_name":facility,
                "floor":floor,
                "postal_code":postal,
                "method":facts.get("method"),
                "quality":facts.get("quality"),
                "resolved_url":facts.get("resolved_url")
            }

            if facts.get("quality") in (
                "rejected_prefecture_mismatch",
                "rejected_not_street_address"
            ):
                rejected+=1

            new_conf=calculate_confidence(
                title or "",summary or "",detected_status,
                pref,city,event_date,category,addr,facility
            )
            confidence=max(int(confidence or 0),int(new_conf or 0))

            with psycopg.connect(database_url) as conn:
                with conn.cursor() as cur:
                    # Explicit casts avoid PostgreSQL IndeterminateDatatype
                    # when a value is NULL.
                    cur.execute("""
                        UPDATE discovery_items
                        SET resolved_source_url=%s::text,
                            address_candidate=COALESCE(%s::text,address_candidate),
                            facility_name_candidate=COALESCE(%s::text,facility_name_candidate),
                            floor_candidate=COALESCE(%s::text,floor_candidate),
                            postal_code_candidate=COALESCE(%s::text,postal_code_candidate),
                            confidence=GREATEST(confidence,%s),
                            rescue_attempted_at=NOW(),
                            rescue_status=%s,
                            rescue_error=NULL
                        WHERE id=%s
                    """,(
                        real_url,addr,facility,floor,postal,confidence,
                        "success_address" if addr else "relocated_no_valid_address",
                        did
                    ))

                    if better_name and better_name != store_name:
                        cur.execute("""
                            UPDATE stores
                            SET name=%s,updated_at=NOW()
                            WHERE id=%s
                        """,(better_name,store_id))
                        cur.execute("""
                            UPDATE discovery_items
                            SET store_name_candidate=%s
                            WHERE id=%s
                        """,(better_name,did))

                    if addr:
                        address_found+=1
                        cur.execute("""
                            UPDATE stores
                            SET address=%s::text,
                                facility_name=COALESCE(%s::text,facility_name),
                                floor=%s::text,
                                postal_code=%s::text,
                                source_url=%s::text,
                                source_name=COALESCE(%s::text,source_name),
                                confidence=GREATEST(confidence,%s),
                                updated_at=NOW()
                            WHERE id=%s
                        """,(addr,facility,floor,postal,real_url,real_publisher,confidence,store_id))
                        store_updated+=cur.rowcount

            results.append(item)

        except Exception as e:
            failed+=1
            err=f"{type(e).__name__}: {e}"[:500]
            item["error"]=err
            try:
                with psycopg.connect(database_url) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE discovery_items
                            SET rescue_attempted_at=NOW(),
                                rescue_status='error',
                                rescue_error=%s
                            WHERE id=%s
                        """,(err,did))
            except Exception:
                pass
            results.append(item)

    return {
        "batch_size":batch_size,
        "scanned":scanned,
        "relocated":relocated,
        "address_found":address_found,
        "store_updated":store_updated,
        "rejected_address":rejected,
        "excluded_event":excluded_event,
        "unsupported_publisher":unsupported,
        "failed":failed,
        "results":results
    }

