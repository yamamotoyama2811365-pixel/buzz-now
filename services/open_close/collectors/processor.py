from .public_quality import valid_store_name, valid_facility, valid_floor
from .article_enricher import fetch_article_facts
from .text_rules import calculate_confidence,is_likely_non_store_event

def _propagate_to_stores(cur, candidate):
    """
    discovery item から既存 stores に住所を後付けする。
    厳密一致できる範囲だけ更新し、別店舗への誤結合を避ける。
    """
    name=candidate.get("name")
    if not name:return 0

    params=[candidate.get("address"),candidate.get("facility"),candidate.get("floor"),
            candidate.get("postal"),candidate.get("resolved_url"),candidate.get("official_url"),
            candidate.get("confidence"),name]

    extra=""
    if candidate.get("pref"):
        extra+=" AND COALESCE(prefecture,'')=COALESCE(%s,'')"
        params.append(candidate.get("pref"))
    if candidate.get("city"):
        extra+=" AND COALESCE(city,'')=COALESCE(%s,'')"
        params.append(candidate.get("city"))

    cur.execute(f"""
        UPDATE stores
        SET address=COALESCE(address,%s),
            facility_name=COALESCE(facility_name,%s),
            floor=COALESCE(floor,%s),
            postal_code=COALESCE(postal_code,%s),
            source_url=COALESCE(%s,source_url),
            official_url=COALESCE(official_url,%s),
            confidence=GREATEST(confidence,%s),
            updated_at=NOW()
        WHERE name=%s {extra}
    """,params)

    return cur.rowcount

def enrich_candidates(database_url,limit=50,include_processed=True):
    import psycopg
    enriched=address_found=store_updates=0

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            where="" if include_processed else "WHERE processed=FALSE"
            cur.execute(f"""
                SELECT id,title,raw_summary,store_name_candidate,detected_status,
                       category_candidate,prefecture,city,event_date_candidate,
                       source_url,source_name,confidence,address_candidate,
                       facility_name_candidate,floor_candidate,postal_code_candidate,
                       official_url_candidate
                FROM discovery_items
                {where}
                ORDER BY
                    CASE WHEN address_candidate IS NULL THEN 0 ELSE 1 END,
                    confidence DESC,id DESC
                LIMIT %s
            """,(limit,))
            rows=cur.fetchall()

            for row in rows:
                (
                    did,title,summary,name,ds,cat,pref,city,event,url,sname,conf,
                    addr,facility,floor,postal,official
                )=row

                facts=fetch_article_facts(url,expected_prefecture=pref,expected_store_name=name)

                addr=addr or facts.get("address")
                facility=facility or facts.get("facility_name")
                floor=floor or facts.get("floor")
                postal=postal or facts.get("postal_code")
                pref=pref or facts.get("prefecture")
                city=city or facts.get("city")
                resolved=facts.get("resolved_url") or url

                new_conf=calculate_confidence(
                    title or "",summary or "",ds,pref,city,event,cat,addr,facility
                )
                conf=max(int(conf or 0),int(new_conf or 0))

                cur.execute("""
                    UPDATE discovery_items
                    SET address_candidate=%s,
                        facility_name_candidate=%s,
                        floor_candidate=%s,
                        postal_code_candidate=%s,
                        resolved_source_url=%s,
                        prefecture=COALESCE(prefecture,%s),
                        city=COALESCE(city,%s),
                        confidence=%s
                    WHERE id=%s
                """,(addr,facility,floor,postal,resolved,pref,city,conf,did))

                enriched+=1
                if addr:address_found+=1

                store_updates+=_propagate_to_stores(cur,{
                    "name":name,"address":addr,"facility":facility,"floor":floor,
                    "postal":postal,"resolved_url":resolved,"official_url":official,
                    "confidence":conf,"pref":pref,"city":city
                })

    return {
        "enriched":enriched,
        "address_found":address_found,
        "store_updates":store_updates,
        "limit":limit
    }

def promote_candidates(database_url,min_confidence=80,enrich_limit=40):
    import psycopg

    if enrich_limit>0:
        enrich_candidates(database_url,limit=enrich_limit,include_processed=True)

    promoted=skipped=0

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id,store_name_candidate,detected_status,category_candidate,
                       prefecture,city,event_date_candidate,
                       COALESCE(resolved_source_url,source_url),
                       source_name,confidence,address_candidate,
                       facility_name_candidate,floor_candidate,postal_code_candidate,
                       official_url_candidate,title,raw_summary
                FROM discovery_items
                WHERE processed=FALSE
                  AND confidence >= %s
                  AND detected_status IN('opening','closing')
                  AND store_name_candidate IS NOT NULL
                ORDER BY confidence DESC,id DESC
            """,(min_confidence,))

            for row in cur.fetchall():
                did,name,ds,cat,pref,city,event,url,sname,conf,addr,facility,floor,postal,official,title,summary=row
                if is_likely_non_store_event(title,summary or ""):
                    cur.execute("UPDATE discovery_items SET processed=TRUE,rescue_status='excluded_event' WHERE id=%s",(did,))
                    skipped+=1
                    continue

                if not valid_store_name(name):
                    skipped += 1
                    continue
                facility = valid_facility(facility)
                floor = valid_floor(floor)
                status="opening" if ds=="opening" else "closing"
                od=event if status=="opening" else None
                cd=event if status=="closing" else None

                cur.execute("""
                    SELECT id FROM stores
                    WHERE name=%s
                      AND COALESCE(prefecture,'')=COALESCE(%s,'')
                      AND COALESCE(city,'')=COALESCE(%s,'')
                      AND COALESCE(open_date,DATE '1900-01-01')=COALESCE(%s,DATE '1900-01-01')
                      AND COALESCE(close_date,DATE '1900-01-01')=COALESCE(%s,DATE '1900-01-01')
                    LIMIT 1
                """,(name,pref,city,od,cd))
                existing=cur.fetchone()

                if existing:
                    cur.execute("""
                        UPDATE stores
                        SET address=COALESCE(address,%s),
                            facility_name=COALESCE(facility_name,%s),
                            floor=COALESCE(floor,%s),
                            postal_code=COALESCE(postal_code,%s),
                            source_url=COALESCE(%s,source_url),
                            official_url=COALESCE(official_url,%s),
                            confidence=GREATEST(confidence,%s),
                            updated_at=NOW()
                        WHERE id=%s
                    """,(addr,facility,floor,postal,url,official,conf,existing[0]))
                    cur.execute("UPDATE discovery_items SET processed=TRUE WHERE id=%s",(did,))
                    skipped+=1
                    continue

                cur.execute("""
                    INSERT INTO stores(
                        name,status,category,prefecture,city,address,facility_name,
                        floor,postal_code,open_date,close_date,source_url,source_name,
                        official_url,confidence,last_verified_at
                    )
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                """,(name,status,cat,pref,city,addr,facility,floor,postal,od,cd,url,sname,official,conf))

                cur.execute("UPDATE discovery_items SET processed=TRUE WHERE id=%s",(did,))
                promoted+=1

    return {"promoted":promoted,"skipped":skipped,"min_confidence":min_confidence}


