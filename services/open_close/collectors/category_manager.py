from .text_rules import detect_category

GENERIC_CATEGORIES={None,"","未分類","業種未分類","小売","飲食店"}

def backfill_store_categories(database_url,limit=100):
    """
    Existing stores: repair missing/generic categories in small batches.
    Uses store name + linked discovery title/summary.
    """
    import psycopg

    checked=updated=unresolved=0
    items=[]

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    s.id,s.name,s.category,s.source_url,
                    d.title,d.raw_summary,d.category_candidate
                FROM stores s
                LEFT JOIN LATERAL(
                    SELECT title,raw_summary,category_candidate
                    FROM discovery_items
                    WHERE source_url=s.source_url
                    ORDER BY id DESC
                    LIMIT 1
                ) d ON TRUE
                WHERE COALESCE(s.status,'') <> 'excluded'
                  AND (
                    s.category IS NULL OR s.category='' OR
                    s.category IN('未分類','業種未分類','小売','飲食店')
                  )
                ORDER BY s.updated_at DESC NULLS LAST,s.id DESC
                LIMIT %s
            """,(limit,))
            rows=cur.fetchall()

            for sid,name,current,source_url,title,summary,candidate in rows:
                checked+=1

                inferred=detect_category(
                    "\\n".join([
                        name or "",
                        title or "",
                        summary or ""
                    ]),
                    store_name=name
                ) or candidate

                if not inferred:
                    unresolved+=1
                    continue

                # Do not replace a generic category with another generic category.
                if current in ("小売","飲食店") and inferred in ("小売","飲食店"):
                    unresolved+=1
                    continue

                cur.execute("""
                    UPDATE stores
                    SET category=%s,updated_at=NOW()
                    WHERE id=%s
                """,(inferred,sid))
                updated+=1
                items.append({
                    "store_id":sid,
                    "name":name,
                    "from":current,
                    "to":inferred
                })

    return {
        "checked":checked,
        "updated":updated,
        "unresolved":unresolved,
        "items":items[:30]
    }

def category_stats(database_url):
    import psycopg
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COALESCE(NULLIF(category,''),'業種未分類') category,
                    COUNT(*)
                FROM stores
                WHERE COALESCE(status,'') <> 'excluded'
                GROUP BY 1
                ORDER BY COUNT(*) DESC,1
            """)
            rows=cur.fetchall()

    return {
        "items":[
            {"category":category,"count":count}
            for category,count in rows
        ]
    }

