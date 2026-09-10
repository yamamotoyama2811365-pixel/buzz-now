from .text_rules import is_likely_non_store_event,is_likely_aggregate_store_article

def audit_non_store_events(database_url,apply=False,limit=200):
    import psycopg

    checked=excluded=kept=0
    items=[]

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    s.id,s.name,s.status,
                    d.id,d.title,d.raw_summary
                FROM stores s
                JOIN LATERAL(
                    SELECT id,title,raw_summary
                    FROM discovery_items
                    WHERE store_name_candidate=s.name
                    ORDER BY id DESC
                    LIMIT 1
                ) d ON TRUE
                WHERE COALESCE(s.status,'') <> 'excluded'
                ORDER BY s.id DESC
                LIMIT %s
            """,(limit,))
            rows=cur.fetchall()

            for sid,sname,sstatus,did,title,summary in rows:
                checked+=1
                bad=is_likely_non_store_event(title,summary or "") or is_likely_aggregate_store_article(title,summary or "")

                if bad:
                    excluded+=1
                    action="exclude"
                    if apply:
                        cur.execute("""
                            UPDATE stores
                            SET status='excluded',updated_at=NOW()
                            WHERE id=%s
                        """,(sid,))
                        cur.execute("""
                            UPDATE discovery_items
                            SET processed=TRUE,
                                rescue_status='excluded_event',
                                rescue_attempted_at=COALESCE(rescue_attempted_at,NOW()),
                                rescue_error=NULL
                            WHERE id=%s
                        """,(did,))
                else:
                    kept+=1
                    action="keep"

                items.append({
                    "store_id":sid,
                    "store_name":sname,
                    "title":title,
                    "action":action
                })

    return {
        "apply":apply,
        "checked":checked,
        "excluded":excluded,
        "kept":kept,
        "items":items
    }

