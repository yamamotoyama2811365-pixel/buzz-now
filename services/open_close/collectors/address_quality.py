from .text_rules import detect_prefecture, is_plausible_street_address

def audit_and_clean(database_url, apply=False, limit=100):
    import psycopg

    checked=kept=cleared=review=0
    items=[]

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id,name,prefecture,city,address,facility_name,floor,postal_code,source_url,source_name
                FROM stores
                WHERE address IS NOT NULL AND address<>''
                ORDER BY id DESC
                LIMIT %s
            """,(limit,))
            rows=cur.fetchall()

            for r in rows:
                sid,name,pref,city,address,facility,floor,postal,url,source_name=r
                checked+=1

                addr_pref=detect_prefecture(address or "")
                status="keep"
                reason="valid_address"

                if pref and addr_pref and pref!=addr_pref:
                    status="clear"
                    reason="prefecture_mismatch"
                elif not is_plausible_street_address(address):
                    status="clear"
                    reason="not_plausible_street_address"
                elif not addr_pref:
                    status="review"
                    reason="address_has_no_prefecture"

                if status=="clear":
                    cleared+=1
                    if apply:
                        cur.execute("""
                            UPDATE stores
                            SET address=NULL,
                                floor=NULL,
                                postal_code=NULL,
                                updated_at=NOW()
                            WHERE id=%s
                        """,(sid,))
                elif status=="keep":
                    kept+=1
                else:
                    review+=1

                items.append({
                    "store_id":sid,
                    "store_name":name,
                    "expected_prefecture":pref,
                    "address":address,
                    "address_prefecture":addr_pref,
                    "status":status,
                    "reason":reason
                })

    return {
        "apply":apply,
        "checked":checked,
        "kept":kept,
        "cleared":cleared,
        "review":review,
        "items":items
    }

