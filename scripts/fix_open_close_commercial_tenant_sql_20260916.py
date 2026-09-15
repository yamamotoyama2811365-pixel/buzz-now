from pathlib import Path

p=Path('services/open_close/collectors/seo_pages.py')
s=p.read_text()
old='''                cur.execute(f"""
                    SELECT COUNT(DISTINCT tl.store_id)
                    FROM tenant_listings tl
                    JOIN stores s ON s.id=tl.store_id
                    WHERE tl.status IN('detected','active')
                      AND {area_where.replace('COALESCE(status', "COALESCE(s.status").replace("prefecture=%s", "s.prefecture=%s").replace("(city=", "(s.city=").replace(" OR city=", " OR s.city=").replace(" OR city LIKE", " OR s.city LIKE")}
                """,area_params)
                active_tenant_count = int(cur.fetchone()[0] or 0)
'''
new='''                tenant_city_filter = city_filter.replace("city", "s.city")
                cur.execute(f"""
                    SELECT COUNT(DISTINCT tl.store_id)
                    FROM tenant_listings tl
                    JOIN stores s ON s.id=tl.store_id
                    WHERE tl.status IN('detected','active')
                      AND COALESCE(s.status,'') <> 'excluded'
                      AND (COALESCE(NULLIF(s.address,''),'')<>'' OR COALESCE(s.confidence,0)>=94)
                      AND s.prefecture=%s
                      AND {tenant_city_filter}
                """,[prefecture]+city_filter_params)
                active_tenant_count = int(cur.fetchone()[0] or 0)
'''
if old not in s:
    raise SystemExit('expected tenant SQL block not found')
s=s.replace(old,new,1)
p.write_text(s)
print('fixed tenant SQL')
