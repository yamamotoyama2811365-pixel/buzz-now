from pathlib import Path

p = Path('services/open_close/collectors/seo_pages.py')
s = p.read_text()

if 'def city_scope_clause(' not in s:
    insert_after = '''def city_is_indexable(prefecture, city):
    value=(city or "").strip()
    if not value or value in {"都市", "市区町村", "地域"}:
        return False
    if city_conflicts_with_prefecture(prefecture, value):
        return False
    if any(p in value for p in PREFECTURES):
        return False
    if len(value) > 16 or not _MUNICIPALITY_RE.fullmatch(value):
        return False
    if re.fullmatch(r"[ァ-ヶー]{2,}市", value):
        return False
    return True
'''
    assert insert_after in s
    s = s.replace(insert_after, insert_after + '''

def city_scope_clause(prefecture, city):
    """Match normalized/prefecture-prefixed cities and aggregate city wards."""
    value=(city or "").strip()
    if not value:
        return None, []
    prefixed=(prefecture or "")+value
    if value.endswith("市") and "区" not in value:
        return "(city=%s OR city=%s OR city LIKE %s OR city LIKE %s)", [
            value,prefixed,value+"%",prefixed+"%"
        ]
    return "(city=%s OR city=%s)", [value,prefixed]
''', 1)

quality = "COALESCE(status,'') <> 'excluded' AND (COALESCE(NULLIF(address,''),'')<>'' OR COALESCE(confidence,0)>=94)"
if quality not in s:
    s=s.replace("COALESCE(status,'') <> 'excluded'", quality)

old='''    offset = (page-1)*per_page

    with _connect(database_url) as conn:
        with conn.cursor() as cur:
            clauses = ["COALESCE(status,'') <> 'excluded' AND (COALESCE(NULLIF(address,''),'')<>'' OR COALESCE(confidence,0)>=94)","prefecture=%s"]
            params = [prefecture]
            if city:
                clauses.append("city=%s")
                params.append(city)
'''
new='''    offset = (page-1)*per_page
    city_filter, city_filter_params = city_scope_clause(prefecture, city)

    with _connect(database_url) as conn:
        with conn.cursor() as cur:
            clauses = ["COALESCE(status,'') <> 'excluded' AND (COALESCE(NULLIF(address,''),'')<>'' OR COALESCE(confidence,0)>=94)","prefecture=%s"]
            params = [prefecture]
            if city_filter:
                clauses.append(city_filter)
                params.extend(city_filter_params)
'''
assert old in s
s=s.replace(old,new,1)

old='''            stats_clauses = ["COALESCE(status,'') <> 'excluded' AND (COALESCE(NULLIF(address,''),'')<>'' OR COALESCE(confidence,0)>=94)","prefecture=%s"]
            stats_params = [prefecture]
            if city:
                stats_clauses.append("city=%s")
                stats_params.append(city)
'''
new='''            stats_clauses = ["COALESCE(status,'') <> 'excluded' AND (COALESCE(NULLIF(address,''),'')<>'' OR COALESCE(confidence,0)>=94)","prefecture=%s"]
            stats_params = [prefecture]
            if city_filter:
                stats_clauses.append(city_filter)
                stats_params.extend(city_filter_params)
'''
assert old in s
s=s.replace(old,new,1)

old='''            if city:
                cur.execute("""
                    SELECT COALESCE(category,'業種未分類'),COUNT(*)
                    FROM stores
                    WHERE COALESCE(status,'') <> 'excluded' AND (COALESCE(NULLIF(address,''),'')<>'' OR COALESCE(confidence,0)>=94)
                      AND prefecture=%s AND city=%s
                    GROUP BY COALESCE(category,'業種未分類')
                    ORDER BY COUNT(*) DESC,1
                    LIMIT 18
                """,(prefecture,city))
                facets = cur.fetchall()
                facet_type = "category"
'''
new='''            if city:
                cur.execute(f"""
                    SELECT COALESCE(category,'業種未分類'),COUNT(*)
                    FROM stores
                    WHERE COALESCE(status,'') <> 'excluded' AND (COALESCE(NULLIF(address,''),'')<>'' OR COALESCE(confidence,0)>=94)
                      AND prefecture=%s AND {city_filter}
                    GROUP BY COALESCE(category,'業種未分類')
                    ORDER BY COUNT(*) DESC,1
                    LIMIT 18
                """,[prefecture]+city_filter_params)
                facets = cur.fetchall()
                facet_type = "category"
'''
assert old in s
s=s.replace(old,new,1)

old='''            area_clauses = ["COALESCE(status,'') <> 'excluded' AND (COALESCE(NULLIF(address,''),'')<>'' OR COALESCE(confidence,0)>=94)","prefecture=%s"]
            area_params = [prefecture]
            if city:
                area_clauses.append("city=%s")
                area_params.append(city)
'''
new='''            area_clauses = ["COALESCE(status,'') <> 'excluded' AND (COALESCE(NULLIF(address,''),'')<>'' OR COALESCE(confidence,0)>=94)","prefecture=%s"]
            area_params = [prefecture]
            if city_filter:
                area_clauses.append(city_filter)
                area_params.extend(city_filter_params)
'''
assert old in s
s=s.replace(old,new,1)

old='''    area_name = f"{prefecture}{city or ''}"
    if city:
        title = f"{city}の開店・閉店情報｜最新店舗一覧｜{SITE_NAME}"
        desc = f"{prefecture}{city}の開店・閉店情報を掲載。新規オープン、閉店予定、閉店した店舗を住所・日付・業種から確認できます。"
        canonical_path = f"/area/{qpath(prefecture)}/{qpath(city)}"
        crumbs = [("トップ","/"),(prefecture,f"/area/{qpath(prefecture)}"),(city,canonical_path)]
    else:
        title = f"{prefecture}の開店・閉店情報｜新店・閉店一覧｜{SITE_NAME}"
        desc = f"{prefecture}の開店・閉店情報を市区町村別に掲載。新規オープン、閉店予定、閉店した店舗の最新情報を確認できます。"
        canonical_path = f"/area/{qpath(prefecture)}"
        crumbs = [("トップ","/"),(prefecture,canonical_path)]

    base_canonical = canonical_path

    if status == "opening":
        title = title.replace("開店・閉店情報","開店情報")
        desc = desc.replace("開店・閉店情報","開店情報")
    elif status == "closing":
        title = title.replace("開店・閉店情報","閉店情報")
        desc = desc.replace("開店・閉店情報","閉店情報")
'''
new='''    area_name = f"{prefecture}{city or ''}"
    display_name = city or prefecture
    if city:
        canonical_path = f"/area/{qpath(prefecture)}/{qpath(city)}"
        crumbs = [("トップ","/"),(prefecture,f"/area/{qpath(prefecture)}"),(city,canonical_path)]
    else:
        canonical_path = f"/area/{qpath(prefecture)}"
        crumbs = [("トップ","/"),(prefecture,canonical_path)]

    base_canonical = canonical_path
    if status == "opening":
        title = f"{display_name}の開店情報｜新規オープン・開店予定一覧｜{SITE_NAME}"
        desc = f"{area_name}の開店情報。新規オープン、開店予定の店舗を日付・住所・業種から確認できます。"
        page_heading = f"{display_name}の開店情報"
        hero_intro = f"{area_name}で確認された新規オープン、開店予定の店舗情報をまとめています。"
    elif status == "closing":
        title = f"{display_name}の閉店情報｜閉店予定・閉店店舗一覧｜{SITE_NAME}"
        desc = f"{area_name}の閉店情報。閉店予定、閉店した店舗を日付・住所・業種から確認できます。"
        page_heading = f"{display_name}の閉店情報"
        hero_intro = f"{area_name}で確認された閉店予定、閉店した店舗の情報をまとめています。"
    else:
        title = f"{display_name}の開店閉店情報｜新店・閉店一覧｜{SITE_NAME}"
        desc = f"{area_name}の開店閉店情報。新規オープン、開店予定、閉店予定、閉店店舗を日付・住所・業種から確認できます。"
        page_heading = f"{display_name}の開店閉店情報"
        hero_intro = f"{area_name}で確認された新規オープン、開店予定、閉店予定、閉店店舗の情報をまとめています。"
'''
assert old in s
s=s.replace(old,new,1)

old='''    for r in rows:
        d = row_to_dict(r)
        ev = event_date(d)
'''
new='''    for r in rows:
        d = row_to_dict(r)
        if d.get("quality_pending"):
            continue
        ev = event_date(d)
'''
assert old in s
s=s.replace(old,new,1)

needle='''    max_month = max(
        [int(r[1] or 0) for r in monthly_trend]
'''
if 'search-summary' not in s:
    summary='''    if status == "closing":
        summary_copy = f"このページでは{area_name}の閉店情報を中心に、閉店予定と閉店済みの店舗を新しい順に確認できます。地域全体の掲載データでは開店・開店予定が{opens}件、閉店・閉店予定が{closes}件あります。"
    elif status == "opening":
        summary_copy = f"このページでは{area_name}の開店情報を中心に、新規オープンと開店予定の店舗を新しい順に確認できます。地域全体の掲載データでは開店・開店予定が{opens}件、閉店・閉店予定が{closes}件あります。"
    else:
        summary_copy = f"{area_name}の開店閉店情報を、新規オープン・開店予定・閉店予定・閉店済みに分けて確認できます。現在の掲載データは{all_total}件で、開店・開店予定が{opens}件、閉店・閉店予定が{closes}件です。"
    summary_html = f"""
    <section class="panel search-summary">
      <div class="section-head"><div><h2>{esc(display_name)}の店舗の動き</h2><p>地域別の開店閉店情報</p></div></div>
      <p style="margin:0;color:var(--ink);font-size:14px">{esc(summary_copy)}</p>
      <p class="side-copy" style="margin:8px 0 0">直近12か月では開店{open_12m}件、閉店{close_12m}件を確認しています。掲載件数は当サイトで確認できた公開情報の範囲で、地域内すべての店舗を示すものではありません。</p>
    </section>

'''
    s=s.replace(needle,summary+needle,1)

old='''    <div class="crumbs">トップ　›　{esc(prefecture)}{('　›　'+esc(city)) if city else ''}</div>
    <h1>{esc(area_name)}の開店・閉店情報</h1>
    <p>{esc(area_name)}で確認された新規オープン、開店予定、閉店予定、閉店店舗の情報をまとめています。</p>
'''
new='''    <div class="crumbs">トップ　›　{esc(prefecture)}{('　›　'+esc(city)) if city else ''}</div>
    <h1>{esc(page_heading)}</h1>
    <p>{esc(hero_intro)}</p>
'''
assert old in s
s=s.replace(old,new,1)

old='''  <main>
    {insight_html}
'''
new='''  <main>
    {summary_html}
    {insight_html}
'''
assert old in s
s=s.replace(old,new,1)

s=s.replace('title = f"{category}の開店・閉店情報｜全国の新店・閉店一覧｜{SITE_NAME}"', 'title = f"{category}の開店閉店情報｜全国の新店・閉店一覧｜{SITE_NAME}"', 1)
s=s.replace('desc = f"全国の「{category}」に関する開店・閉店情報。新規オープン、開店予定、閉店予定、閉店店舗をエリア別に確認できます。"', 'desc = f"全国の「{category}」に関する開店閉店情報。新規オープン、開店予定、閉店予定、閉店店舗をエリア別に確認できます。"', 1)
s=s.replace('<h1>{esc(category)}の開店・閉店情報</h1>', '<h1>{esc(category)}の開店閉店情報</h1>', 1)

start=s.index('def sitemap_xml(database_url, origin, max_urls=45000):')
end=s.index('\ndef robots_txt(origin):',start)
new_sitemap='''def sitemap_xml(database_url, origin, max_urls=45000):
    urls=[origin.rstrip("/")+"/", origin.rstrip("/")+"/open/", origin.rstrip("/")+"/close/"]
    seen=set(urls)
    def add(path):
        url=site_url(origin,path)
        if url not in seen:
            seen.add(url);urls.append(url)

    public_filter="COALESCE(status,'') <> 'excluded' AND (COALESCE(NULLIF(address,''),'')<>'' OR COALESCE(confidence,0)>=94)"
    with _connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT prefecture,COUNT(*),
                       COUNT(*) FILTER(WHERE status IN('open','opening')),
                       COUNT(*) FILTER(WHERE status IN('closed','closing'))
                FROM stores WHERE {public_filter}
                  AND prefecture IS NOT NULL AND prefecture<>''
                GROUP BY prefecture ORDER BY prefecture
            """)
            for pref,total,open_count,close_count in cur.fetchall():
                base=f"/area/{qpath(pref)}";add(base)
                if int(open_count or 0)>=5:add(base+"?status=opening")
                if int(close_count or 0)>=5:add(base+"?status=closing")

            cur.execute(f"""
                SELECT prefecture,city,COUNT(*),
                       COUNT(*) FILTER(WHERE status IN('open','opening')),
                       COUNT(*) FILTER(WHERE status IN('closed','closing'))
                FROM stores WHERE {public_filter}
                  AND prefecture IS NOT NULL AND prefecture<>''
                  AND city IS NOT NULL AND city<>''
                GROUP BY prefecture,city ORDER BY prefecture,city
            """)
            city_stats={};parent_stats={}
            for pref,raw_city,total,open_count,close_count in cur.fetchall():
                city=(raw_city or '').strip()
                if city.startswith(pref):city=city[len(pref):]
                if not city_is_indexable(pref,city):continue
                vals=city_stats.setdefault((pref,city),[0,0,0])
                vals[0]+=int(total or 0);vals[1]+=int(open_count or 0);vals[2]+=int(close_count or 0)
                m=re.fullmatch(r"(.+市).+区",city)
                if m:
                    vals=parent_stats.setdefault((pref,m.group(1)),[0,0,0])
                    vals[0]+=int(total or 0);vals[1]+=int(open_count or 0);vals[2]+=int(close_count or 0)
            for mapping in (city_stats,parent_stats):
                for (pref,city),(total,open_count,close_count) in sorted(mapping.items()):
                    if total<2 or not city_is_indexable(pref,city):continue
                    base=f"/area/{qpath(pref)}/{qpath(city)}";add(base)
                    if open_count>=3:add(base+"?status=opening")
                    if close_count>=3:add(base+"?status=closing")

            cur.execute(f"""
                SELECT COALESCE(category,'業種未分類'),COUNT(*),
                       COUNT(*) FILTER(WHERE status IN('open','opening')),
                       COUNT(*) FILTER(WHERE status IN('closed','closing'))
                FROM stores WHERE {public_filter}
                GROUP BY COALESCE(category,'業種未分類') ORDER BY 1
            """)
            for cat,total,open_count,close_count in cur.fetchall():
                base=f"/category/{qpath(cat)}";add(base)
                if int(open_count or 0)>=5:add(base+"?status=opening")
                if int(close_count or 0)>=5:add(base+"?status=closing")

            remain=max(0,max_urls-len(urls))
            cur.execute(f"""
                SELECT id,updated_at FROM stores WHERE {public_filter}
                ORDER BY updated_at DESC NULLS LAST,id DESC LIMIT %s
            """,(remain,))
            store_rows=cur.fetchall()

    entries=[f"<url><loc>{html.escape(u)}</loc></url>" for u in urls[:max_urls]]
    for sid,updated in store_rows:
        u=site_url(origin,f"/store/{sid}")
        lm=f"<lastmod>{updated.date().isoformat()}</lastmod>" if updated else ""
        entries.append(f"<url><loc>{html.escape(u)}</loc>{lm}</url>")
    return '<?xml version="1.0" encoding="UTF-8"?>' + \\
           '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">' + \\
           "".join(entries[:max_urls]) + '</urlset>'
'''
s=s[:start]+new_sitemap+s[end:]
p.write_text(s)
print('patched',p)
