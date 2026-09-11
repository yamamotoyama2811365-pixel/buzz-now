"""Separate X feed for store closures + corporate bankruptcies.

Disabled until a dedicated Buffer channel is configured. Reuses the existing
Buffer API key but never the BUZZ NOW X channel id. Posts only factual fields
already published by the two sites; no generated claims or inferred causes.
"""
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

JST = timezone(timedelta(hours=9))
SLOTS = ((8,15,'close'), (10,30,'bankruptcy'), (13,0,'close'),
         (15,30,'close'), (18,15,'bankruptcy'), (21,15,'close'))
WINDOW_MINUTES = 25
PREFIX = 'citycorp_x:'


def config():
    return {
        'enabled': os.getenv('CITY_CORP_X_ENABLED','false').lower() == 'true',
        'channel_id': os.getenv('CITY_CORP_X_BUFFER_CHANNEL_ID','').strip(),
        'open_close_dsn': os.getenv('OPEN_CLOSE_DATABASE_URL','').strip(),
        'corporate_dsn': os.getenv('CORPORATE_DATABASE_URL','').strip(),
        'daily_cap': max(1, min(int(os.getenv('CITY_CORP_X_DAILY_CAP','6')), 10)),
    }


def current_slot(now):
    local=now.astimezone(JST)
    for h,m,kind in SLOTS:
        start=local.replace(hour=h,minute=m,second=0,microsecond=0)
        if start <= local < start+timedelta(minutes=WINDOW_MINUTES):
            return {'key':start.strftime('%Y-%m-%dT%H:%M'),'kind':kind,'start':start}
    return None


def _state_get(c,key):
    row=c.execute('SELECT value FROM system_state WHERE key=?',(PREFIX+key,)).fetchone()
    return row['value'] if row else None


def _state_set(c,key,value):
    c.execute('INSERT INTO system_state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',(PREFIX+key,str(value)))


def _close_candidate(dsn, seen):
    if not dsn:return None
    import psycopg
    from psycopg.rows import dict_row
    with psycopg.connect(dsn,row_factory=dict_row,connect_timeout=5) as con, con.cursor() as cur:
        cur.execute("""SELECT id,name,status,prefecture,city,close_date,updated_at
          FROM stores
          WHERE status IN ('closing','closed') AND COALESCE(status,'')<>'excluded'
            AND updated_at >= now()-interval '10 days'
            AND close_date BETWEEN (now() AT TIME ZONE 'Asia/Tokyo')::date-10 AND (now() AT TIME ZONE 'Asia/Tokyo')::date+180
            AND COALESCE(TRIM(name),'')<>'' AND COALESCE(TRIM(source_url),'')<>''
          ORDER BY COALESCE(close_date, updated_at::date) DESC, updated_at DESC
          LIMIT 40""")
        for r in cur.fetchall():
            key='close:'+str(r['id'])
            if key not in seen:return key,dict(r)
    return None


def _bankruptcy_candidate(dsn, seen):
    if not dsn:return None
    import psycopg2
    from psycopg2.extras import RealDictCursor
    con=psycopg2.connect(dsn,connect_timeout=5)
    try:
        with con.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT id,company,prefecture,industry,reported_date,payload
              FROM corporate_events
              WHERE published AND kind='bankruptcy'
                AND company NOT IN ('運営会社','老舗','同社','会社','企業','事業者','飲食店','店舗')
                AND reported_date BETWEEN (now() AT TIME ZONE 'Asia/Tokyo')::date-10 AND (now() AT TIME ZONE 'Asia/Tokyo')::date
                AND COALESCE(payload->>'source_url','')<>''
              ORDER BY reported_date DESC,id DESC LIMIT 40""")
            for r in cur.fetchall():
                key='bankruptcy:'+str(r['id'])
                if key not in seen:return key,dict(r)
    finally:con.close()
    return None


def _hashtags(*parts):
    tags=[]
    for value in parts:
        v=''.join(str(value or '').split())
        if 1 <= len(v) <= 12 and v not in tags:tags.append(v)
    return ' '.join('#'+x for x in tags[:3])


def close_text(r):
    area=' '.join(x for x in (r.get('prefecture'),r.get('city')) if x) or '地域確認中'
    label='閉店予定' if r.get('status')=='closing' else '閉店情報'
    lines=[f'【{label}｜{area}】',str(r.get('name') or '店舗名未確認')]
    if r.get('close_date'):lines.append('閉店日：'+r['close_date'].strftime('%Y/%m/%d'))
    lines.append('詳細・出典 → https://open-close-map.onrender.com/store/'+str(r['id']))
    lines.append(_hashtags('閉店',r.get('prefecture'),r.get('city')))
    return '\n'.join(x for x in lines if x)


def bankruptcy_text(r):
    p=r.get('payload') or {}
    pref=r.get('prefecture') or '地域確認中'
    company=r.get('company') or p.get('company') or '会社名確認中'
    stage=str(p.get('stage') or '倒産事案を掲載')
    lines=[f'【倒産速報｜{pref}】',company,stage]
    if r.get('reported_date'):lines.append('報道日：'+r['reported_date'].strftime('%Y/%m/%d'))
    lines.append('詳細・出典 → https://buzz-now-1.onrender.com/corporate/company/'+str(r['id']))
    lines.append(_hashtags('倒産',pref,r.get('industry')))
    return '\n'.join(x for x in lines if x)


def run(shared_db, sender, now=None):
    cfg=config(); now=now or datetime.now(timezone.utc)
    if not cfg['enabled']:
        return {'sent':0,'reason':'CITY_CORP_X_ENABLED=false'}
    if not cfg['channel_id']:
        return {'sent':0,'reason':'dedicated_buffer_channel_missing'}
    slot=current_slot(now)
    if not slot:return {'sent':0,'reason':'outside_posting_window'}
    with shared_db() as c:
        # CITYCORP_DURABLE_ACTIVATION: serialize reservation across workers.
        if hasattr(c, '_con'):
            c.execute('SELECT pg_advisory_xact_lock(3544102)')
        used=c.execute("SELECT COUNT(*) AS n FROM system_state WHERE key LIKE ? AND value<>'no_candidate'",(PREFIX+'slot:'+now.astimezone(JST).date().isoformat()+'%',)).fetchone()['n']
        if used >= cfg['daily_cap']:return {'sent':0,'reason':'daily_cap_reached'}
        if _state_get(c,'slot:'+slot['key']):return {'sent':0,'reason':'slot_already_attempted'}
        cutoff=(now.astimezone(JST).date()-timedelta(days=1)).isoformat()
        # keep a small durable sent-key set in system_state; no external-user data is stored.
        rows=c.execute("SELECT key FROM system_state WHERE key LIKE ?",(PREFIX+'sent:%',)).fetchall()
        seen={r['key'].removeprefix(PREFIX+'sent:') for r in rows}
        candidate=_close_candidate(cfg['open_close_dsn'],seen) if slot['kind']=='close' else _bankruptcy_candidate(cfg['corporate_dsn'],seen)
        _state_set(c,'slot:'+slot['key'],'reserved');c.commit()
        if not candidate:
            _state_set(c,'slot:'+slot['key'],'no_candidate');c.commit()
            return {'sent':0,'reason':'no_fresh_candidate','kind':slot['kind']}
        key,row=candidate
        text=close_text(row) if slot['kind']=='close' else bankruptcy_text(row)
        # X limit is 280 characters; keep link/hashtags but never truncate factual names into ambiguity.
        if len(text)>275:
            text='\n'.join(text.splitlines()[:3])+'\n'+('詳細 → https://open-close-map.onrender.com/store/'+str(row['id']) if slot['kind']=='close' else '詳細 → https://buzz-now-1.onrender.com/corporate/company/'+str(row['id']))
        # Reserve the item before submitting. An uncertain response must not
        # cause the same bankruptcy/closure to be sent again in another slot.
        _state_set(c,'sent:'+key,'pending:'+now.isoformat());c.commit()
        try:
            result=sender(cfg['channel_id'],text,'','shareNow')
        except Exception:
            _state_set(c,'slot:'+slot['key'],'uncertain');c.commit()
            return {'sent':0,'reason':'submission_uncertain','kind':slot['kind']}

        if result.get('ok'):
            _state_set(c,'sent:'+key,now.isoformat())
            _state_set(c,'slot:'+slot['key'],'sent');c.commit()
            return {'sent':1,'kind':slot['kind'],'slot':slot['key'],'post_id':result.get('post_id'),'source_key':key}
        _state_set(c,'slot:'+slot['key'],'failed:'+str(result.get('reason','unknown'))[:100]);c.commit()
        return {'sent':0,'reason':result.get('reason','buffer_submission_failed'),'kind':slot['kind']}


def status():
    cfg=config()
    return {'enabled':cfg['enabled'],'channel_configured':bool(cfg['channel_id']),
            'sources':{'open_close':bool(cfg['open_close_dsn']),'corporate':bool(cfg['corporate_dsn'])},
            'daily_cap':cfg['daily_cap'],'timezone':'Asia/Tokyo',
            'slots':[{'time':f'{h:02d}:{m:02d}','kind':kind} for h,m,kind in SLOTS],
            'account_role':'街と企業の変化速報（X専用）'}
