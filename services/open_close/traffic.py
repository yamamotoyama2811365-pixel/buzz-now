"""Open Close Map only: aggregate PVs, no visitor identity or raw referrer.

Schema is applied explicitly (migrations/open_close_pv.sql), never at import.
Public writes are origin/shape/path checked and globally rate-limited. This is
an approximate PV counter, not unique visitors, sessions, or fraud-proof data.
"""
import json
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote, urlsplit

from fastapi import Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from starlette.concurrency import run_in_threadpool

JST = timezone(timedelta(hours=9))
SOURCES = frozenset({'google', 'yahoo', 'bing', 'direct_or_unknown', 'referral', 'internal'})
STATIC = frozenset({'/', '/open/', '/close/', '/about.html', '/privacy.html', '/contact.html'})
BOT = re.compile(r'bot|spider|crawl|headless|lighthouse|preview|python|curl|wget', re.I)
NO_CACHE = {'Cache-Control': 'no-store', 'X-Robots-Tag': 'noindex'}


def japan_day():
    return datetime.now(JST).date()


def normalize_page(value):
    if not isinstance(value, str) or len(value) > 900:
        raise ValueError('Invalid page')
    # Do not accept or store any query strings, fragments or absolute URLs.
    if not value.startswith('/') or value.startswith('//') or '?' in value or '#' in value:
        raise ValueError('Invalid page')
    page = unquote(value, errors='strict')
    if len(page) > 250 or re.search(r'[\x00-\x1f\x7f?#%\\<>]', page) or '..' in page or '//' in page:
        raise ValueError('Invalid page')
    if page == '/index.html':
        return '/'
    if page.rstrip('/') in ('/open', '/close'):
        return page.rstrip('/') + '/'
    if page in STATIC:
        return page
    page = page.rstrip('/')
    if re.fullmatch(r'/store/[1-9][0-9]{0,17}', page):
        return page
    if re.fullmatch(r'/(area|category)/[^/]{1,100}(?:/[^/]{1,100})?', page):
        return page
    raise ValueError('Untracked page')


def validate_payload(data):
    if not isinstance(data, dict) or set(data) - {'page', 'source', 'is_test'}:
        raise ValueError('Invalid payload')
    page = normalize_page(data.get('page'))
    source = data.get('source')
    if not isinstance(source, str) or source not in SOURCES:
        raise ValueError('Invalid source')
    is_test = data.get('is_test', False)
    if type(is_test) is not bool:
        raise ValueError('Invalid test marker')
    return page, source, is_test


class Budget:
    """Process-wide safeguard only; no per-person, IP or browser identifiers."""
    def __init__(self):
        self.lock = threading.Lock()
        self.tokens = 120.0
        self.updated = time.monotonic()

    def take(self):
        with self.lock:
            now = time.monotonic()
            self.tokens = min(120.0, self.tokens + (now - self.updated) * 2)
            self.updated = now
            if self.tokens < 1:
                return False
            self.tokens -= 1
            return True


def _connect(dsn):
    import psycopg
    if not dsn:
        raise RuntimeError('PV database unavailable')
    return psycopg.connect(dsn, connect_timeout=5)


def _exists(cur, page):
    if page in STATIC:
        return True
    parts = page.strip('/').split('/')
    if parts[0] == 'store':
        cur.execute("SELECT EXISTS(SELECT 1 FROM stores WHERE id=%s AND COALESCE(status,'')<>'excluded')", [int(parts[1])])
    elif parts[0] == 'area':
        sql = "SELECT EXISTS(SELECT 1 FROM stores WHERE prefecture=%s AND COALESCE(status,'')<>'excluded'"
        args = [parts[1]]
        if len(parts) == 3:
            sql += ' AND city=%s'
            args.append(parts[2])
        cur.execute(sql + ')', args)
    elif parts[0] == 'category' and len(parts) == 2:
        cur.execute("SELECT EXISTS(SELECT 1 FROM stores WHERE category=%s AND COALESCE(status,'')<>'excluded')", [parts[1]])
    else:
        return False
    return bool(cur.fetchone()[0])


def record(dsn, page, source, is_test):
    with _connect(dsn) as conn, conn.cursor() as cur:
        # Transaction-local setting is compatible with Neon/PgBouncer pooling.
        cur.execute('SET LOCAL statement_timeout = 3000')
        if not _exists(cur, page):
            return False
        cur.execute('''INSERT INTO public.ocm_page_views_daily(day,page,source,is_test,views)
                       VALUES (%s,%s,%s,%s,1)
                       ON CONFLICT(day,page,source,is_test)
                       DO UPDATE SET views=ocm_page_views_daily.views+1''',
                    [japan_day(), page, source, is_test])
    return True


def report(dsn, days=30, include_today=False):
    today = japan_day()
    end = today if include_today else today - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    where = 'is_test=FALSE AND day BETWEEN %s AND %s'
    args = [start, end]
    with _connect(dsn) as conn, conn.cursor() as cur:
        # Transaction-local setting is compatible with Neon/PgBouncer pooling.
        cur.execute('SET LOCAL statement_timeout = 3000')
        cur.execute('SELECT started_at FROM public.ocm_pv_metadata WHERE singleton=TRUE')
        row = cur.fetchone()
        started_at = row[0].isoformat() if row else None
        cur.execute('SELECT COALESCE(SUM(views),0) FROM public.ocm_page_views_daily WHERE ' + where, args)
        total = int(cur.fetchone()[0])
        cur.execute('SELECT day,SUM(views) FROM public.ocm_page_views_daily WHERE ' + where + ' GROUP BY day ORDER BY day', args)
        actual_days = {r[0].isoformat(): int(r[1]) for r in cur.fetchall()}
        cur.execute('SELECT source,SUM(views) FROM public.ocm_page_views_daily WHERE ' + where + ' GROUP BY source ORDER BY SUM(views) DESC', args)
        sources = [{'source': r[0], 'views': int(r[1])} for r in cur.fetchall()]
        cur.execute('SELECT page,SUM(views) FROM public.ocm_page_views_daily WHERE ' + where + ' GROUP BY page ORDER BY SUM(views) DESC,page LIMIT 101', args)
        pages = [{'page': r[0], 'views': int(r[1])} for r in cur.fetchall()]
    by_day = [{'day': (start + timedelta(days=i)).isoformat(), 'views': actual_days.get((start + timedelta(days=i)).isoformat(), 0)} for i in range(days)]
    return {'site': 'open-close-map', 'metric': 'first_party_page_views', 'timezone': 'Asia/Tokyo',
            'start_date': start.isoformat(), 'end_date': end.isoformat(), 'includes_partial_today': include_today,
            'measurement_started_at': started_at, 'views': total, 'by_day': by_day, 'by_source': sources,
            'by_page': pages[:100], 'pages_truncated': len(pages) > 100, 'tests_excluded': True,
            'notes': 'PV estimates; no unique users/sessions. Sources describe each page referrer, not session attribution. Missing referrers include privacy-restricted traffic. Dates before measurement started are not measured.'}


def install(app, dsn, public_origin, require_admin):
    if getattr(app.state, 'ocm_pv_installed', False):
        return
    app.state.ocm_pv_installed = True
    expected = public_origin.rstrip('/')
    parsed = urlsplit(expected)
    if parsed.scheme != 'https' or parsed.path:
        raise ValueError('PV origin must be an HTTPS origin')
    budget = Budget()

    @app.post('/api/page-view', include_in_schema=False)
    async def page_view(request: Request):
        if request.headers.get('origin') != expected:
            raise HTTPException(403, 'Origin not permitted')
        if request.headers.get('dnt') == '1' or request.headers.get('sec-gpc') == '1':
            return Response(status_code=204, headers=NO_CACHE)
        if 'prefetch' in (request.headers.get('purpose', '') + request.headers.get('sec-purpose', '')).lower():
            return Response(status_code=204, headers=NO_CACHE)
        if request.headers.get('content-type', '').split(';')[0].strip().lower() != 'application/json':
            raise HTTPException(415, 'JSON required')
        if not budget.take():
            raise HTTPException(429, 'Counter busy', headers={'Retry-After': '30'})
        chunks = bytearray()
        async for chunk in request.stream():
            chunks.extend(chunk)
            if len(chunks) > 1024:
                raise HTTPException(413, 'Payload too large')
        try:
            page, source, is_test = validate_payload(json.loads(chunks))
        except (ValueError, TypeError, UnicodeError):
            raise HTTPException(422, 'Invalid counter payload') from None
        if BOT.search(request.headers.get('user-agent', '')) and not is_test:
            return Response(status_code=204, headers=NO_CACHE)
        try:
            accepted = await run_in_threadpool(record, dsn, page, source, is_test)
        except Exception:
            # No request bodies, headers, user agents, referrers, or secrets logged.
            raise HTTPException(503, 'Counter temporarily unavailable') from None
        return Response(status_code=204 if accepted else 422, headers=NO_CACHE)

    @app.get('/api/pv-report', dependencies=[Depends(require_admin)], include_in_schema=False)
    def pv_report(days: int = Query(30, ge=1, le=366), include_today: bool = False):
        try:
            result = report(dsn, days, include_today)
        except Exception:
            raise HTTPException(503, 'Counter report temporarily unavailable') from None
        return JSONResponse(result, headers=NO_CACHE)
