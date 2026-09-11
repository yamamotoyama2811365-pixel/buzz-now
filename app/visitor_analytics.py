"""First-party aggregate pageviews. Never mix these with traffic_totals models."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from urllib.parse import unquote, urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse, Response
from starlette.concurrency import run_in_threadpool

SOURCES = frozenset(('direct', 'internal', 'x', 'google', 'yahoo', 'bing', 'paid', 'referral', 'other'))
JST = timezone(timedelta(hours=9))
BOT = re.compile(r'bot|spider|crawler|headless|curl|wget|python|httpx|uptime|preview', re.I)
SCHEMA = """CREATE TABLE IF NOT EXISTS visitor_pageviews_daily (
    day TEXT NOT NULL,
    path TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('direct','internal','x','google','yahoo','bing','paid','referral','other')),
    is_test INTEGER NOT NULL DEFAULT 0 CHECK (is_test IN (0,1)),
    views BIGINT NOT NULL DEFAULT 0 CHECK (views >= 0),
    PRIMARY KEY (day, path, source, is_test)
)"""
STATE_SCHEMA = """CREATE TABLE IF NOT EXISTS visitor_analytics_state (
    key TEXT PRIMARY KEY, value TEXT NOT NULL
)"""
logger = logging.getLogger('buzz-now.visitor-analytics')


def normalized_path(value: str) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 1800:
        raise ValueError('invalid path')
    if '?' in value or '#' in value or any(ord(c) < 32 for c in value):
        raise ValueError('invalid path')
    value = unquote(value, errors='strict')
    if len(value) > 300 or '?' in value or '#' in value or any(ord(c) < 32 for c in value):
        raise ValueError('invalid path')
    if value != '/' and not re.fullmatch(r'/trend/[\w\-ぁ-んァ-ヶ一-龠々ー]+', value):
        raise ValueError('not a tracked page')
    return value


@contextmanager
def managed_connection(db):
    connection = db()
    try:
        with connection:
            yield connection
    finally:
        connection.close()


def initialize(db):
    with managed_connection(db) as connection:
        connection.execute(SCHEMA)
        connection.execute(STATE_SCHEMA)
        connection.execute(
            "INSERT INTO visitor_analytics_state (key, value) VALUES (?, ?) ON CONFLICT (key) DO NOTHING",
            ('started_at_utc', datetime.now(timezone.utc).isoformat()),
        )
        connection.commit()


def record_pageview(db, path: str, source: str, is_test: bool, now=None):
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        raise ValueError('timezone required')
    if source not in SOURCES or type(is_test) is not bool:
        raise ValueError('invalid event')
    path = normalized_path(path)
    with managed_connection(db) as connection:
        if path.startswith('/trend/'):
            found = connection.execute('SELECT slug FROM trends WHERE slug = ? LIMIT 1', (path[7:],)).fetchone()
            if not found:
                return False
        connection.execute(
            """INSERT INTO visitor_pageviews_daily (day, path, source, is_test, views)
               VALUES (?, ?, ?, ?, 1)
               ON CONFLICT (day, path, source, is_test)
               DO UPDATE SET views = visitor_pageviews_daily.views + 1""",
            (now.astimezone(JST).date().isoformat(), path, source, int(is_test)),
        )
        connection.commit()
    return True


def install_visitor_analytics(app, db, is_legacy=False):
    """Attach bounded ingestion only. Reports stay private in Neon, not public HTTP."""
    if getattr(app.state, 'visitor_analytics_installed', False):
        return
    app.state.visitor_analytics_installed = True
    enabled = not is_legacy and os.getenv('VISITOR_ANALYTICS_ENABLED', 'true').lower() == 'true'
    ready = False
    allowed = {'https://buzz-now-1.onrender.com'}
    site = os.getenv('SITE_URL', '').rstrip('/')
    if urlsplit(site).scheme in ('https', 'http') and urlsplit(site).netloc:
        allowed.add(f'{urlsplit(site).scheme}://{urlsplit(site).netloc}')
    # Per-document random nonce: bounded RAM only, never persisted or user-linked.
    seen = OrderedDict()
    lock = asyncio.Lock()

    @app.on_event('startup')
    async def analytics_startup():
        nonlocal ready
        if not enabled:
            return
        try:
            await run_in_threadpool(initialize, db)
            ready = True
        except Exception as exc:
            logger.warning('Analytics initialization unavailable: %s', type(exc).__name__)

    @app.get('/api/visitor-analytics/status', include_in_schema=False)
    async def analytics_status():
        return JSONResponse({'version': 1, 'enabled': enabled, 'ready': ready, 'timezone': 'Asia/Tokyo',
                             'measurement': 'browser_pageviews', 'reports': 'private_database'},
                            headers={'Cache-Control': 'no-store', 'X-Robots-Tag': 'noindex'})

    @app.post('/api/visitor-analytics/pageview', include_in_schema=False)
    async def analytics_pageview(request: Request):
        if not enabled:
            return Response(status_code=204)
        if request.headers.get('origin', '').rstrip('/') not in allowed:
            return Response(status_code=403)
        if request.headers.get('sec-fetch-site') not in (None, 'same-origin'):
            return Response(status_code=403)
        if request.headers.get('dnt') == '1' or request.headers.get('sec-gpc') == '1':
            return Response(status_code=204)
        agent = request.headers.get('user-agent', '')
        if not agent or BOT.search(agent):
            return Response(status_code=204)
        if request.headers.get('content-type', '').split(';')[0].strip() != 'application/json':
            return Response(status_code=415)
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > 4096:
                return Response(status_code=413)
        try:
            payload = json.loads(body)
            if not isinstance(payload, dict) or set(payload) != {'path', 'source', 'test', 'nonce'}:
                raise ValueError('invalid fields')
            path = normalized_path(payload['path'])
            source, is_test, nonce = payload['source'], payload['test'], payload['nonce']
            if not isinstance(source, str) or source not in SOURCES or type(is_test) is not bool:
                raise ValueError('invalid values')
            if not isinstance(nonce, str) or not re.fullmatch(r'[a-zA-Z0-9-]{16,64}', nonce):
                raise ValueError('invalid nonce')
        except (ValueError, TypeError, UnicodeError):
            return Response(status_code=400)
        if not ready:
            return Response(status_code=503, headers={'Retry-After': '60'})
        async with lock:
            timestamp = time.monotonic()
            while seen and next(iter(seen.values())) < timestamp - 600:
                seen.popitem(last=False)
            if nonce in seen:
                return Response(status_code=204)
            try:
                accepted = await run_in_threadpool(record_pageview, db, path, source, is_test)
            except Exception as exc:
                logger.warning('Analytics write unavailable: %s', type(exc).__name__)
                return Response(status_code=503)
            if not accepted:
                return Response(status_code=400)
            seen[nonce] = timestamp
            if len(seen) > 4096:
                seen.popitem(last=False)
        return Response(status_code=204, headers={'Cache-Control': 'no-store'})
