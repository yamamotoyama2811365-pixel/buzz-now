"""Fail-closed activation for @machi_to_kigyo; no credentials in status/logs."""
import json
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from . import city_corporate_social as feed

EXPECTED_CHANNEL = '6aa41ee3cd8b9c702c4e120d'
EXPECTED_HANDLE = 'machi_to_kigyo'
_cache = {}
_lock = threading.Lock()
_last_run = {}
# Count only our generated, ASCII detail URLs as t.co links. Other text is
# counted conservatively (non-ASCII as two) rather than risking overlong posts.
_LINK = re.compile(r'https://(?:open-close-map\.onrender\.com/store/[0-9]+|buzz-now-1\.onrender\.com/corporate/company/[a-zA-Z0-9_-]+)(?=\s|$)')


def weighted_length(text):
    length = 0
    start = 0
    for match in _LINK.finditer(text):
        length += sum(1 if ord(c) < 128 else 2 for c in text[start:match.start()]) + 23
        start = match.end()
    return length + sum(1 if ord(c) < 128 else 2 for c in text[start:])


def fit_text(text):
    if weighted_length(text) <= 280:
        return text
    # Remove only an optional final hashtag line; never truncate a business
    # name, a date, a legal-procedure label, or the destination link.
    lines = text.splitlines()
    if lines and lines[-1].startswith('#'):
        text = '\n'.join(lines[:-1])
    return text if weighted_length(text) <= 280 else None


def channel_check(graphql, channel_id):
    if channel_id != EXPECTED_CHANNEL:
        return {'ok': False, 'reason': 'dedicated_channel_mismatch'}
    now = time.monotonic()
    cached = _cache.get(channel_id)
    if cached and cached['expires'] > now:
        return cached['result']
    query = 'query CityCorporateChannel { channel(input: { id: ' + json.dumps(channel_id) + ' }) { id name service isDisconnected isLocked isQueuePaused } }'
    try:
        result = graphql(query)
        channel = ((result.get('data') or {}).get('channel') or {})
        handle = str(channel.get('name') or '').lstrip('@').lower()
        ok = (result.get('ok') is True and channel.get('id') == EXPECTED_CHANNEL
              and handle == EXPECTED_HANDLE and channel.get('service') == 'twitter'
              and channel.get('isDisconnected') is False
              and channel.get('isLocked') is False
              and channel.get('isQueuePaused') is False)
        safe = {'ok': ok, 'reason': 'verified' if ok else 'channel_not_ready',
                'handle_matches': handle == EXPECTED_HANDLE,
                'id_matches': channel.get('id') == EXPECTED_CHANNEL,
                'service': channel.get('service'),
                'disconnected': channel.get('isDisconnected'),
                'locked': channel.get('isLocked'),
                'queue_paused': channel.get('isQueuePaused'),
                'checked_at': datetime.now(timezone.utc).isoformat()}
        if result.get('status_code') in (401, 403, 429):
            safe['http_status'] = result['status_code']
    except Exception:
        safe = {'ok': False, 'reason': 'channel_lookup_failed',
                'checked_at': datetime.now(timezone.utc).isoformat()}
    # At most four successful channel lookups per day. Failures back off.
    _cache[channel_id] = {'result': safe, 'expires': now + (21600 if safe['ok'] else 600)}
    return safe


def run(shared_db, sender, graphql):
    global _last_run
    cfg = feed.config()
    if not cfg['enabled']:
        _last_run = {'sent': 0, 'reason': 'CITY_CORP_X_ENABLED=false'}
        return _last_run
    if not _lock.acquire(blocking=False):
        return {'sent': 0, 'reason': 'worker_busy'}
    try:
        ready = channel_check(graphql, cfg['channel_id'])
        if not ready['ok']:
            _last_run = {'sent': 0, 'reason': ready['reason']}
            return _last_run
        if not cfg['open_close_dsn'] or not cfg['corporate_dsn']:
            _last_run = {'sent': 0, 'reason': 'source_database_missing'}
            return _last_run
        def guarded_sender(channel_id, text, image_url, mode):
            if channel_id != EXPECTED_CHANNEL:
                return {'ok': False, 'reason': 'dedicated_channel_mismatch'}
            fitted = fit_text(text)
            if not fitted:
                return {'ok': False, 'reason': 'post_exceeds_weighted_limit'}
            return sender(channel_id, fitted, image_url, mode)
        raw = feed.run(shared_db, guarded_sender)
        _last_run = {k: raw[k] for k in ('sent', 'reason', 'kind', 'slot', 'post_id', 'source_key') if k in raw}
        _last_run['checked_at'] = datetime.now(timezone.utc).isoformat()
        _last_run['publication_verified'] = False
        return _last_run
    except Exception:
        _last_run = {'sent': 0, 'reason': 'worker_error',
                     'checked_at': datetime.now(timezone.utc).isoformat()}
        return _last_run
    finally:
        _lock.release()


def status():
    cfg = feed.config()
    now = datetime.now(feed.JST)
    slots = []
    for offset in (0, 1):
        day = now + timedelta(days=offset)
        for h, m, kind in feed.SLOTS:
            start = day.replace(hour=h, minute=m, second=0, microsecond=0)
            if start + timedelta(minutes=feed.WINDOW_MINUTES) > now:
                slots.append({'start_jst': start.isoformat(), 'kind': kind})
    verification = (_cache.get(cfg['channel_id']) or {}).get('result')
    return {**feed.status(), 'handle': EXPECTED_HANDLE,
            'channel_id_suffix': cfg['channel_id'][-6:],
            'channel_id_matches': cfg['channel_id'] == EXPECTED_CHANNEL,
            'channel_verification': verification,
            'last_run': dict(_last_run),
            'next_slot': slots[0] if slots else None,
            'note': 'sent denotes Buffer acceptance, not verified X publication; slots without eligible facts are skipped.'}
