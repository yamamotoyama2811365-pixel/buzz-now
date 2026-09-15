"""Add privacy-preserving aggregate click tracking for internal magazine links."""
from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[1]
PY = ROOT / 'app/visitor_analytics.py'
JS = ROOT / 'static/visitor-analytics.js'

CLICK_SCHEMA = '''CLICK_SCHEMA = """CREATE TABLE IF NOT EXISTS visitor_internal_clicks_daily (\n    day TEXT NOT NULL,\n    source_path TEXT NOT NULL,\n    target_path TEXT NOT NULL,\n    placement TEXT NOT NULL CHECK (placement IN ('magazine_stream','magazine_more','article_top')),\n    is_test INTEGER NOT NULL DEFAULT 0 CHECK (is_test IN (0,1)),\n    clicks BIGINT NOT NULL DEFAULT 0 CHECK (clicks >= 0),\n    PRIMARY KEY (day, source_path, target_path, placement, is_test)\n)"""\nPLACEMENTS = frozenset(('magazine_stream', 'magazine_more', 'article_top'))\n\n'''

RECORD_FN = '''\n\ndef record_internal_click(db, source_path: str, target_path: str, placement: str, is_test: bool, now=None):\n    now = now or datetime.now(timezone.utc)\n    if now.tzinfo is None:\n        raise ValueError('timezone required')\n    if placement not in PLACEMENTS or type(is_test) is not bool:\n        raise ValueError('invalid event')\n    source_path = normalized_path(source_path)\n    target_path = normalized_path(target_path)\n    if not source_path.startswith('/trend/'):\n        raise ValueError('invalid source page')\n    with managed_connection(db) as connection:\n        if not connection.execute('SELECT slug FROM trends WHERE slug=? LIMIT 1', (source_path[7:],)).fetchone():\n            return False\n        if target_path.startswith('/trend/') and not connection.execute('SELECT slug FROM trends WHERE slug=? LIMIT 1', (target_path[7:],)).fetchone():\n            return False\n        connection.execute(\n            \"\"\"INSERT INTO visitor_internal_clicks_daily (day, source_path, target_path, placement, is_test, clicks)\n               VALUES (?, ?, ?, ?, ?, 1)\n               ON CONFLICT (day, source_path, target_path, placement, is_test)\n               DO UPDATE SET clicks = visitor_internal_clicks_daily.clicks + 1\"\"\",\n            (now.astimezone(JST).date().isoformat(), source_path, target_path, placement, int(is_test)),\n        )\n        connection.commit()\n    return True\n'''

ENDPOINT = '''\n\n    @app.post('/api/visitor-analytics/internal-click', include_in_schema=False)\n    async def analytics_internal_click(request: Request):\n        if not enabled:\n            return Response(status_code=204)\n        if request.headers.get('origin', '').rstrip('/') not in allowed:\n            return Response(status_code=403)\n        if request.headers.get('sec-fetch-site') not in (None, 'same-origin'):\n            return Response(status_code=403)\n        if request.headers.get('dnt') == '1' or request.headers.get('sec-gpc') == '1':\n            return Response(status_code=204)\n        agent = request.headers.get('user-agent', '')\n        if not agent or BOT.search(agent):\n            return Response(status_code=204)\n        if request.headers.get('content-type', '').split(';')[0].strip() != 'application/json':\n            return Response(status_code=415)\n        body = bytearray()\n        async for chunk in request.stream():\n            body.extend(chunk)\n            if len(body) > 4096:\n                return Response(status_code=413)\n        try:\n            payload = json.loads(body)\n            if not isinstance(payload, dict) or set(payload) != {'source_path','target_path','placement','test','nonce'}:\n                raise ValueError('invalid fields')\n            source_path = normalized_path(payload['source_path'])\n            target_path = normalized_path(payload['target_path'])\n            placement = payload['placement']\n            is_test = payload['test']\n            nonce = payload['nonce']\n            if placement not in PLACEMENTS or type(is_test) is not bool:\n                raise ValueError('invalid values')\n            if not isinstance(nonce, str) or not re.fullmatch(r'[a-zA-Z0-9-]{16,64}', nonce):\n                raise ValueError('invalid nonce')\n        except (ValueError, TypeError, UnicodeError):\n            return Response(status_code=400)\n        if not ready:\n            return Response(status_code=503, headers={'Retry-After':'60'})\n        dedupe_key = f'click:{nonce}'\n        async with lock:\n            timestamp = time.monotonic()\n            while seen and next(iter(seen.values())) < timestamp - 600:\n                seen.popitem(last=False)\n            if dedupe_key in seen:\n                return Response(status_code=204)\n            try:\n                accepted = await run_in_threadpool(record_internal_click, db, source_path, target_path, placement, is_test)\n            except Exception as exc:\n                logger.warning('Internal click analytics write unavailable: %s', type(exc).__name__)\n                return Response(status_code=503)\n            if not accepted:\n                return Response(status_code=400)\n            seen[dedupe_key] = timestamp\n            if len(seen) > 4096:\n                seen.popitem(last=False)\n        return Response(status_code=204, headers={'Cache-Control':'no-store'})\n'''

JS_ADD = r'''\n  document.addEventListener('click', (event) => {\n    const link = event.target && event.target.closest ? event.target.closest('a[data-reading-placement]') : null;\n    if (!link || navigator.webdriver || navigator.doNotTrack === '1' || navigator.globalPrivacyControl) return;\n    let target;\n    try { target = new URL(link.href, location.href); } catch (_) { return; }\n    if (target.origin !== location.origin) return;\n    const sourcePath = location.pathname;\n    if (!sourcePath.startsWith('/trend/')) return;\n    const placement = link.dataset.readingPlacement || '';\n    if (!['magazine_stream','magazine_more','article_top'].includes(placement)) return;\n    const q = new URLSearchParams(location.search);\n    const nonce = crypto.randomUUID ? crypto.randomUUID() : Array.from(crypto.getRandomValues(new Uint32Array(4)), v => v.toString(16).padStart(8, '0')).join('');\n    fetch('/api/visitor-analytics/internal-click', {\n      method: 'POST', mode: 'cors', credentials: 'omit', referrerPolicy: 'no-referrer', keepalive: true,\n      headers: {'Content-Type':'application/json'},\n      body: JSON.stringify({source_path: sourcePath, target_path: target.pathname, placement, test: q.get('bn_analytics_test') === '1', nonce})\n    }).catch(() => {});\n  }, {capture: true});\n'''


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise ValueError('integration anchor missing or ambiguous')
    return text.replace(old, new, 1)


def apply_python():
    text = PY.read_text()
    if 'visitor_internal_clicks_daily' in text:
        return
    text = replace_once(text, 'NETWORK_SITES = {\n', CLICK_SCHEMA + 'NETWORK_SITES = {\n')
    text = replace_once(text, '        connection.execute(STATE_SCHEMA)\n', '        connection.execute(STATE_SCHEMA)\n        connection.execute(CLICK_SCHEMA)\n')
    text = replace_once(text, '\ndef install_visitor_analytics(app, db, is_legacy=False):\n', RECORD_FN + '\ndef install_visitor_analytics(app, db, is_legacy=False):\n')
    text = replace_once(text, "    @app.post('/api/network-analytics/pageview', include_in_schema=False)\n", ENDPOINT + "\n    @app.post('/api/network-analytics/pageview', include_in_schema=False)\n")
    ast.parse(text)
    PY.write_text(text)


def apply_js():
    text = JS.read_text()
    if "'/api/visitor-analytics/internal-click'" in text:
        return
    text = replace_once(text, "  document.addEventListener('prerenderingchange', send);\n", "  document.addEventListener('prerenderingchange', send);\n" + JS_ADD)
    JS.write_text(text)


if __name__ == '__main__':
    apply_python()
    apply_js()
    print('Installed aggregate magazine click tracking')
