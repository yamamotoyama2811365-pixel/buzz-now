from pathlib import Path

path = Path('app/migration_control.py')
text = path.read_text()

marker = 'class MigrationMaintenance:\n'
if '_legacy_x_imobile_gate' not in text:
    helper = r'''
async def _legacy_x_imobile_gate(scope, send):
    """Serve i-mobile only on the approved legacy host for X-origin entry URLs.

    The gate is intentionally noindex, never requires an ad click, and always
    exposes an immediate article button. A one-hour first-party cookie avoids
    showing the same visitor the gate on every X click.
    """
    if scope.get('type') != 'http':
        return False

    method = (scope.get('method') or 'GET').upper()
    path = scope.get('path') or '/'
    query = (scope.get('query_string') or b'').decode('ascii', 'ignore')
    headers = {k.lower(): v for k, v in (scope.get('headers') or [])}
    host = headers.get(b'host', b'').decode('ascii', 'ignore').split(':', 1)[0].lower()
    legacy = host == 'buzz-now.onrender.com' or os.getenv('RENDER_SERVICE_ID', '') == 'srv-daa321mk1f9s73fbjfcg'
    if not legacy:
        return False

    is_short = bool(re.fullmatch(r'/t/\d+', path))
    is_x_trend = path.startswith('/trend/') and ('utm_source=x' in query or 'source=x' in query)
    if not (is_short or is_x_trend):
        return False

    raw_path = scope.get('raw_path') or path.encode('utf-8')
    try:
        raw_path_text = raw_path.decode('ascii')
    except UnicodeDecodeError:
        from urllib.parse import quote
        raw_path_text = quote(path, safe='/-_%')

    destination = (
        'https://buzz-now-1.onrender.com' + raw_path_text
        + '?utm_source=x&utm_medium=social&utm_campaign=prebuzz&utm_content=legacy_imobile_gate'
    )

    if method == 'HEAD':
        await send({'type': 'http.response.start', 'status': 302, 'headers': [
            (b'location', destination.encode('ascii')),
            (b'cache-control', b'no-store'),
            (b'x-robots-tag', b'noindex, nofollow'),
        ]})
        await send({'type': 'http.response.body', 'body': b''})
        return True

    ad_html = os.getenv('IMOBILE_X_GATE_HTML', '').strip()
    enabled = os.getenv('IMOBILE_X_GATE_ENABLED', 'true').lower() == 'true'
    if not enabled or not ad_html:
        await send({'type': 'http.response.start', 'status': 302, 'headers': [
            (b'location', destination.encode('ascii')),
            (b'cache-control', b'no-store'),
            (b'x-robots-tag', b'noindex, nofollow'),
        ]})
        await send({'type': 'http.response.body', 'body': b''})
        return True

    cookie = headers.get(b'cookie', b'').decode('latin1', 'ignore')
    if 'buzznow_x_gate_seen=1' in cookie:
        await send({'type': 'http.response.start', 'status': 302, 'headers': [
            (b'location', destination.encode('ascii')),
            (b'cache-control', b'no-store'),
            (b'x-robots-tag', b'noindex, nofollow'),
        ]})
        await send({'type': 'http.response.body', 'body': b''})
        return True

    dest_json = json.dumps(destination, ensure_ascii=True)
    page = f'''<!doctype html>
<html lang="ja"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="robots" content="noindex,nofollow"><title>BUZZ NOW｜記事を開く</title>
<style>
*{{box-sizing:border-box}}html,body{{margin:0;background:#07090d;color:#f7f8fb;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans JP",sans-serif}}
body{{min-height:100vh;display:flex;align-items:center;justify-content:center;padding:18px}}
.gate{{width:min(100%,520px);text-align:center}}.brand{{font-size:13px;letter-spacing:.18em;font-weight:900;opacity:.72;margin-bottom:18px}}
h1{{font-size:22px;line-height:1.45;margin:0 0 8px}}.lead{{font-size:13px;line-height:1.75;opacity:.72;margin:0 auto 16px;max-width:430px}}
.adlabel{{font-size:10px;letter-spacing:.12em;opacity:.42;margin:8px 0}}.adbox{{min-height:250px;border:1px solid rgba(255,255,255,.12);border-radius:16px;background:rgba(255,255,255,.035);display:flex;align-items:center;justify-content:center;padding:8px;overflow:hidden}}
.continue{{width:100%;margin-top:16px;border:0;border-radius:14px;padding:16px 18px;font-weight:900;font-size:15px;background:#f5f7fb;color:#080a0e;cursor:pointer}}
.note{{font-size:11px;line-height:1.65;opacity:.5;margin-top:10px}}
</style></head><body><main class="gate">
<div class="brand">BUZZ NOW</div><h1>話題の記事を開きます</h1>
<p class="lead">BUZZ NOWは広告掲載で運営しています。広告のクリックは不要です。記事は下のボタンからすぐ開けます。</p>
<div class="adlabel">ADVERTISEMENT</div><div class="adbox">{ad_html}</div>
<button class="continue" id="continueBtn">記事を読む →</button>
<p class="note">広告は任意です。ボタンを押すとBUZZ NOW本編へ移動します。</p>
</main><script>document.getElementById('continueBtn').addEventListener('click',()=>{{window.location.href={dest_json};}});</script></body></html>'''.encode('utf-8')

    await send({'type': 'http.response.start', 'status': 200, 'headers': [
        (b'content-type', b'text/html; charset=utf-8'),
        (b'cache-control', b'no-store'),
        (b'x-robots-tag', b'noindex, nofollow'),
        (b'set-cookie', b'buzznow_x_gate_seen=1; Max-Age=3600; Path=/; Secure; HttpOnly; SameSite=Lax'),
    ]})
    await send({'type': 'http.response.body', 'body': page})
    return True


'''
    if marker not in text:
        raise SystemExit('MigrationMaintenance marker not found')
    text = text.replace(marker, helper + marker, 1)

old = """        try:\n            if scope['type'] == 'http':\n                path = scope.get('path') or '/'\n                method = (scope.get('method') or 'GET').upper()\n"""
new = """        try:\n            if scope['type'] == 'http':\n                if await _legacy_x_imobile_gate(scope, send):\n                    return\n                path = scope.get('path') or '/'\n                method = (scope.get('method') or 'GET').upper()\n"""
if new not in text:
    if old not in text:
        raise SystemExit('Middleware insertion point not found')
    text = text.replace(old, new, 1)

path.write_text(text)
print('installed X-only legacy i-mobile gate')
