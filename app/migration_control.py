"""Reversible migration controls and database-outage protection.

This middleware also short-circuits cached social JPEG delivery. The legacy route in
main.py regenerates a JPEG from the much larger PNG source on every GET/HEAD. For a
metered remote database that needlessly transfers the PNG and rewrites the derivative.
Once a derivative exists, serve it directly; a cache miss falls through to the legacy
route so first-generation behavior remains unchanged.
"""
import base64
import json
import os
import re


def migration_settings(env):
    paused = env.get("MIGRATION_MAINTENANCE", "0") == "1"
    backend = env.get("DATABASE_BACKEND", "source").strip()
    if backend not in {"source", "neon"}:
        raise ValueError("Unsupported DATABASE_BACKEND")
    key = "NEON_DATABASE_URL" if backend == "neon" else "DATABASE_URL"
    url = env.get(key, "").strip()
    if backend == "neon" and not url:
        raise ValueError("NEON_DATABASE_URL is required for Neon mode")
    return paused, backend, url


def _is_database_unavailable(exc):
    """Return True only for connection/quota failures, not ordinary app bugs."""
    seen = set()
    current = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        module = type(current).__module__.lower()
        name = type(current).__name__.lower()
        text = str(current).lower()
        if (
            (module.startswith("psycopg") and name in {"operationalerror", "databaseerror"})
            or "data transfer quota" in text
            or "connection failed" in text
            or "network is unreachable" in text
        ):
            return True
        current = getattr(current, "__cause__", None) or getattr(current, "__context__", None)
    return False


def _fallback_html(path):
    if path.startswith("/corporate"):
        title = "企業倒産・新規法人情報サイト"
        description = "全国の企業倒産・新規法人情報を地域別に整理する情報サイトです。現在、最新データの更新処理を行っています。"
        canonical = "https://buzz-now-1.onrender.com/corporate/"
        heading = "企業倒産・新規法人情報"
        lead = "最新データを更新しています。ページ自体は正常に公開中です。しばらくしてから再読み込みしてください。"
    else:
        title = "Buzz Now｜いま話題のトピック"
        description = "いま話題になっているキーワードと、その理由をわかりやすく整理するBuzz Now。"
        canonical = "https://buzz-now-1.onrender.com/"
        heading = "Buzz Now"
        lead = "最新トピックを更新しています。ページ自体は正常に公開中です。しばらくしてから再読み込みしてください。"
    return f"""<!doctype html><html lang=\"ja\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>{title}</title><meta name=\"description\" content=\"{description}\"><meta name=\"robots\" content=\"index,follow,max-image-preview:large\"><link rel=\"canonical\" href=\"{canonical}\"><style>body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:0;background:#f6f7fb;color:#171923}}main{{max-width:860px;margin:0 auto;padding:72px 24px}}.card{{background:#fff;border-radius:24px;padding:40px;box-shadow:0 12px 40px rgba(20,30,60,.08)}}h1{{font-size:clamp(38px,7vw,72px);margin:0 0 18px}}p{{font-size:18px;line-height:1.8;color:#525866}}a{{color:#2457ff}}</style></head><body><main><div class=\"card\"><h1>{heading}</h1><p>{lead}</p><p><a href=\"{canonical}\">再読み込み</a></p></div></main></body></html>""".encode("utf-8")


def _runtime_database_url():
    backend = os.getenv("DATABASE_BACKEND", "source").strip()
    key = "NEON_DATABASE_URL" if backend == "neon" else "DATABASE_URL"
    return os.getenv(key, "").strip()


def _cached_social_jpeg(trend_id, head_only=False):
    """Return (body, byte_length) for an existing JPEG derivative, or None on miss.

    HEAD deliberately selects only byte_length, so media probes do not transfer a
    Base64 image blob from Neon. GET selects only the smaller JPEG derivative and
    never reads the source PNG or performs a database write.
    """
    url = _runtime_database_url()
    if not url:
        return None

    import psycopg
    with psycopg.connect(url) as con:
        if head_only:
            row = con.execute(
                "SELECT byte_length FROM social_image_derivatives WHERE trend_id=%s",
                (int(trend_id),),
            ).fetchone()
            if not row:
                return None
            return b"", int(row[0] or 0)

        row = con.execute(
            "SELECT jpeg_b64,byte_length FROM social_image_derivatives WHERE trend_id=%s",
            (int(trend_id),),
        ).fetchone()
        if not row:
            return None
        try:
            body = base64.b64decode(row[0], validate=True)
        except Exception:
            return None
        if not body:
            return None
        length = int(row[1] or len(body))
        if length != len(body):
            length = len(body)
        return body, length


async def _send_social_jpeg(send, trend_id, body, length):
    headers = [
        (b"content-type", b"image/jpeg"),
        (b"cache-control", b"public, max-age=31536000, immutable"),
        (b"content-disposition", f'inline; filename="buzz-now-{int(trend_id)}.jpg"'.encode("ascii")),
        (b"content-length", str(int(length)).encode("ascii")),
        (b"x-content-type-options", b"nosniff"),
        (b"x-buzz-now-image-cache", b"jpeg-derivative"),
    ]
    await send({'type': 'http.response.start', 'status': 200, 'headers': headers})
    return await send({'type': 'http.response.body', 'body': body})


class MigrationMaintenance:
    def __init__(self, app, paused=False):
        self.app = app
        self.paused = paused

    async def __call__(self, scope, receive, send):
        if scope['type'] not in {'http', 'websocket'}:
            return await self.app(scope, receive, send)

        if self.paused:
            if scope['type'] == 'websocket':
                return await send({'type': 'websocket.close', 'code': 1013})
            if scope.get('path') == '/health':
                return await self.app(scope, receive, send)
            body = json.dumps({'detail': 'データ移設のため一時メンテナンス中です。'}, ensure_ascii=False).encode()
            await send({'type': 'http.response.start', 'status': 503, 'headers': [
                (b'content-type', b'application/json; charset=utf-8'),
                (b'retry-after', b'600'), (b'cache-control', b'no-store')]})
            return await send({'type': 'http.response.body', 'body': body})

        try:
            if scope['type'] == 'http':
                path = scope.get('path') or '/'
                method = (scope.get('method') or 'GET').upper()
                match = re.fullmatch(r"/social-image/(\d+)\.jpg", path)
                if match and method in {'GET', 'HEAD'}:
                    cached = _cached_social_jpeg(int(match.group(1)), head_only=(method == 'HEAD'))
                    if cached is not None:
                        body, length = cached
                        return await _send_social_jpeg(send, int(match.group(1)), body, length)

            return await self.app(scope, receive, send)
        except Exception as exc:
            if scope['type'] != 'http' or not _is_database_unavailable(exc):
                raise

            path = scope.get('path') or '/'
            if path in {'/', '/corporate', '/corporate/'}:
                body = _fallback_html(path)
                await send({'type': 'http.response.start', 'status': 200, 'headers': [
                    (b'content-type', b'text/html; charset=utf-8'),
                    (b'cache-control', b'public, max-age=60'),
                    (b'x-buzz-now-degraded', b'database-unavailable')]})
                return await send({'type': 'http.response.body', 'body': body})

            body = json.dumps({'detail': '最新データを更新中です。しばらくしてから再試行してください。'}, ensure_ascii=False).encode()
            await send({'type': 'http.response.start', 'status': 503, 'headers': [
                (b'content-type', b'application/json; charset=utf-8'),
                (b'retry-after', b'300'), (b'cache-control', b'no-store')]})
            return await send({'type': 'http.response.body', 'body': body})
