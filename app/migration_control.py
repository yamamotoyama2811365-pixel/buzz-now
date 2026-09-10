"""Reversible migration controls; no credentials or database writes here."""
import json


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


class MigrationMaintenance:
    def __init__(self, app, paused=False):
        self.app = app
        self.paused = paused

    async def __call__(self, scope, receive, send):
        if not self.paused or scope['type'] not in {'http', 'websocket'}:
            return await self.app(scope, receive, send)
        if scope['type'] == 'websocket':
            return await send({'type': 'websocket.close', 'code': 1013})
        if scope.get('path') == '/health':
            return await self.app(scope, receive, send)
        body = json.dumps({'detail': 'データ移設のため一時メンテナンス中です。'}, ensure_ascii=False).encode()
        await send({'type': 'http.response.start', 'status': 503, 'headers': [
            (b'content-type', b'application/json; charset=utf-8'),
            (b'retry-after', b'600'), (b'cache-control', b'no-store')]})
        await send({'type': 'http.response.body', 'body': body})
