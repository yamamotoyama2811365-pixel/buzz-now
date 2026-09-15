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
    """Return a useful, indexable page while the database is temporarily unavailable.

    Search engines should not see a six-word placeholder or a generic error page just
    because the data layer is unavailable. These pages intentionally contain only
    durable editorial information that does not depend on the database. They never
    invent current rankings, counts, company facts, or trend data.
    """
    corporate_pages = {
        "/corporate": (
            "企業倒産・新規法人情報サイト",
            "全国の企業倒産・新規法人情報を、出典・地域・業種から確認できる企業情報サイトです。",
            "企業倒産・新規法人情報",
            "倒産に関する公開報道と新たに法人番号が指定された法人情報を整理しています。個別企業のページでは、確認できた所在地、業種、手続きの状況、報道日、出典などを区別して掲載します。",
        ),
        "/corporate/": (
            "企業倒産・新規法人情報サイト",
            "全国の企業倒産・新規法人情報を、出典・地域・業種から確認できる企業情報サイトです。",
            "企業倒産・新規法人情報",
            "倒産に関する公開報道と新たに法人番号が指定された法人情報を整理しています。個別企業のページでは、確認できた所在地、業種、手続きの状況、報道日、出典などを区別して掲載します。",
        ),
        "/corporate/bankruptcies": (
            "倒産速報｜企業倒産・新規法人情報サイト",
            "企業の倒産関連報道を、会社名・地域・手続き・出典とともに整理しています。",
            "企業の倒産速報",
            "破産、民事再生、特別清算などの報道を同じものとして扱わず、出典で確認できる手続きの状況と報道日を分けて整理します。報道後に状況が変わる場合があるため、詳細ページでは元の出典も確認できる構成にしています。",
        ),
        "/corporate/registrations": (
            "新設・新規法人情報｜企業倒産・新規法人情報サイト",
            "新たに法人番号が指定された法人情報を、地域別に確認できます。",
            "新設・新規法人情報",
            "国税庁の公表情報をもとに、新たに法人番号が指定された法人を整理します。法人番号の指定日は会社の設立日と一致しない場合があるため、当サイトでは両者を同一の意味として表示しません。",
        ),
        "/corporate/signals": (
            "地域・業種の動き｜企業倒産・新規法人情報サイト",
            "公開情報から確認できる企業動向を、地域と業種の切り口で整理するページです。",
            "地域・業種の動き",
            "当サイトが収集した公開情報の範囲で、地域・業種ごとの掲載事案を整理します。全国統計や個別企業の信用評価ではなく、検索や比較の入口として利用できるよう、集計条件と出典を明確にして掲載します。",
        ),
    }
    corporate = path in corporate_pages
    if corporate:
        title, description, heading, lead = corporate_pages[path]
        canonical = "https://buzz-now-1.onrender.com" + ("/corporate/" if path == "/corporate" else path)
        nav = """
        <nav class=\"nav\" aria-label=\"企業情報メニュー\">
          <a href=\"/corporate/\">企業情報トップ</a>
          <a href=\"/corporate/bankruptcies\">倒産速報</a>
          <a href=\"/corporate/registrations\">新規法人</a>
          <a href=\"/corporate/signals\">地域・業種の動き</a>
        </nav>
        """
        sections = """
        <section><h2>このサイトで確認できること</h2><p>会社名だけでなく、地域、業種、報道日、手続きの状況、法人番号、確認できた出典を分けて整理します。確認できない項目は推測で埋めず、未確認として扱います。</p></section>
        <section><h2>情報の読み方</h2><p>倒産関連情報は報道時点の内容です。新規法人情報は法人番号の新規指定を示すもので、設立日そのものを保証するものではありません。詳細ページでは一次情報や報道元へ戻れるよう出典を表示します。</p></section>
        <section><h2>現在の更新状況</h2><p>データベースの利用上限により最新一覧の更新を一時停止しています。公開設定、検索エンジン向けのページ構造、既存URLは維持し、データ接続が復旧し次第、通常の一覧表示へ戻ります。</p></section>
        """
    else:
        title = "Buzz Now｜いま話題の理由がわかるトレンド情報"
        description = "いま話題になっているキーワードを集め、なぜ今話題なのかを出典と時点付きで整理するBuzz Now。"
        canonical = "https://buzz-now-1.onrender.com/"
        heading = "Buzz Now"
        lead = "Buzz Nowは、検索やSNSで注目が集まり始めたキーワードについて、名前だけを並べるのではなく『なぜ今話題なのか』を確認できるよう整理するトレンド情報サイトです。"
        nav = """
        <nav class=\"nav\" aria-label=\"サイトメニュー\">
          <a href=\"/\" aria-current=\"page\">Buzz Nowトップ</a>
          <a href=\"/corporate/\">企業倒産・新規法人情報</a>
        </nav>
        """
        sections = """
        <section><h2>Buzz Nowが整理する情報</h2><p>話題のキーワード、注目が高まった背景、関連する出来事、確認できた情報源を分けて整理します。単にトレンド名を転載するのではなく、読者が「何が起きたのか」「なぜ今検索されているのか」を短時間で把握できるページを目指しています。</p></section>
        <section><h2>情報の確認方針</h2><p>公開情報やニュースなど確認できる情報を基にし、確認できない内容を事実として補完しません。話題の理由は時点によって変わるため、更新時刻や出典を確認できる構成で通常ページを公開しています。</p></section>
        <section><h2>現在の更新状況</h2><p>データベースの月間利用上限により、最新ランキングと個別トピックの読み込みを一時停止しています。この案内ページは検索エンジンがサイトの目的を理解できるよう維持し、データ接続が復旧し次第、通常のトレンド一覧に自動で戻ります。</p></section>
        """
    website_json = json.dumps({
        "@context": "https://schema.org",
        "@type": "WebSite",
        "name": "企業倒産・新規法人情報サイト" if corporate else "Buzz Now",
        "url": canonical,
        "description": description,
        "inLanguage": "ja",
    }, ensure_ascii=False).replace("<", "\\u003c")
    return f"""<!doctype html><html lang=\"ja\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>{title}</title><meta name=\"description\" content=\"{description}\"><meta name=\"robots\" content=\"index,follow,max-image-preview:large\"><link rel=\"canonical\" href=\"{canonical}\"><link rel=\"icon\" href=\"/favicon.ico\" type=\"image/vnd.microsoft.icon\"><link rel=\"icon\" href=\"/static/buzz-now-icon.png\" type=\"image/png\" sizes=\"192x192\"><script type=\"application/ld+json\">{website_json}</script><style>body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:0;background:#f6f7fb;color:#171923}}main{{max-width:920px;margin:0 auto;padding:56px 24px 80px}}.card{{background:#fff;border-radius:24px;padding:clamp(26px,5vw,48px);box-shadow:0 12px 40px rgba(20,30,60,.08)}}h1{{font-size:clamp(38px,7vw,68px);line-height:1.05;margin:20px 0}}h2{{font-size:24px;margin:34px 0 10px}}p{{font-size:17px;line-height:1.9;color:#525866}}.status{{display:inline-block;border-radius:999px;background:#eef2ff;color:#3049a5;padding:8px 12px;font-size:13px;font-weight:700}}.nav{{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:28px}}.nav a{{padding:10px 14px;border-radius:999px;background:#f1f3f8;color:#24324a;text-decoration:none;font-weight:700}}a{{color:#2457ff}}.reload{{display:inline-flex;margin-top:22px;font-weight:700}}</style></head><body><main>{nav}<div class=\"card\"><span class=\"status\">公開継続中・最新データ更新待ち</span><h1>{heading}</h1><p>{lead}</p>{sections}<a class=\"reload\" href=\"{canonical}\">最新状態を再読み込み →</a></div></main></body></html>""".encode("utf-8")


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
            fallback_paths = {
                '/', '/corporate', '/corporate/', '/corporate/bankruptcies',
                '/corporate/registrations', '/corporate/signals'
            }
            if path in fallback_paths:
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
