"""Private correction inbox; no personal information is exposed publicly."""
import json
import re
import uuid
from fastapi import Depends, HTTPException, Request
from fastapi.responses import JSONResponse


def install_inquiries(app, db_conn, require_admin):
    @app.on_event("startup")
    def init_inquiries():
        conn = db_conn()
        if conn is None:
            return
        with conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS site_inquiries (
                id TEXT PRIMARY KEY, page_url TEXT NOT NULL,
                email TEXT NOT NULL, message TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())""")

    @app.post("/api/inquiries")
    async def submit_inquiry(request: Request):
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > 16384:
                raise HTTPException(413, "入力が長すぎます。")
        try:
            data = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            raise HTTPException(400, "入力を確認してください。")
        if not isinstance(data, dict) or data.get("consent") is not True:
            raise HTTPException(400, "プライバシーポリシーへの同意が必要です。")
        if data.get("website"):
            raise HTTPException(400, "送信できませんでした。")
        values = [data.get(key, "") for key in ("page_url", "email", "message")]
        if not all(isinstance(value, str) for value in values):
            raise HTTPException(400, "入力を確認してください。")
        page, email, message = [value.strip() for value in values]
        if len(page) > 1000 or (page and not page.startswith(("https://", "http://"))):
            raise HTTPException(400, "対象ページのURLを確認してください。")
        if len(email) > 254 or (email and not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email)):
            raise HTTPException(400, "メールアドレスを確認してください。")
        if not 10 <= len(message) <= 3000:
            raise HTTPException(400, "内容は10〜3000文字で入力してください。")
        conn = db_conn()
        if conn is None:
            raise HTTPException(503, "現在受付を停止しています。時間をおいてお試しください。")
        receipt = str(uuid.uuid4())
        with conn:
            # Serialize the global quota across app instances. No IP addresses stored.
            conn.execute("SELECT pg_advisory_xact_lock(78431027)")
            conn.execute("DELETE FROM site_inquiries WHERE created_at < NOW() - INTERVAL '90 days'")
            count = conn.execute("SELECT COUNT(*) FROM site_inquiries WHERE created_at > NOW() - INTERVAL '1 hour'").fetchone()[0]
            if count >= 30:
                raise HTTPException(429, "受付が混み合っています。時間をおいてお試しください。")
            conn.execute("INSERT INTO site_inquiries (id,page_url,email,message) VALUES (%s,%s,%s,%s)", (receipt, page, email, message))
        return JSONResponse({"ok": True, "receipt": receipt}, headers={"Cache-Control": "no-store"})

    @app.get("/api/admin/inquiries", dependencies=[Depends(require_admin)])
    def read_inquiries():
        conn = db_conn()
        if conn is None:
            raise HTTPException(503, "Database unavailable")
        with conn:
            conn.execute("DELETE FROM site_inquiries WHERE created_at < NOW() - INTERVAL '90 days'")
            rows = conn.execute("SELECT id,page_url,email,message,created_at FROM site_inquiries ORDER BY created_at DESC LIMIT 100").fetchall()
        items = [dict(zip(("id", "page_url", "email", "message", "created_at"), (*row[:4], row[4].isoformat()))) for row in rows]
        return JSONResponse({"items": items}, headers={"Cache-Control": "no-store"})

