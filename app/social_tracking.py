"""Per-dispatch attribution without schema changes or analytics fabrication.

Buffer acceptance is not X publication. Missing metrics remain unknown.
Only public identifiers are persisted in the existing system_state table.
"""
import json
from uuid import uuid4


def new_content_id():
    return "news_v1_" + uuid4().hex


def _key(trend_id, attempted_at):
    return f"x_post_tracking:{int(trend_id)}:{attempted_at}"


def record(c, trend_id, attempted_at, content_id, buffer_result, post_text):
    value = {
        "utm_content": content_id,
        "buffer_post_id": buffer_result.get("post_id") or None,
        "buffer_accepted": bool(buffer_result.get("ok")),
        "attempted_at": attempted_at,
        "destination_url": post_text.rsplit("\n", 1)[-1],
        "metrics_status": "not_collected",
    }
    c.execute("""INSERT INTO system_state(key,value) VALUES(?,?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
        (_key(trend_id, attempted_at), json.dumps(value, ensure_ascii=False)))


def lookup(c, trend_id, attempted_at):
    row = c.execute("SELECT value FROM system_state WHERE key=?",
                    (_key(trend_id, attempted_at),)).fetchone()
    if row is None:
        return None  # Historical posts cannot be retroactively attributed.
    try:
        return json.loads(row["value"])
    except (ValueError, TypeError):
        return None
