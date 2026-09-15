"""Magazine-style discovery blocks built only from existing BUZZ NOW data.

No X post text/media is copied. Public X URLs are rendered through X's official
embed widget by the template. Real site PV is preferred for internal ranking;
if first-party PV is unavailable, the fallback is explicitly score-based.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

JST = timezone(timedelta(hours=9))
_X_POST = re.compile(r"^https://(?:www\.)?x\.com/(?:i/web|[A-Za-z0-9_]{1,15})/status/\d+(?:[?#].*)?$")


def _value(row, key, default=""):
    try:
        value = row[key]
    except (KeyError, IndexError, TypeError):
        return default
    return default if value is None else value


def _clean(value, limit=170):
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[: max(1, limit - 1)].rstrip() + "…"


def _first_sentence(value, limit=145):
    text = _clean(value, max(limit * 3, 240))
    if not text:
        return ""
    parts = re.split(r"(?<=[。！？!?])\s*", text, maxsplit=1)
    return _clean(parts[0], limit)


def three_points(trend, briefing=None, editorial=None):
    """Return up to three grounded, concise points without another AI call."""
    keyword = _clean(_value(trend, "keyword"), 70)
    status = _clean(_value(trend, "status"), 40)
    candidates = []
    if editorial:
        candidates.append(_first_sentence(editorial.get("summary", "")))
    candidates.append(_first_sentence(_value(trend, "why_now")))
    for item in list(briefing or [])[:2]:
        title = _clean(item.get("title", ""), 135)
        if title:
            candidates.append("関連報道：" + title)
    if keyword and status:
        candidates.append(f"BUZZ NOWでは「{keyword}」を{status}として継続観測中。")
    candidates.append("公開情報とSNS・検索・閲覧シグナルを照合し、続報があれば同じページを更新します。")

    result = []
    seen = set()
    for candidate in candidates:
        candidate = _clean(candidate, 170)
        if not candidate:
            continue
        key = re.sub(r"\W+", "", candidate).casefold()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(candidate)
        if len(result) == 3:
            break
    return result


def viral_posts(c, keyword: str, limit: int = 4, max_age_days: int = 21):
    """Recent public X URLs previously selected by BUZZ NOW's high-reaction flow."""
    limit = max(0, min(int(limit), 6))
    if not limit:
        return []
    cutoff = (datetime.now(timezone.utc) - timedelta(days=max(1, max_age_days))).isoformat()
    try:
        rows = c.execute(
            """SELECT keyword,tweet_id,tweet_url,sent_at
               FROM buzzing_quote_posts
               WHERE status='sent' AND sent_at>=?
               ORDER BY CASE WHEN keyword=? THEN 0 ELSE 1 END, sent_at DESC
               LIMIT ?""",
            (cutoff, keyword, max(limit * 4, 12)),
        ).fetchall()
    except Exception:
        return []

    result, seen = [], set()
    for row in rows:
        url = str(_value(row, "tweet_url") or "").strip()
        tweet_id = str(_value(row, "tweet_id") or "").strip()
        if not url or not _X_POST.fullmatch(url) or not tweet_id or tweet_id in seen:
            continue
        seen.add(tweet_id)
        result.append({
            "keyword": _clean(_value(row, "keyword"), 80),
            "tweet_id": tweet_id,
            "tweet_url": url,
            "sent_at": str(_value(row, "sent_at")),
            "is_related": str(_value(row, "keyword")) == str(keyword),
        })
        if len(result) >= limit:
            break
    return result


def internal_buzz_rows(c, current_trend_id: int, limit: int = 8, days: int = 7):
    """Prefer real first-party PV; fill remaining slots with current BUZZ scores."""
    limit = max(1, min(int(limit), 12))
    cutoff = (datetime.now(JST).date() - timedelta(days=max(1, days) - 1)).isoformat()
    result, used = [], {int(current_trend_id)}

    try:
        pv_rows = c.execute(
            """SELECT path,SUM(views) AS views
               FROM visitor_pageviews_daily
               WHERE is_test=0 AND day>=? AND path LIKE '/trend/%'
               GROUP BY path ORDER BY views DESC LIMIT 40""",
            (cutoff,),
        ).fetchall()
        for pv in pv_rows:
            slug = str(_value(pv, "path"))[7:]
            if not slug:
                continue
            row = c.execute(
                """SELECT id,keyword,slug,status,pre_buzz_score,buzz_score,acceleration,category
                   FROM trends WHERE slug=? AND is_indexable=1 LIMIT 1""",
                (slug,),
            ).fetchone()
            if not row:
                continue
            trend_id = int(_value(row, "id", 0) or 0)
            if not trend_id or trend_id in used:
                continue
            used.add(trend_id)
            result.append({
                "id": trend_id,
                "keyword": _clean(_value(row, "keyword"), 90),
                "slug": str(_value(row, "slug")),
                "status": _clean(_value(row, "status"), 40),
                "views": int(_value(pv, "views", 0) or 0),
                "basis": "pv7d",
            })
            if len(result) >= limit:
                return result
    except Exception:
        pass

    try:
        fallback = c.execute(
            """SELECT id,keyword,slug,status,pre_buzz_score,buzz_score,acceleration,category
               FROM trends WHERE id<>? AND is_indexable=1
               ORDER BY buzz_score DESC,pre_buzz_score DESC,acceleration DESC,updated_at DESC
               LIMIT ?""",
            (current_trend_id, max(limit * 3, 12)),
        ).fetchall()
    except Exception:
        return result

    for row in fallback:
        trend_id = int(_value(row, "id", 0) or 0)
        if not trend_id or trend_id in used:
            continue
        used.add(trend_id)
        result.append({
            "id": trend_id,
            "keyword": _clean(_value(row, "keyword"), 90),
            "slug": str(_value(row, "slug")),
            "status": _clean(_value(row, "status"), 40),
            "views": 0,
            "basis": "buzz_score",
        })
        if len(result) >= limit:
            break
    return result
