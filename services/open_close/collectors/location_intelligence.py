from __future__ import annotations


def _clamp(value, low=0, high=100):
    return max(low, min(high, int(round(value))))


def _rank(score):
    if score >= 90:
        return "S"
    if score >= 80:
        return "A"
    if score >= 65:
        return "B"
    if score >= 50:
        return "C"
    return "D"


def compute_category_signal(
    total,
    open_24m,
    close_24m,
    median_lifetime_months=None,
    lifetime_sample_count=0,
):
    """Build a transparent category-level location signal.

    This is not a probability of success. It summarises observed opening/closing
    momentum, observation depth and store persistence in the Open Close Map DB.
    """
    total = max(0, int(total or 0))
    opened = max(0, int(open_24m or 0))
    closed = max(0, int(close_24m or 0))
    events = opened + closed
    samples = max(0, int(lifetime_sample_count or 0))

    if events:
        momentum = _clamp(100 * opened / events)
    else:
        momentum = 50

    evidence = _clamp(events / 8 * 100)

    if median_lifetime_months is not None and samples >= 3:
        months = max(0.0, float(median_lifetime_months))
        persistence = _clamp(months / 60 * 100)
        persistence_available = True
    else:
        months = None
        persistence = 50
        persistence_available = False

    score = _clamp(momentum * 0.45 + persistence * 0.30 + evidence * 0.25)

    if events < 3:
        rank = "-"
        label = "データ蓄積中"
    else:
        rank = _rank(score)
        if opened >= closed * 2 and opened >= 3:
            label = "開店優勢"
        elif closed >= opened * 2 and closed >= 3:
            label = "閉店優勢"
        else:
            label = "拮抗"

    return {
        "score": score,
        "rank": rank,
        "label": label,
        "total": total,
        "open_24m": opened,
        "close_24m": closed,
        "events_24m": events,
        "momentum_score": momentum,
        "evidence_score": evidence,
        "persistence_score": persistence,
        "persistence_available": persistence_available,
        "median_lifetime_months": None if months is None else round(months, 1),
        "lifetime_sample_count": samples,
        "version": "category-signal-v1",
        "disclaimer": "出店参考スコアは売上・利益・成功率を示すものではありません。",
    }
