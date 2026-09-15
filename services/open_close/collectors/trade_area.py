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


def compute_trade_area_score(
    activity_score,
    same_category_open_24m,
    same_category_close_24m,
    median_lifetime_months=None,
    lifetime_sample_count=0,
    foot_traffic_score=None,
):
    """Transparent beta score for store/location intelligence.

    This is deliberately not a success probability. Until a licensed/current
    human-flow source is connected, the score uses only this site's observed
    opening/closing and store-lifetime data. Once foot_traffic_score is
    supplied, the weighting automatically gives human-flow a meaningful share.
    """
    activity = _clamp(activity_score)
    opened = max(0, int(same_category_open_24m or 0))
    closed = max(0, int(same_category_close_24m or 0))
    category_total = opened + closed

    if category_total >= 3:
        category_balance = _clamp(100 * opened / category_total)
        category_note = "同業種の直近24か月の開店・閉店バランス"
    else:
        category_balance = 50
        category_note = "同業種データが少ないため中立値"

    samples = max(0, int(lifetime_sample_count or 0))
    if median_lifetime_months is not None and samples >= 3:
        months = max(0.0, float(median_lifetime_months))
        # 60 months (5 years) or more receives full marks; this is a relative
        # persistence signal, not a claim about profitability.
        longevity = _clamp(months / 60 * 100)
        longevity_note = f"同業種の閉店店舗{samples}件の営業期間中央値を反映"
    else:
        months = None
        longevity = 50
        longevity_note = "営業期間サンプルが少ないため中立値"

    components = [
        ("周辺店舗の動き", activity, 45 if foot_traffic_score is None else 30),
        ("同業種の開店優勢度", category_balance, 25 if foot_traffic_score is None else 20),
        ("同業種の営業継続傾向", longevity, 30 if foot_traffic_score is None else 20),
    ]

    flow_connected = foot_traffic_score is not None
    if flow_connected:
        flow = _clamp(foot_traffic_score)
        components.append(("人流", flow, 30))
    else:
        flow = None

    weighted = sum(score * weight for _, score, weight in components)
    total_weight = sum(weight for _, _, weight in components) or 1
    score = _clamp(weighted / total_weight)

    return {
        "score": score,
        "rank": _rank(score),
        "foot_traffic_connected": flow_connected,
        "foot_traffic_score": flow,
        "same_category_open_24m": opened,
        "same_category_close_24m": closed,
        "category_balance_score": category_balance,
        "median_lifetime_months": None if months is None else round(months, 1),
        "lifetime_sample_count": samples,
        "longevity_score": longevity,
        "category_note": category_note,
        "longevity_note": longevity_note,
        "components": [
            {"name": name, "score": component_score, "weight": weight}
            for name, component_score, weight in components
        ],
        "version": "trade-area-beta-v1",
        "basis": "site_open_close_and_lifetime_data" if not flow_connected else "site_data_plus_human_flow",
        "disclaimer": "商圏ポテンシャルは参考指標で、売上・利益・出店成功率を示すものではありません。",
    }
