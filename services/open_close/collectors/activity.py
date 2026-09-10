def compute_activity_score(total, recent_open, recent_close, planned_open=0, planned_close=0):
    total=int(total or 0)
    recent_open=int(recent_open or 0)
    recent_close=int(recent_close or 0)
    planned_open=int(planned_open or 0)
    planned_close=int(planned_close or 0)

    # Data volume: enough observations to make the signal meaningful.
    volume=min(30, round(total / 30 * 30)) if total else 0

    # Opening momentum: recent + planned openings.
    open_signal=recent_open + planned_open
    momentum=min(40, round(open_signal / 12 * 40)) if open_signal else 0

    # Turnover: both openings and closings indicate commercial movement.
    turnover_signal=recent_open + recent_close + planned_open + planned_close
    turnover=min(30, round(turnover_signal / 18 * 30)) if turnover_signal else 0

    score=max(0,min(100,volume+momentum+turnover))

    # Avoid overclaiming when the sample is very thin.
    if total < 8:
        label="データ蓄積中"
        comment="掲載データがまだ少ないため、参考値としてご覧ください。"
    elif score >= 80:
        label="非常に活発"
        comment="開店・閉店の動きが多く、店舗の入れ替わりが活発なエリアです。"
    elif score >= 65:
        label="活発"
        comment="新規出店や店舗の入れ替わりが比較的多いエリアです。"
    elif score >= 45:
        label="標準"
        comment="一定の開店・閉店動向が確認できるエリアです。"
    else:
        label="落ち着いている"
        comment="現在確認できる開店・閉店の動きは比較的少なめです。"

    return {
        "score":score,
        "label":label,
        "comment":comment,
        "counts":{
            "listed_stores":total,
            "recent_open":recent_open,
            "recent_close":recent_close,
            "planned_open":planned_open,
            "planned_close":planned_close,
        },
        "breakdown":{
            "data_volume":volume,
            "opening_momentum":momentum,
            "turnover_activity":turnover,
        },
        "version":"activity-v2",
        "basis":"site_open_close_data"
    }

