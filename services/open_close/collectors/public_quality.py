"""Conservative publication checks; original records are retained for review."""
import re
import unicodedata


def valid_store_name(value):
    name = (value or "").strip()
    if not name or len(name) > 100:
        return False
    if any(name.count(a) != name.count(b) for a, b in [("「", "」"), ("『", "』")]):
        return False
    return not re.search(r"に新店舗|に新店|がオープン|が開店|が閉店|をオープン|を出店|オープン予定|閉店予定|という|について", name)


def valid_facility(value):
    value = (value or "").strip()
    if not value or re.search(r"^(?:に|の|へ|で|を)|新店舗?$|初出店|オープン|閉店|募集|ニュース", value):
        return None
    return value if valid_store_name(value) else None


def valid_floor(value):
    value = re.sub(r"\s+", "", unicodedata.normalize("NFKC", value or ""))
    return value if re.fullmatch(r"(?:地下|B)?[1-9][0-9]?(?:階|F)", value, re.I) else None


def public_store(record):
    result = dict(record)
    result["facility_name"] = valid_facility(record.get("facility_name"))
    result["floor"] = valid_floor(record.get("floor"))
    result["quality_pending"] = not valid_store_name(record.get("name"))
    if result["quality_pending"]:
        result["name"] = "店舗名未確認"
        for field in ("category", "facility_name", "floor", "open_date", "close_date", "confidence"):
            result[field] = None
    return result

