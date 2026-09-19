import os
import sys
import time
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import requests

from services.corporate.collect import collect_news, collect_registrations

BASE = os.getenv("CORPORATE_API_URL", "https://buzz-now-1.onrender.com/corporate").rstrip("/")


def oidc_headers():
    token_url = os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"]
    token_req = os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]
    url = token_url + "&audience=" + urllib.parse.quote(BASE, safe="")
    r = requests.get(url, headers={"Authorization": "Bearer " + token_req}, timeout=20)
    r.raise_for_status()
    return {"Authorization": "Bearer " + r.json()["value"]}


def request(method, path, **kwargs):
    last = None
    for attempt in range(8):
        try:
            r = requests.request(method, BASE + path, headers=oidc_headers(), timeout=90, **kwargs)
            if r.status_code in (401, 403) and attempt < 7:
                time.sleep(20)
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            last = exc
            if attempt == 7:
                raise
            time.sleep(min(20, 3 + attempt * 2))
    raise last


def send(payload):
    return request("POST", "/api/ingest", json=payload).json()


def publish_rows(name, rows):
    changed = 0
    for offset in range(0, len(rows), 100):
        changed += int(send({"items": rows[offset:offset + 100]}).get("changed", 0))
    updates = getattr(rows, "updates", [])
    for offset in range(0, len(updates), 100):
        send({"items": [], "registry_updates": updates[offset:offset + 100]})
    detail = ",".join(getattr(rows, "file_ids", []))
    if name == "JC-NET":
        detail = (
            f"candidates={getattr(rows, 'candidates', 0)};"
            f"deferred={getattr(rows, 'deferred', 0)};"
            f"errors={len(getattr(rows, 'errors', []))};"
            f"rejected={len(getattr(rows, 'rejected', []))};"
            f"outside={getattr(rows, 'outside_window', 0)}"
        )
    send({"items": [], "sources": [{"source": name, "status": "ok", "received": len(rows), "detail": detail}]})
    print(name, "received", len(rows), "changed", changed, "skipped", getattr(rows, "skipped", 0), flush=True)


def enrich_known_candidates():
    try:
        from services.corporate.enrich import enrich, known_candidates
        response = request("GET", "/api/enrichment-candidates", params={"search_enabled": "false"})
        candidates = response.json().get("items", [])
        changed = 0
        for candidate in candidates[:6]:
            profile = enrich(candidate["payload"]) if known_candidates(candidate["payload"]) else None
            result = request(
                "POST",
                "/api/enrichment",
                json={"items": [{
                    "id": candidate["id"],
                    "entity": candidate["payload"],
                    "profile": profile,
                    "search_deferred": False,
                }]},
            ).json()
            changed += int(result.get("changed", 0))
        send({"items": [], "sources": [{
            "source": "公式サイト補完",
            "status": "ok",
            "received": changed,
            "detail": f"backup_checked={len(candidates[:6])};search=disabled",
        }]})
        print("Known-site enrichment changed", changed, flush=True)
    except Exception as exc:
        print("Known-site enrichment optional failure:", type(exc).__name__, flush=True)


def main():
    for attempt in range(24):
        try:
            r = requests.get(BASE + "/ready", timeout=20)
            r.raise_for_status()
            if r.json().get("ready"):
                break
        except requests.RequestException:
            pass
        if attempt == 23:
            raise RuntimeError("Corporate API did not become ready")
        time.sleep(10)

    state_response = request("GET", "/api/collection-state")
    state = state_response.json() if state_response.status_code == 200 else {}
    failed = False

    jobs = [
        ("JC-NET", lambda: collect_news(state.get("news", {}))),
        ("国税庁", lambda: collect_registrations(state.get("nta_files", []))),
    ]
    for name, collector in jobs:
        try:
            rows = collector()
            if not rows and not getattr(rows, "skipped", 0):
                raise RuntimeError("source returned no eligible records")
            publish_rows(name, rows)
        except Exception as exc:
            failed = True
            print(name, "FAILED", type(exc).__name__, flush=True)
            try:
                send({"items": [], "sources": [{"source": name, "status": "error", "detail": type(exc).__name__}]})
            except Exception:
                pass

    enrich_known_candidates()
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
