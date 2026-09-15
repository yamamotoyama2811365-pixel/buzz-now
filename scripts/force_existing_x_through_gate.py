from pathlib import Path

# New host: catch already-published X URLs that still point directly to buzz-now-1.
p = Path('app/main.py')
text = p.read_text()
needle = """async def canonical_origin(request: Request, call_next):\n    path = request.url.path\n    # Keep Render health checks and Google ownership verification local.\n"""
replacement = """async def canonical_origin(request: Request, call_next):\n    path = request.url.path\n    query_text = request.scope.get(\"query_string\", b\"\").decode(\"ascii\", \"ignore\")\n    host = (request.url.hostname or \"\").lower()\n    # Existing X posts may still point at the new host. Send X-origin article\n    # entries through the approved legacy i-mobile gate exactly once.\n    x_entry = (\n        host == \"buzz-now-1.onrender.com\"\n        and \"utm_source=x\" in query_text\n        and \"x_gate_passed=1\" not in query_text\n        and (path.startswith(\"/trend/\") or bool(re.fullmatch(r\"/t/\\d+\", path)))\n    )\n    if x_entry:\n        raw_path = request.scope.get(\"raw_path\", b\"/\").decode(\"ascii\")\n        target = \"https://buzz-now.onrender.com\" + raw_path + ((\"?\" + query_text) if query_text else \"\")\n        return RedirectResponse(target, status_code=302)\n    # Keep Render health checks and Google ownership verification local.\n"""
if needle not in text:
    raise SystemExit('canonical_origin insertion point not found')
text = text.replace(needle, replacement, 1)
p.write_text(text)

# Legacy gate: mark the return URL as already passed so the new host does not bounce back.
p = Path('app/migration_control.py')
text = p.read_text()
old = """        'https://buzz-now-1.onrender.com' + raw_path_text\n        + '?utm_source=x&utm_medium=social&utm_campaign=prebuzz&utm_content=legacy_imobile_gate'\n"""
new = """        'https://buzz-now-1.onrender.com' + raw_path_text\n        + '?utm_source=x&utm_medium=social&utm_campaign=prebuzz&utm_content=legacy_imobile_gate&x_gate_passed=1'\n"""
if old not in text:
    raise SystemExit('legacy destination block not found')
text = text.replace(old, new, 1)
p.write_text(text)
print('existing X URLs now route through legacy gate once')
