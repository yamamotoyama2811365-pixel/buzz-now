from pathlib import Path

# 1) Force a fresh gate cookie version so earlier test cookies cannot bypass the ad gate.
p = Path('app/migration_control.py')
text = p.read_text()
text = text.replace('buzznow_x_gate_seen=1', 'buzznow_x_gate_seen_v2=1')
p.write_text(text)

# 2) Split X public links from Threads/general social public links.
p = Path('app/main.py')
text = p.read_text()
anchor = 'SOCIAL_PUBLIC_BASE_URL = os.getenv("SOCIAL_PUBLIC_BASE_URL", "https://buzz-now-1.onrender.com").rstrip("/")\n'
insert = anchor + 'X_PUBLIC_BASE_URL = os.getenv("X_PUBLIC_BASE_URL", "https://buzz-now.onrender.com").rstrip("/")\n'
if 'X_PUBLIC_BASE_URL = os.getenv(' not in text:
    if anchor not in text:
        raise SystemExit('SOCIAL_PUBLIC_BASE_URL anchor not found')
    text = text.replace(anchor, insert, 1)

old_short = 'def _social_short_url(trend_id: int) -> str:\n    return f"{SOCIAL_PUBLIC_BASE_URL}/t/{int(trend_id)}"\n'
new_short = 'def _social_short_url(trend_id: int) -> str:\n    return f"{X_PUBLIC_BASE_URL}/t/{int(trend_id)}"\n'
if old_short in text:
    text = text.replace(old_short, new_short, 1)

old_build = 'url = (SOCIAL_PUBLIC_BASE_URL + _social_detail_path(row["slug"])\n           + "?utm_source=x&utm_medium=social&utm_campaign=prebuzz&utm_content="'
new_build = 'url = (X_PUBLIC_BASE_URL + _social_detail_path(row["slug"])\n           + "?utm_source=x&utm_medium=social&utm_campaign=prebuzz&utm_content="'
if old_build in text:
    text = text.replace(old_build, new_build, 1)

p.write_text(text)
print('fixed X-only i-mobile gate v2')
