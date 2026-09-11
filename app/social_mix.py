"""Per-platform 8:2 content rotation based on durable Buffer acceptances.

Acceptance is not proof of network publication. Unknown submissions pause the
channel rather than advancing its sequence or risking a duplicate.
"""
from datetime import datetime, timezone
from pathlib import Path
from . import social_schedule as plan

ROOT = Path(__file__).resolve().parent.parent
CHARACTER_ID = -1
TRIAL_KEYS = {'x': 'detective_trial_start', 'threads': 'detective_threads_trial_start'}


def counts(c, platform):
    if platform == 'x':
        row = c.execute("SELECT COUNT(*) AS n, COALESCE(SUM(CASE WHEN kind='character' THEN 1 ELSE 0 END),0) AS characters FROM social_schedule_log WHERE state='sent'").fetchone()
    elif platform == 'threads':
        row = c.execute('SELECT COUNT(*) AS n, COALESCE(SUM(CASE WHEN trend_id=-1 THEN 1 ELSE 0 END),0) AS characters FROM threads_posts WHERE buffer_status=1').fetchone()
    else:
        raise ValueError('unsupported platform')
    return {'accepted': int(row['n']), 'character_accepted': int(row['characters'])}


def trial_day(c, platform, now):
    key = TRIAL_KEYS[platform]
    row = c.execute('SELECT value FROM system_state WHERE key=?', (key,)).fetchone()
    if not row:
        return 0
    return (now.astimezone(plan.JST).date() - datetime.fromisoformat(row['value']).date()).days


def start_trial(c, platform, now):
    c.execute('INSERT INTO system_state(key,value) VALUES(?,?) ON CONFLICT(key) DO NOTHING',
              (TRIAL_KEYS[platform], now.astimezone(plan.JST).date().isoformat()))


def next_kind(c, platform, now=None):
    now = now or datetime.now(timezone.utc)
    if not 0 <= trial_day(c, platform, now) < 14:
        return 'trend'
    return 'character' if (counts(c, platform)['accepted'] + 1) % 5 == 0 else 'trend'


def pending(c, platform):
    if platform == 'x':
        return bool(c.execute("SELECT slot_key FROM social_schedule_log WHERE state='reserved' LIMIT 1").fetchone())
    if platform == 'threads':
        # -3 is an ambiguous submission made by this version. Historical 0 rows
        # include failures from before the confirmed Threads connection existed.
        return bool(c.execute('SELECT id FROM threads_posts WHERE buffer_status IN (-1,-3) LIMIT 1').fetchone())
    raise ValueError('unsupported platform')


def character_content(c, platform, now):
    day = trial_day(c, platform, now)
    if not 0 <= day < 14:
        return {'ok': False, 'reason': 'character_trial_finished'}
    relative = 'static/detective/approved.jpg'
    if not (ROOT / relative).is_file():
        return {'ok': False, 'reason': 'approved_character_image_missing'}
    local = now.astimezone(plan.JST)
    slot = {'start': local.replace(hour=12 if 6 <= local.hour < 18 else 21)}
    text = plan.character_text(day, slot)
    # Existing approved captions are conservatively within both network limits.
    if len(text) * 2 > 280:
        return {'ok': False, 'reason': 'character_caption_too_long'}
    return {'ok': True, 'text': text, 'image_url': 'https://buzz-now-1.onrender.com/' + relative}


def status(db):
    now = datetime.now(timezone.utc)
    with db() as c:
        plan.init(c)
        platforms = {}
        for platform in ('x', 'threads'):
            row = c.execute('SELECT value FROM system_state WHERE key=?', (TRIAL_KEYS[platform],)).fetchone()
            platforms[platform] = {**counts(c, platform), 'next_kind': next_kind(c, platform, now),
                'blocked_by_uncertain_submission': pending(c, platform),
                'trial_start_jst': row['value'] if row else None,
                'trial_active': 0 <= trial_day(c, platform, now) < 14}
    return {'version': 2, 'normal_per_10': 8, 'character_per_10': 2,
            'character_positions': [5,10], 'counter_basis': 'buffer_accepted_not_publication_confirmed',
            'trial_days': 14, 'x_counter_scope': 'persistent_slot_ledger',
            'missed_slots': 'skip; preserve content sequence; never catch up in a burst',
            'platforms': platforms}
