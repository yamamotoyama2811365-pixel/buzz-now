"""Two-week character trial, gated on the approved investigator asset."""
from pathlib import Path
from datetime import datetime, timezone
from . import social_schedule as plan

ROOT = Path(__file__).resolve().parent.parent
APPROVED_ASSET = 'static/detective/approved.jpg'


def media(day, slot):
    return APPROVED_ASSET if (ROOT / APPROVED_ASSET).is_file() else None


def run(db, sender, pause, enabled, configured, init_quote, cap, cooldown, mixed=False):
    if not enabled or not configured:
        return {'sent':0, 'reason':'social_not_configured'}
    blocked = pause()
    if blocked:
        return {'sent':0, **blocked}
    now = datetime.now(timezone.utc)
    slot = plan.current_slot(now, 'mixed' if mixed else 'character')
    if not slot:
        return {'sent':0, 'reason':'outside_posting_window'}
    with db() as c:
        plan.init(c)
        init_quote(c)
        day = plan.trial_day(c, now)
        if day < 0 or day >= 14:
            return {'sent':0, 'reason':'character_trial_finished'}
        asset = media(day, slot)
        if not asset:
            return {'sent':0, 'reason':'approved_character_image_missing'}
        reserved, reason = plan.reserve(c, 'character', now, cap, cooldown, mixed=mixed)
        if not reserved:
            return {'sent':0, 'reason':reason}
        caption_slot = {**slot, 'start': slot['start'].replace(hour=12 if 6 <= slot['start'].hour < 18 else 21)}
        response = sender(plan.character_text(day, caption_slot), 'https://buzz-now-1.onrender.com/'+asset, 'shareNow')
        if response.get('ok'):
            c.execute("INSERT INTO system_state(key,value) VALUES('detective_trial_start',?) ON CONFLICT(key) DO NOTHING", (now.astimezone(plan.JST).date().isoformat(),))
            plan.finish(c, reserved, True, response.get('post_id'))
            return {'sent':1,'slot':slot['key'],'post_id':response.get('post_id')}
        if response.get('status_code') == 429:
            plan.finish(c, reserved, False, reason='buffer_rate_limited')
        return {'sent':0,'reason':response.get('reason','buffer_submission_uncertain')}


def status(db):
    now = datetime.now(timezone.utc)
    with db() as c:
        plan.init(c)
        row = c.execute("SELECT value FROM system_state WHERE key='detective_trial_start'").fetchone()
        day = plan.trial_day(c, now)
    return {**plan.status(), 'trial_start_jst':row['value'] if row else None,
            'approved_scene_images':1 if (ROOT/APPROVED_ASSET).is_file() else 0,
            'character_state':'trial_finished' if day>=14 else ('awaiting_approved_images' if not (ROOT/APPROVED_ASSET).is_file() else 'ready'),
            'profile_update':'not_performed'}
