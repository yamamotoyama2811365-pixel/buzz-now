"""Bounded, idempotent integration; no service calls or production writes."""
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MARKER = '# Social content rotation v2: separate accepted-post counters for X and Threads.'


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise ValueError('Integration anchor missing or ambiguous: ' + old[:90])
    return text.replace(old, new, 1)


def edit_function(source, name, transform):
    matches = [n for n in ast.parse(source).body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name]
    if not matches:
        raise ValueError('Missing function: ' + name)
    node = matches[-1]
    lines = source.splitlines(keepends=True)
    old = ''.join(lines[node.lineno-1:node.end_lineno])
    new = transform(old)
    ast.parse(new)
    return ''.join(lines[:node.lineno-1]) + new.rstrip() + '\n' + ''.join(lines[node.end_lineno:])


def integrate():
    target = ROOT / 'app/main.py'
    src = target.read_text()
    if MARKER in src:
        return
    schedule_path = ROOT / 'app/social_schedule.py'
    schedule = schedule_path.read_text()
    schedule = replace_once(schedule, 'if role == kind and start <= local < start + timedelta(minutes=WINDOW_MINUTES):',
                            "if (kind == 'mixed' or role == kind) and start <= local < start + timedelta(minutes=WINDOW_MINUTES):")
    schedule = replace_once(schedule, 'def reserve(c, kind, now, daily_cap=10, cooldown_minutes=60):\n    slot = current_slot(now, kind)',
                            "def reserve(c, kind, now, daily_cap=10, cooldown_minutes=60, mixed=False):\n    slot = current_slot(now, 'mixed' if mixed else kind)\n    if slot and mixed:\n        slot['kind'] = kind")
    schedule = replace_once(schedule, "        return None, 'slot_already_attempted'\n    cutoff =",
                            "        return None, 'slot_already_attempted'\n    if mixed:\n        from . import social_mix\n        if social_mix.pending(c, 'x'):\n            return None, 'uncertain_submission_requires_review'\n        if kind != social_mix.next_kind(c, 'x', now):\n            return None, 'content_kind_not_due'\n    cutoff =")

    detective_path = ROOT / 'app/detective_posts.py'
    detective = detective_path.read_text()
    detective = replace_once(detective, "period = 'noon' if slot['start'].hour == 12 else 'night'", "period = 'noon' if 6 <= slot['start'].hour < 18 else 'night'")
    detective = replace_once(detective, 'def run(db, sender, pause, enabled, configured, init_quote, cap, cooldown):',
                             'def run(db, sender, pause, enabled, configured, init_quote, cap, cooldown, mixed=False):')
    detective = replace_once(detective, "slot = plan.current_slot(now, 'character')", "slot = plan.current_slot(now, 'mixed' if mixed else 'character')")
    detective = replace_once(detective, "plan.reserve(c, 'character', now, cap, cooldown)", "plan.reserve(c, 'character', now, cap, cooldown, mixed=mixed)")
    detective = replace_once(detective, "response = sender(plan.character_text(day, slot),", "caption_slot = {**slot, 'start': slot['start'].replace(hour=12 if 6 <= slot['start'].hour < 18 else 21)}\n        response = sender(plan.character_text(day, caption_slot),")
    detective = replace_once(detective, '        # Keep reserved on failure:', "        if response.get('status_code') == 429:\n            plan.finish(c, reserved, False, reason='buffer_rate_limited')\n        # Keep reserved on failure:")

    src = replace_once(src, 'from app import social_schedule, detective_posts', 'from app import social_schedule, detective_posts, social_mix\n' + MARKER)
    for name in ('auto_post_social', 'auto_quote_yahoo_buzzing_now'):
        src = edit_function(src, name, lambda code: replace_once(code, 'SOCIAL_DAILY_CAP, SOCIAL_GLOBAL_COOLDOWN_MINUTES)', 'SOCIAL_DAILY_CAP, SOCIAL_GLOBAL_COOLDOWN_MINUTES, mixed=True)'))
    src = edit_function(src, 'run_scheduled_social', lambda _: '''def run_scheduled_social():
    if LEGACY_SERVICE:
        return
    now_dt = datetime.now(timezone.utc)
    if not social_schedule.current_slot(now_dt, 'mixed'):
        return {'sent': 0, 'reason': 'outside_posting_window'}
    with db() as c:
        social_schedule.init(c)
        kind = social_mix.next_kind(c, 'x', now_dt)
    if kind == 'character':
        return detective_posts.run(db, _send_to_buffer_direct, _buffer_pause,
            SOCIAL_AUTO_ENABLED, bool(BUFFER_API_KEY and BUFFER_CHANNEL_ID),
            _ensure_quote_post_log, SOCIAL_DAILY_CAP, SOCIAL_GLOBAL_COOLDOWN_MINUTES, mixed=True)
    with db() as c:
        result = auto_post_social(c, now_iso())
        c.commit()
        return result
''')
    src = edit_function(src, 'social_schedule_status', lambda code: replace_once(code, '"buffer_pause": _buffer_pause()',
        '"buffer_pause": _buffer_pause(), "mix_policy": social_mix.status(db), "posting_enabled": {"x": SOCIAL_AUTO_ENABLED, "threads": THREADS_AUTO_ENABLED}'))
    src = edit_function(src, '_threads_post_allowed', lambda code: replace_once(code, '    last_same = c.execute(',
        "    if row['id'] == social_mix.CHARACTER_ID:\n        return True, 'ok'\n\n    last_same = c.execute("))

    def threads(code):
        start = code.index('        chosen = None\n')
        end = code.index('        if chosen is None:', start)
        loop = code[start+len('        chosen = None\n'):end]
        new = '''        if social_mix.pending(c, 'threads'):
            return {**result, 'reason': 'uncertain_submission_requires_review'}
        kind = social_mix.next_kind(c, 'threads', now)
        chosen = None
        character = {}
        if kind == 'character':
            allowed, reason = _threads_post_allowed(c, {'id': social_mix.CHARACTER_ID}, now)
            if not allowed:
                return {**result, 'reason': reason}
            character = social_mix.character_content(c, 'threads', now)
            if not character.get('ok'):
                return {**result, 'reason': character.get('reason')}
            text = character['text']
            chosen = {'id': social_mix.CHARACTER_ID, 'keyword': 'SNS捜査官の日常',
                      'pre_buzz_score': 0, 'traffic_potential': 0}
        else:
'''
        new += ''.join('    ' + line if line.strip() else line for line in loop.splitlines(keepends=True))
        code = code[:start] + new + code[end:]
        code = replace_once(code, "        cached = c.execute('SELECT trend_id FROM social_images WHERE trend_id=?', (chosen['id'],)).fetchone()\n        image_url = _social_image_url(chosen['id']) if cached else ''",
            "        if kind == 'character':\n            image_url = character['image_url']\n        else:\n            cached = c.execute('SELECT trend_id FROM social_images WHERE trend_id=?', (chosen['id'],)).fetchone()\n            image_url = _social_image_url(chosen['id']) if cached else ''")
        code = replace_once(code, "(2 if response.get('status_code') == 429 else 0)", "(2 if response.get('status_code') == 429 else -3)")
        code = replace_once(code, '            history.commit()', "            if accepted and kind == 'character':\n                social_mix.start_trial(history, 'threads', now)\n            history.commit()")
        return replace_once(code, "'last_keyword': chosen['keyword'],", "'last_keyword': chosen['keyword'], 'kind': kind,")
    src = edit_function(src, 'auto_post_threads', threads)
    # A scheduled quote consumes one ordinary slot, never several at once.
    nodes = [n for n in ast.parse(src).body if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'YAHOO_QUOTE_MAX_PER_RUN' for t in n.targets)]
    if len(nodes) != 1:
        raise ValueError('Quote per-run limit missing')
    n = nodes[0]
    lines = src.splitlines(keepends=True)
    lines[n.lineno-1:n.end_lineno] = ['YAHOO_QUOTE_MAX_PER_RUN = min(1, ' + ast.get_source_segment(src, n.value) + ')\n']
    src = ''.join(lines)
    for text in (src, schedule, detective):
        ast.parse(text)
    target.write_text(src)
    schedule_path.write_text(schedule)
    detective_path.write_text(detective)


if __name__ == '__main__':
    integrate()
