"""Narrow offline migration for the user's X-only posting policy change."""
import ast
from pathlib import Path
from textwrap import dedent, indent

ROOT = Path(__file__).resolve().parents[1]
MARKER = '# X-only news policy applied: 2026-09-12.'


def once(text, old, new):
    if text.count(old) != 1:
        raise ValueError('Missing or ambiguous anchor: ' + old[:120])
    return text.replace(old, new, 1)


def replace_method(text, cls_name, method, new):
    cls = next(n for n in ast.parse(text).body if isinstance(n, ast.ClassDef) and n.name == cls_name)
    node = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == method)
    lines = text.splitlines(keepends=True)
    lines[node.lineno-1:node.end_lineno] = [indent(dedent(new).strip() + '\n', '    ')]
    return ''.join(lines)


def install():
    updates = {}
    path = ROOT / 'app/social_schedule.py'
    text = path.read_text()
    if MARKER not in text:
        text = once(text, "(12,30,'character')", "(12,30,'trend')")
        text = once(text, "(21,0,'character')", "(21,0,'trend')")
        text = once(text, 'WINDOW_MINUTES = 30', 'WINDOW_MINUTES = 30\nX_CHARACTER_POSTS_ENABLED = False\n' + MARKER)
        text = once(text, "    slot = current_slot(now, 'mixed' if mixed else kind)",
            "    if kind == 'character' and not X_CHARACTER_POSTS_ENABLED:\n        return None, 'x_character_posts_disabled'\n    slot = current_slot(now, 'mixed' if mixed else kind)")
        text = once(text, "'trend_slots':8,'character_slots':2", "'trend_slots':10,'character_slots':0")
        updates[path] = text

    path = ROOT / 'app/main.py'
    text = path.read_text()
    if MARKER not in text:
        node = [n for n in ast.parse(text).body if isinstance(n, ast.FunctionDef) and n.name == 'auto_quote_yahoo_buzzing_now'][-1]
        lines = text.splitlines(keepends=True)
        code = ''.join(lines[node.lineno-1:node.end_lineno])
        code = once(code, '        _ensure_quote_post_log(c)\n',
            '        _ensure_quote_post_log(c)\n        ' + MARKER + '\n        social_schedule.init(c)\n        if social_mix.ordinary_news_turn(c):\n            return {"posted_count": 0, "reason": "reserved_for_ordinary_news"}\n')
        lines[node.lineno-1:node.end_lineno] = [code]
        updates[path] = ''.join(lines)

    path = ROOT / 'tests/test_social_mix.py'
    text = path.read_text()
    if MARKER not in text:
        text = replace_method(text, 'MixTest', 'test_each_platform_has_two_per_ten', '''
            def test_x_news_only_and_threads_two_per_ten(self):
                for platform in ('x', 'threads'):
                    sequence = []
                    for n in range(1, 31):
                        kind = mix.next_kind(self.c, platform, self.now)
                        sequence.append(kind)
                        self.accepted(platform, kind, n)
                    expected = [] if platform == 'x' else [5, 10, 15, 20, 25, 30]
                    self.assertEqual([i+1 for i, k in enumerate(sequence) if k == 'character'], expected)
                    for start in range(21):
                        self.assertEqual(sequence[start:start+10].count('character'), 0 if platform == 'x' else 2)
        ''')
        # X's historical counter remains; it no longer selects character content.
        text = text.replace("self.assertEqual(mix.next_kind(self.c,'x',self.now),'character')", "self.assertEqual(mix.next_kind(self.c,'x',self.now),'trend')")
        text = replace_method(text, 'MixTest', 'test_mixed_slot_uses_count_not_old_time_role', '''
            def test_fifth_x_turn_accepts_news(self):
                for n in range(1, 5): self.accepted('x', 'trend', n)
                self.assertTrue(mix.ordinary_news_turn(self.c))
                slot, reason = plan.reserve(self.c, 'trend', self.now, mixed=True)
                self.assertEqual(reason, 'ok')
                self.assertEqual(slot['kind'], 'trend')
                self.assertEqual(plan.reserve(self.c, 'trend', self.now, mixed=True)[1], 'slot_already_attempted')
        ''')
        text = replace_method(text, 'MixTest', 'test_news_cannot_take_character_turn', '''
            def test_character_cannot_take_reassigned_news_turn(self):
                for n in range(1, 5): self.accepted('x', 'trend', n)
                self.assertEqual(plan.reserve(self.c, 'character', self.now, mixed=True)[1], 'x_character_posts_disabled')
                self.assertEqual(plan.reserve(self.c, 'trend', self.now, mixed=True)[1], 'ok')
        ''')
        text = replace_method(text, 'MixTest', 'test_trial_is_separate_and_not_extended', '''
            def test_threads_trial_is_not_extended_by_x_change(self):
                mix.start_trial(self.c, 'x', self.now-timedelta(days=14))
                self.assertEqual(mix.next_kind(self.c, 'x', self.now), 'trend')
                self.assertEqual(mix.character_content(self.c, 'x', self.now)['reason'], 'x_character_posts_disabled')
                self.assertTrue(mix.character_content(self.c, 'threads', self.now)['ok'])
                mix.start_trial(self.c, 'threads', self.now-timedelta(days=14))
                self.assertEqual(mix.character_content(self.c, 'threads', self.now)['reason'], 'character_trial_finished')
        ''')
        node = next(n for n in ast.parse(text).body if isinstance(n, ast.ClassDef) and n.name == 'MixTest')
        method = next(n for n in node.body if isinstance(n, ast.FunctionDef) and n.name == 'test_existing_asset_and_simplified_caption')
        lines = text.splitlines(keepends=True)
        code = ''.join(lines[method.lineno-1:method.end_lineno])
        code = once(code, "for platform in ('x','threads'):", "for platform in ('threads',):")
        lines[method.lineno-1:method.end_lineno] = [code]
        text = ''.join(lines)
        updates[path] = MARKER + '\n' + text

    # Update old live checks to read the now explicitly per-platform policy.
    path = ROOT / '.github/workflows/social-mix.yml'
    text = path.read_text()
    if MARKER not in text:
        text = once(text, "if policy.get('version')==2:", "if policy.get('version')==3:")
        text = once(text, "assert policy['character_positions']==[5,10]", "assert policy['platforms']['x']['character_positions']==[]\n                      assert policy['platforms']['threads']['character_positions']==[5,10]")
        updates[path] = MARKER + '\n' + text
    path = ROOT / '.github/workflows/detective-assets-final.yml'
    text = path.read_text()
    if MARKER not in text:
        text = once(text, "assert status['mix_policy']['character_positions']==[5,10]", "assert status['mix_policy']['platforms']['threads']['character_positions']==[5,10]\n                  assert status['mix_policy']['platforms']['x']['character_positions']==[]")
        updates[path] = MARKER + '\n' + text

    for path, text in updates.items():
        if path.suffix == '.py': ast.parse(text)
    for path, text in updates.items():
        path.write_text(text)
    print('Applied X-only change to:', ', '.join(str(p.relative_to(ROOT)) for p in updates) or 'already integrated')


if __name__ == '__main__': install()
