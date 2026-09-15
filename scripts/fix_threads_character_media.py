"""Keep the approved Threads investigator photo while X remains text-only.

SOCIAL_TEXT_ONLY is an X/news presentation policy. It must not erase the
explicitly-approved image from the independent Threads character slot.
"""
from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT / 'app/main.py'


def apply():
    text = MAIN.read_text()
    tree = ast.parse(text)
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'auto_post_threads')
    lines = text.splitlines(keepends=True)
    block = ''.join(lines[fn.lineno-1:fn.end_lineno])
    old = "        if SOCIAL_TEXT_ONLY:\n            image_url = ''\n"
    new = "        if SOCIAL_TEXT_ONLY and kind != 'character':\n            image_url = ''\n"
    if old in block:
        block = block.replace(old, new, 1)
        text = ''.join(lines[:fn.lineno-1]) + block.rstrip() + '\n' + ''.join(lines[fn.end_lineno:])
    elif new not in block:
        raise ValueError('Threads text-only media gate anchor not found')
    ast.parse(text)
    MAIN.write_text(text)


if __name__ == '__main__':
    apply()
    print('Threads character image preserved; ordinary social posts remain text-only')
