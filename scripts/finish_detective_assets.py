"""Complete the approved image transfer, then narrow source integration.
Only run in the feature verification checkout; never sends social posts.
"""
from pathlib import Path
from io import BytesIO
from PIL import Image, ImageFile
import hashlib
import ast
import json

ROOT = Path(__file__).resolve().parents[1]
SOURCE_SHA = 'b2b402d084786d14e410dff6e27917abf124071c2d7736ec9bbe088007f2e891'
ASSET = 'static/detective/v20260911/approved.jpg'


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise ValueError('Unexpected integration anchor: ' + old[:90])
    return text.replace(old, new, 1)


def run():
    ImageFile.LOAD_TRUNCATED_IMAGES = False
    archived = ROOT / 'ops/media-source/approved-20260911.avif'
    parts = [ROOT / f'ops/media-import/part{i}.bin' for i in range(5)]
    if all(p.is_file() for p in parts):
        raw = b''.join(p.read_bytes() for p in parts)
    else:
        raw = archived.read_bytes()
    if len(raw) != 16716 or hashlib.sha256(raw).hexdigest() != SOURCE_SHA:
        raise ValueError('Approved image transfer checksum mismatch')
    target = ROOT / ASSET
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        with Image.open(BytesIO(raw)) as im:
            im.load()
            if im.size != (640, 800):
                raise ValueError('Unexpected source dimensions')
            im.convert('RGB').save(target, 'JPEG', quality=94, optimize=True, progressive=False)
    with Image.open(target) as im:
        im.load()
        assert im.format == 'JPEG' and im.size == (640, 800)
    jpg = target.read_bytes()
    (target.parent / 'manifest.json').write_text(json.dumps({
        'revision': 'approved-photo-20260911', 'file': 'approved.jpg',
        'source_sha256': SOURCE_SHA, 'sha256': hashlib.sha256(jpg).hexdigest(),
        'width': 640, 'height': 800, 'rotation': 'fixed_approved_photo'
    }, indent=2) + '\n')
    # Repair the earlier truncated alias too; new posts use the cache-safe versioned URL.
    (ROOT / 'static/detective/approved.jpg').write_bytes(jpg)
    archived.parent.mkdir(parents=True, exist_ok=True)
    archived.write_bytes(raw)
    for p in parts:
        if p.exists(): p.unlink()

    for name in ('detective_posts.py', 'social_mix.py'):
        path = ROOT / 'app' / name
        text = path.read_text()
        marker = '# Verified approved-photo integration.'
        if marker not in text:
            text = replace_once(text, 'from . import social_schedule as plan', 'from . import social_schedule as plan\nfrom . import detective_media\n' + marker)
            text = replace_once(text, "APPROVED_ASSET = 'static/detective/approved.jpg'", 'APPROVED_ASSET = detective_media.ASSET')
            if name == 'detective_posts.py':
                text = replace_once(text, 'return APPROVED_ASSET if (ROOT / APPROVED_ASSET).is_file() else None', "return APPROVED_ASSET if detective_media.inspect(ROOT)['ready'] else None")
                text = replace_once(text, "'approved_scene_images':1 if (ROOT/APPROVED_ASSET).is_file() else 0,", "'approved_scene_images':int(detective_media.inspect(ROOT)['ready']),\n            'approved_media':detective_media.inspect(ROOT),\n            'caption_prefix':'🕵️ SNS捜査官｜BUZZ NOW', 'caption_hashtags':['#SNS捜査官'],")
                text = replace_once(text, "not (ROOT/APPROVED_ASSET).is_file()", "not detective_media.inspect(ROOT)['ready']")
            else:
                text = replace_once(text, 'if not (ROOT / APPROVED_ASSET).is_file():', "if not detective_media.inspect(ROOT)['ready']:")
                text = replace_once(text, "'character_image': '/static/detective/approved.jpg',", "'character_image': '/' + APPROVED_ASSET,\n            'media_revision': detective_media.REVISION,")
            ast.parse(text)
            path.write_text(text)

    schedule = (ROOT / 'app/social_schedule.py').read_text()
    assert "return '🕵️ SNS捜査官｜BUZZ NOW\\n\\n' + prompt + '\\n\\n#SNS捜査官'" in schedule
    assert '公式AIキャラクター' not in schedule and '#AIキャラクター' not in schedule
    # Remove only the known broken upload scratch files created during this request.
    for name in ('approved-image-test.txt','approved-image.1.b64','approved-image.b64',
                 'approved-image.final.b64','approved-image.full.b64','approved-image.txt','approved-240.txt'):
        p = ROOT / 'static/detective' / name
        if p.exists() and p.stat().st_size < 100:
            p.unlink()
    print('Approved photo verified and both social paths integrated; no post sent.')


if __name__ == '__main__': run()
