"""Validated user-approved investigator photo; no generation or network calls."""
import hashlib
import json
from pathlib import Path
from PIL import Image

ASSET = 'static/detective/v20260911/approved.jpg'
MANIFEST = 'static/detective/v20260911/manifest.json'
REVISION = 'approved-photo-20260911'


def inspect(root):
    root = Path(root)
    try:
        info = json.loads((root / MANIFEST).read_text())
        raw = (root / ASSET).read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        if info.get('source_sha256') != 'b2b402d084786d14e410dff6e27917abf124071c2d7736ec9bbe088007f2e891':
            raise ValueError('wrong approved source')
        if digest != info.get('sha256') or not raw.startswith(b'\xff\xd8') or not raw.endswith(b'\xff\xd9'):
            raise ValueError('image bytes not verified')
        with Image.open(root / ASSET) as im:
            if im.format != 'JPEG' or im.size != (640, 800):
                raise ValueError('wrong photo format or size')
            im.load()
        return {'ready': True, 'revision': REVISION, 'image': '/' + ASSET,
                'sha256': digest, 'width': 640, 'height': 800,
                'rotation': 'fixed_approved_photo', 'runtime_generation': False}
    except (OSError, ValueError, KeyError):
        return {'ready': False, 'revision': REVISION, 'image': '/' + ASSET,
                'rotation': 'fixed_approved_photo', 'runtime_generation': False}
