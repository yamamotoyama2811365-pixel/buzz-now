"""Prepare favicon assets and narrowly wire them on a tested feature branch."""
import ast
import hashlib
from pathlib import Path
from PIL import Image

ROOT = Path(__file__).resolve().parents[1]
MARKER = '# Approved BUZZ NOW site icons (September 2026).'
MASTER_SHA256 = 'b0c28a9751b39ec57a99d93d8b022bb66b1a5b78d58546091295f54586c90bda'


def install():
    main = ROOT / 'app/main.py'
    source = main.read_text()
    master = ROOT / 'static/buzz-now-icon.png'
    if hashlib.sha256(master.read_bytes()).hexdigest() != MASTER_SHA256:
        raise ValueError('Approved icon checksum mismatch')
    if MARKER in source:
        return
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in ('get','head','api_route'):
            if node.args and isinstance(node.args[0], ast.Constant) and node.args[0].value in ('/favicon.ico','/apple-touch-icon.png'):
                raise ValueError('An existing icon route requires review before integration')
    addition = '\n\n' + MARKER + '\nfrom app.site_icons import router as site_icons_router\napp.include_router(site_icons_router)\n'
    ast.parse(source + addition)
    updated = {}
    for name in ('index.html','trend.html','x_gate.html'):
        path = ROOT / 'templates' / name
        html = path.read_text()
        if html.count('</head>') != 1 or '_site_icons.html' in html:
            raise ValueError('Unexpected template head: ' + name)
        updated[path] = html.replace('</head>', '{% include "_site_icons.html" %}\n</head>', 1)
    with Image.open(master) as approved:
        rgba = approved.convert('RGBA')
        if rgba.size != (192,192):
            raise ValueError('Expected a square 192px source')
        rgba.save(ROOT/'static/favicon.ico', format='ICO', sizes=[(16,16),(32,32),(48,48),(64,64)])
        # Opaque square for Apple; iOS applies its own corner mask.
        touch = Image.new('RGBA', rgba.size, (204,237,191,255))
        touch.alpha_composite(rgba)
        touch.convert('RGB').resize((180,180), Image.Resampling.LANCZOS).save(ROOT/'static/apple-touch-icon.png', optimize=True)
    main.write_text(source + addition)
    for path, html in updated.items():
        path.write_text(html)
    print('Integrated approved icon into 3 BUZZ NOW templates and 2 public routes')


if __name__ == '__main__':
    install()
