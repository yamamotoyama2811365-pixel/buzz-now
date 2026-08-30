import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from app.main import app
paths={r.path for r in app.routes}
required={"/","/health","/ready","/robots.txt","/sitemap.xml","/api/runtime"}
missing=required-paths
if missing:
    raise SystemExit(f"Missing routes: {sorted(missing)}")
print("BUZZ NOW smoke test: OK")
