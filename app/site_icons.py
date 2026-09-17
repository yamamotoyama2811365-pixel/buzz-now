"""Public site icons and lightweight site-ops router mount."""
import os
from pathlib import Path
from fastapi import APIRouter
from fastapi.responses import FileResponse

router = APIRouter()
STATIC = Path(__file__).resolve().parents[1] / 'static'
HEADERS = {'Cache-Control': 'public, max-age=86400', 'X-Content-Type-Options': 'nosniff'}


@router.api_route('/favicon.ico', methods=['GET', 'HEAD'], include_in_schema=False)
def favicon():
    return FileResponse(STATIC / 'favicon.ico', media_type='image/vnd.microsoft.icon', headers=HEADERS)


@router.api_route('/apple-touch-icon.png', methods=['GET', 'HEAD'], include_in_schema=False)
def apple_touch_icon():
    return FileResponse(STATIC / 'apple-touch-icon.png', media_type='image/png', headers=HEADERS)


# Keep the main application wiring stable: app.main already includes this router.
# Site Ops owns its own protected routes and background collectors.
# The production app's DATABASE_URL can be read-only during migration-safe runtime,
# so Site Ops uses its own dedicated RW Neon connection without changing app.main.
from app import site_ops as _site_ops
_site_ops.DATABASE_URL = os.getenv('SITE_OPS_DATABASE_URL', _site_ops.DATABASE_URL).strip()
if _site_ops.ENABLED and not _site_ops.IS_LEGACY and _site_ops._scheduler is None:
    _site_ops._start_scheduler()
site_ops_router = _site_ops.router
router.include_router(site_ops_router)
