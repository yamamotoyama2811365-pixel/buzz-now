"""Public site icons only: no database, tracking, or external service calls."""
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
