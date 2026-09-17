from fastapi import Request
from fastapi.responses import PlainTextResponse

from app.main import app


@app.middleware("http")
async def ads_txt_with_imobile(request: Request, call_next):
    if request.url.path == "/ads.txt":
        return PlainTextResponse(
            "google.com, pub-5776658615046901, DIRECT, f08c47fec0942fa0\n"
            "i-mobile.co.jp, 85420, DIRECT\n"
        )
    return await call_next(request)
