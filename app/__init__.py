"""BUZZ NOW package hooks kept intentionally small and reversible."""

from fastapi import responses as _responses


_OriginalPlainTextResponse = _responses.PlainTextResponse
_GOOGLE_ADS_LINE = "google.com, pub-5776658615046901, DIRECT, f08c47fec0942fa0"
_IMOBILE_ADS_LINE = "i-mobile.co.jp, 85420, DIRECT"


class _AdsTxtAwarePlainTextResponse(_OriginalPlainTextResponse):
    def __init__(self, content=None, *args, **kwargs):
        if isinstance(content, str) and content.strip() == _GOOGLE_ADS_LINE:
            content = _GOOGLE_ADS_LINE + "\n" + _IMOBILE_ADS_LINE + "\n"
        super().__init__(content, *args, **kwargs)


_responses.PlainTextResponse = _AdsTxtAwarePlainTextResponse
