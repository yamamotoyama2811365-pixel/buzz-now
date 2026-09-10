from urllib.parse import urlparse
import re
import httpx
from bs4 import BeautifulSoup

def is_google_news_url(url: str) -> bool:
    try:
        host = (urlparse(url).hostname or "").lower()
        return host == "news.google.com"
    except Exception:
        return False

def _fallback_html_resolve(url: str):
    """
    Simple fallback only. Does not bypass CAPTCHA/rate limits.
    """
    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=12.0,
            headers={
                "User-Agent":"Mozilla/5.0 (compatible; OpenCloseMap/1.0)",
                "Accept-Language":"ja,en;q=0.8",
            }
        ) as client:
            r = client.get(url)
            final = str(r.url)
            if not is_google_news_url(final):
                return {"ok":True,"url":final,"method":"http_redirect"}

            if r.status_code != 200:
                return {"ok":False,"url":url,"method":"html","error":f"http_{r.status_code}"}

            soup = BeautifulSoup(r.text[:1_000_000], "html.parser")

            # meta refresh
            meta = soup.find("meta", attrs={"http-equiv": re.compile("^refresh$", re.I)})
            if meta:
                content = meta.get("content") or ""
                m = re.search(r'url\s*=\s*(https?://[^;]+)', content, re.I)
                if m and not is_google_news_url(m.group(1)):
                    return {"ok":True,"url":m.group(1).strip("'\" "), "method":"meta_refresh"}

            # canonical / og:url
            for selector, attr in [
                ('link[rel="canonical"]',"href"),
                ('meta[property="og:url"]',"content"),
            ]:
                tag = soup.select_one(selector)
                if tag:
                    candidate = (tag.get(attr) or "").strip()
                    if candidate.startswith("http") and not is_google_news_url(candidate):
                        return {"ok":True,"url":candidate,"method":"canonical"}

            # External links
            for a in soup.find_all("a", href=True):
                href = a.get("href","").strip()
                if href.startswith("http") and not is_google_news_url(href):
                    host = (urlparse(href).hostname or "").lower()
                    if host and "google." not in host and "gstatic." not in host:
                        return {"ok":True,"url":href,"method":"external_link"}

    except Exception as e:
        return {"ok":False,"url":url,"method":"html","error":str(e)[:180]}

    return {"ok":False,"url":url,"method":"html","error":"unresolved"}

def resolve_google_news_url(url: str):
    if not url:
        return {"ok":False,"url":url,"method":"none","error":"empty_url"}

    if not is_google_news_url(url):
        return {"ok":True,"url":url,"method":"passthrough"}

    # Main decoder. This library decodes Google News RSS/read wrappers.
    try:
        from googlenewsdecoder import gnewsdecoder
        decoded = gnewsdecoder(url, interval=1)

        if isinstance(decoded, dict) and decoded.get("status"):
            target = decoded.get("decoded_url")
            if target and target.startswith("http") and not is_google_news_url(target):
                return {"ok":True,"url":target,"method":"googlenewsdecoder"}
    except Exception as e:
        decoder_error = str(e)[:180]
    else:
        decoder_error = "decoder_returned_no_url"

    fallback = _fallback_html_resolve(url)
    if fallback.get("ok"):
        return fallback

    fallback["decoder_error"] = decoder_error
    return fallback

