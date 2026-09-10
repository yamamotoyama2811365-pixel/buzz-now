"""Optional GA4 measurement for corporate pages only."""
import os
import re


def analytics_tag():
    measurement_id = os.getenv("CORPORATE_GA4_ID", "").strip()
    if not re.fullmatch(r"G-[A-Z0-9]{6,20}", measurement_id):
        return ""
    return (
        '<script async src="https://www.googletagmanager.com/gtag/js?id='
        + measurement_id
        + '"></script><script>'
        'window.dataLayer=window.dataLayer||[];'
        'function gtag(){dataLayer.push(arguments);}'
        "gtag('js',new Date());"
        "gtag('config','" + measurement_id + "',{"
        "content_group:'corporate',"
        "page_location:window.location.origin+window.location.pathname"
        "});</script>"
    )
