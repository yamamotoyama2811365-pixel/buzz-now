from urllib.parse import quote_plus

def google_news_rss(query: str):
    q = quote_plus(query)
    return f"https://news.google.com/rss/search?q={q}&hl=ja&gl=JP&ceid=JP:ja"

# Discovery layer.
# We store factual candidate fields and source links only; article bodies/images are not republished.
RSS_SOURCES = [
    {
        "key":"news_open_general",
        "name":"Google News - 開店",
        "url":google_news_rss('"開店" OR "オープン" OR "新店舗" OR "新店" 店舗'),
        "limit":80,"channel":"rss_general","confidence_floor":60
    },
    {
        "key":"news_close_general",
        "name":"Google News - 閉店",
        "url":google_news_rss('"閉店" OR "閉館" OR "営業終了" 店舗'),
        "limit":80,"channel":"rss_general","confidence_floor":60
    },
    {
        "key":"news_tenant_general",
        "name":"Google News - テナント",
        "url":google_news_rss('"テナント募集" OR "居抜き" OR "空き店舗" OR "貸店舗"'),
        "limit":40,"channel":"rss_tenant","confidence_floor":65
    },

    # Local opening/closing coverage
    {
        "key":"goguynet",
        "name":"号外NET",
        "url":google_news_rss('site:goguynet.jp ("開店" OR "閉店" OR "オープン")'),
        "limit":80,"channel":"rss_local","confidence_floor":72
    },

    # Retail / store trade media
    {
        "key":"ryutsuu",
        "name":"流通ニュース",
        "url":google_news_rss('site:ryutsuu.biz ("オープン" OR "閉店" OR "新店" OR "出店")'),
        "limit":60,"channel":"rss_retail","confidence_floor":76
    },
    {
        "key":"diamond_chainstore",
        "name":"ダイヤモンド・チェーンストア",
        "url":google_news_rss('site:dcs.diamond-rm.net ("オープン" OR "閉店" OR "新店" OR "出店")'),
        "limit":60,"channel":"rss_retail","confidence_floor":76
    },
    {
        "key":"retail_leaders",
        "name":"リテール・リーダーズ",
        "url":google_news_rss('site:retailguide.tokubai.co.jp ("オープン" OR "閉店" OR "新店")'),
        "limit":50,"channel":"rss_retail","confidence_floor":74
    },
    {
        "key":"shokuhin_news",
        "name":"食品新聞",
        "url":google_news_rss('site:shokuhin.net ("オープン" OR "閉店" OR "新店")'),
        "limit":50,"channel":"rss_retail","confidence_floor":74
    },

    # Fashion / specialty stores / commercial facilities
    {
        "key":"fashion_press",
        "name":"Fashion Press",
        "url":google_news_rss('site:fashion-press.net ("ショップオープン" OR "新店舗" OR "オープン")'),
        "limit":50,"channel":"rss_specialty","confidence_floor":70
    },
    {
        "key":"fashionsnap",
        "name":"FASHIONSNAP",
        "url":google_news_rss('site:fashionsnap.com ("オープン" OR "新店舗" OR "旗艦店")'),
        "limit":40,"channel":"rss_specialty","confidence_floor":70
    },
    {
        "key":"wwdjapan",
        "name":"WWDJAPAN",
        "url":google_news_rss('site:wwdjapan.com ("オープン" OR "新店舗" OR "閉店")'),
        "limit":40,"channel":"rss_specialty","confidence_floor":70
    },
    {
        "key":"impress_watch",
        "name":"Impress Watch",
        "url":google_news_rss('site:watch.impress.co.jp ("オープン" OR "開業") (店舗 OR 商業施設)'),
        "limit":40,"channel":"rss_facility","confidence_floor":70
    },

    # Press release / official announcement discovery
    {
        "key":"prtimes",
        "name":"PR TIMES",
        "url":google_news_rss('site:prtimes.jp ("新店舗" OR "新店" OR "グランドオープン" OR "閉店")'),
        "limit":80,"channel":"rss_press","confidence_floor":74
    },
    {
        "key":"atpress",
        "name":"@Press",
        "url":google_news_rss('site:atpress.ne.jp ("新店舗" OR "オープン" OR "閉店")'),
        "limit":50,"channel":"rss_press","confidence_floor":70
    },
    {
        "key":"straightpress",
        "name":"STRAIGHT PRESS",
        "url":google_news_rss('site:straightpress.jp ("オープン" OR "新店舗")'),
        "limit":40,"channel":"rss_press","confidence_floor":68
    },

    # Major chains: early official-source discovery
    {
        "key":"aeon_official",
        "name":"イオン公式",
        "url":google_news_rss('site:aeonretail.jp ("オープン" OR "新店" OR "開店")'),
        "limit":30,"channel":"rss_official","confidence_floor":78
    },
    {
        "key":"lawson_official",
        "name":"ローソン公式",
        "url":google_news_rss('site:lawson.co.jp ("オープン" OR "新店" OR "開店")'),
        "limit":30,"channel":"rss_official","confidence_floor":78
    },
    {
        "key":"familymart_official",
        "name":"ファミリーマート公式",
        "url":google_news_rss('site:family.co.jp ("オープン" OR "新店" OR "開店")'),
        "limit":30,"channel":"rss_official","confidence_floor":78
    },
]

