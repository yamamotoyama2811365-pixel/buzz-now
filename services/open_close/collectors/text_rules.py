from .public_quality import valid_floor, valid_facility
import re
from datetime import date

OPEN_KEYWORDS=["オープン","OPEN","開店","新店舗","新店","グランドオープン","リニューアルオープン","出店"]
CLOSE_KEYWORDS=["閉店","閉館","営業終了","撤退"]
TENANT_KEYWORDS=["テナント募集","居抜き","空き店舗","貸店舗"]

PREFECTURES=[
"北海道","青森県","岩手県","宮城県","秋田県","山形県","福島県","茨城県","栃木県","群馬県",
"埼玉県","千葉県","東京都","神奈川県","新潟県","富山県","石川県","福井県","山梨県","長野県",
"岐阜県","静岡県","愛知県","三重県","滋賀県","京都府","大阪府","兵庫県","奈良県","和歌山県",
"鳥取県","島根県","岡山県","広島県","山口県","徳島県","香川県","愛媛県","高知県","福岡県",
"佐賀県","長崎県","熊本県","大分県","宮崎県","鹿児島県","沖縄県"
]

CATEGORY_RULES=[
    # 飲食：具体的な業態を先に判定
    ("ラーメン",[
        "ラーメン","らーめん","中華そば","つけ麺","油そば","まぜそば"
    ]),
    ("中華",[
        "中華料理","中国料理","町中華","中華","餃子","点心","小籠包",
        "四川料理","四川","広東料理","台湾料理","麻婆","担々麺"
    ]),
    ("寿司",["寿司","鮨","すし","回転寿司"]),
    ("焼肉",["焼肉","焼き肉","ホルモン"]),
    ("焼鳥・串",["焼鳥","焼き鳥","串焼き","串カツ","串揚げ"]),
    ("カレー",["カレー","スープカレー"]),
    ("うどん・そば",["うどん","蕎麦","そば"]),
    ("カフェ",[
        "カフェ","喫茶","コーヒー","珈琲","スターバックス",
        "ドトール","タリーズ","コメダ"
    ]),
    ("パン・ベーカリー",[
        "ベーカリー","パン屋","食パン","ブーランジェリー","Bakery","BAKERY"
    ]),
    ("スイーツ",[
        "スイーツ","ケーキ","洋菓子","和菓子","マカロン","クレープ",
        "ジェラート","アイスクリーム","文明堂"
    ]),
    ("ファストフード",[
        "マクドナルド","モスバーガー","バーガーキング","ロッテリア",
        "ケンタッキー","KFC","ハンバーガー"
    ]),
    ("洋食",[
        "イタリアン","フレンチ","ビストロ","パスタ","ピザ","PIZZA",
        "洋食","ステーキ"
    ]),
    ("和食",[
        "和食","定食","天ぷら","天丼","とんかつ","しゃぶしゃぶ",
        "すき焼き","おばんざい"
    ]),
    ("居酒屋",[
        "居酒屋","酒場","大衆酒場","バル","立ち飲み","立呑み"
    ]),
    ("飲食店",["レストラン","食堂","ダイニング","飲食店"]),

    # 小売・サービス
    ("コンビニ",[
        "セブン-イレブン","セブンイレブン","ローソン",
        "ファミリーマート","ミニストップ","コンビニ"
    ]),
    ("スーパー",[
        "スーパーマーケット","食品スーパー","スーパー","イオン",
        "マックスバリュ","業務スーパー"
    ]),
    ("ドラッグストア",[
        "ドラッグストア","薬局","ツルハ","サツドラ","マツモトキヨシ",
        "ウエルシア","ココカラファイン"
    ]),
    ("美容",[
        "美容室","美容院","ヘアサロン","ヘアカラー","カラー専門店",
        "ネイル","エステ","アイラッシュ","まつげ","ビューティー"
    ]),
    ("アパレル",[
        "アパレル","衣料","ファッション","UNIQLO","ユニクロ",
        "GU","ジーユー","洋服"
    ]),
    ("書店・メディア",[
        "書店","本屋","BOOK","ブック","TSUTAYA","蔦屋書店"
    ]),
    ("家電",[
        "家電","電器","ヤマダデンキ","ビックカメラ","ヨドバシ",
        "ケーズデンキ","エディオン"
    ]),
    ("ホームセンター",[
        "ホームセンター","DCM","コーナン","カインズ"
    ]),
    ("フィットネス",[
        "フィットネス","ジム","GYM","スポーツクラブ","ピラティス",
        "ヨガ"
    ]),
    ("ホテル",[
        "ホテル","旅館","宿泊施設","HOTEL"
    ]),
    ("雑貨",[
        "雑貨","生活雑貨","インテリア","無印良品"
    ]),
    ("小売",["ショップ","専門店","ストア"])
]

NEGATIVE_ADDRESS_LABELS=[
    "本社所在地","本社住所","会社所在地","会社住所","運営会社","会社概要",
    "問い合わせ先","お問い合わせ先","連絡先","本部所在地","事務局所在地"
]

POSITIVE_ADDRESS_LABELS=[
    "店舗所在地","施設所在地","会場","開催場所","所在地","住所","場所","アクセス"
]

POSITIVE_NAME_LABELS=[
    "店舗名","施設名","会場名","名称","名 称","ホテル名","店名"
]

def norm(text):
    if not text:
        return ""
    text=text.replace("\u3000"," ")
    text=re.sub(r'[ \t]+',' ',text)
    return text.strip()

def classify_title(title):
    if any(k in title for k in TENANT_KEYWORDS): return "tenant",65
    if any(k in title for k in CLOSE_KEYWORDS): return "closing",60
    if any(k in title for k in OPEN_KEYWORDS): return "opening",60
    return None,0

def detect_prefecture(text):
    return next((p for p in PREFECTURES if p in (text or "")),None)

def detect_city(text):
    t=text or ""
    m=re.search(r'([一-龥ぁ-んァ-ヶー]{1,10}市[一-龥ぁ-んァ-ヶー]{1,10}区)',t)
    if m:return m.group(1)
    m=re.search(r'([一-龥ぁ-んァ-ヶー]{1,15}(?:市|区|町|村))',t)
    return m.group(1) if m else None

def detect_category(text, store_name=None):
    """
    店名を最優先し、その後にタイトル/本文を判定。
    具体業態を先に並べることで「中華そば」は中華ではなくラーメンになる。
    """
    primary=norm(store_name or "")
    secondary=norm(text or "")

    if primary:
        for c,ks in CATEGORY_RULES:
            if any(k.lower() in primary.lower() for k in ks):
                return c

    for c,ks in CATEGORY_RULES:
        if any(k.lower() in secondary.lower() for k in ks):
            return c

    return None

def extract_date(text):
    t=text or ""
    m=re.search(r'(20\d{2})[年/.\-]\s*(\d{1,2})[月/.\-]\s*(\d{1,2})日?',t)
    if m:
        try:return date(int(m.group(1)),int(m.group(2)),int(m.group(3)))
        except:return None
    m=re.search(r'(\d{1,2})月\s*(\d{1,2})日',t)
    if m:
        try:return date(date.today().year,int(m.group(1)),int(m.group(2)))
        except:return None
    return None

def clean_store_name(title):
    t=re.sub(r'\s*[-｜|]\s*[^-｜|]+$','',title or '').strip()
    t=re.sub(r'【[^】]+】','',t)
    t=re.sub(r'(が|を|は)?\s*(新規)?(オープン|OPEN|開店|閉店|閉館|営業終了|出店).*$','',t,flags=re.I)
    return t.strip(' 「」『』【】:：')[:120] or (title or "")[:120]

def extract_source_name_from_title(title):
    if not title:return None
    # last suffix after dash / pipe, common in Google News titles
    m=re.search(r'\s*[-｜|]\s*([^-｜|]{2,60})\s*$',title)
    if m:
        s=norm(m.group(1))
        if s and "Google News" not in s:
            return s
    return None

def extract_postal_code(text):
    m=re.search(r'〒?\s*(\d{3})[-ー－](\d{4})',text or '')
    return f"{m.group(1)}-{m.group(2)}" if m else None

def extract_floor(text):
    import unicodedata
    t=unicodedata.normalize("NFKC", text or "")
    for match in re.finditer(r'(?<![0-9A-Za-z])((?:地下|B)?\s*[1-9][0-9]?\s*(?:階|F))(?![0-9A-Za-z])',t,re.I):
        value=valid_floor(match.group(1))
        if value:
            return value
    return None

def _label_value(text, labels):
    if not text:return None
    t=text.replace('\r','\n')
    alt='|'.join(re.escape(x) for x in labels)
    patterns=[
        rf'[［【\[]\s*(?:{alt})\s*[］】\]]\s*[:：]?\s*([^\n\r]+)',
        rf'(?:^|\n)\s*(?:{alt})\s*[:：]\s*([^\n\r]+)',
        rf'(?:^|\n)\s*(?:{alt})\s+([^\n\r]+)',
    ]
    for p in patterns:
        m=re.search(p,t,re.I|re.M)
        if m:return norm(m.group(1))
    return None

def contains_negative_context(text):
    t=text or ""
    return any(x in t for x in NEGATIVE_ADDRESS_LABELS)

def clean_address_candidate(value):
    if not value:return None
    v=norm(value)
    v=re.sub(r'^〒?\s*\d{3}[-ー－]\d{4}\s*','',v)
    v=re.split(r'(?:TEL|電話|営業時間|アクセス|URL|公式|定休日|予約|問い合わせ|お問い合わせ|オープン|開店|閉店|出店|営業開始|営業終了)',v,maxsplit=1)[0]
    v=v.strip("　 ,、。;；|｜[]［］")
    if not detect_prefecture(v):return None
    return v[:160]

def address_matches_expected_prefecture(address, expected_prefecture):
    if not address or not expected_prefecture:
        return True
    return detect_prefecture(address) == expected_prefecture

def extract_labeled_address_block(text):
    """
    Returns value + context label.
    Avoids company/head-office blocks.
    """
    if not text:return (None,None)
    t=text.replace('\r','\n')
    lines=[norm(x) for x in t.split('\n') if norm(x)]

    for i,line in enumerate(lines):
        # Skip obvious negative labels/sections
        if any(lbl in line for lbl in NEGATIVE_ADDRESS_LABELS):
            continue

        for label in POSITIVE_ADDRESS_LABELS:
            if label in line:
                # If same line contains value
                after=re.split(re.escape(label),line,maxsplit=1)[-1]
                after=after.lstrip("：: ]］】")
                if detect_prefecture(after):
                    return clean_address_candidate(after),label

                # Try next line
                if i+1 < len(lines):
                    nxt=lines[i+1]
                    if not contains_negative_context(nxt) and detect_prefecture(nxt):
                        return clean_address_candidate(nxt),label
    return (None,None)

def extract_address(text, expected_prefecture=None):
    if not text:return None

    # 1) Positive labels / block
    addr,label=extract_labeled_address_block(text)
    if addr and address_matches_expected_prefecture(addr,expected_prefecture):
        return addr

    # 2) Postal + address, but reject negative contexts around it
    t=text.replace('\r','\n')
    for m in re.finditer(
        r'〒?\s*\d{3}[-ー－]\d{4}\s*('
        + '|'.join(map(re.escape,PREFECTURES))
        + r')[一-龥ぁ-んァ-ヶー0-9０-９\-ー丁目番地号ノの\s]{3,120}',
        t
    ):
        start=max(0,m.start()-80)
        context=t[start:m.start()+len(m.group(0))+30]
        if contains_negative_context(context):
            continue
        full=re.sub(r'^〒?\s*\d{3}[-ー－]\d{4}\s*','',m.group(0))
        full=re.split(r'(?:TEL|電話|営業時間|アクセス|URL|公式|定休日)',full,maxsplit=1)[0]
        addr=clean_address_candidate(full)
        if addr and address_matches_expected_prefecture(addr,expected_prefecture):
            return addr

    # 3) Prefecture-starting numbered address
    flat=norm(t)
    prefs=[expected_prefecture] if expected_prefecture else PREFECTURES
    for pref in prefs:
        if not pref:continue
        for m in re.finditer(re.escape(pref),flat):
            start=max(0,m.start()-70)
            precontext=flat[start:m.start()]
            if contains_negative_context(precontext):
                continue
            tail=flat[m.start():m.start()+180]
            mm=re.match(
                re.escape(pref)
                + r'[一-龥ぁ-んァ-ヶー]{1,20}(?:市|区|町|村)'
                + r'[一-龥ぁ-んァ-ヶー0-9０-９\-ー丁目番地号ノの\s]{1,90}',
                tail
            )
            if mm and re.search(r'\d',mm.group(0)):
                candidate=re.split(r'(?:TEL|電話|営業時間|アクセス|URL|公式|定休日)',mm.group(0),maxsplit=1)[0]
                addr=clean_address_candidate(candidate)
                if addr and address_matches_expected_prefecture(addr,expected_prefecture):
                    return addr
    return None

def extract_facility_name(text, address=None):
    if not text:return None

    labeled=_label_value(text,POSITIVE_NAME_LABELS)
    if labeled:
        v=re.split(r'(?:所在地|住所|TEL|電話|営業時間|開業日)',labeled,maxsplit=1)[0]
        v=norm(v).strip("、。:：")
        if 2<=len(v)<=80:return v

    # Prefer known venue-like names
    t=norm(text)
    patterns=[
        r'([A-Za-z0-9一-龥ぁ-んァ-ヶー・＆&\-\s]{2,50}(?:ショッピングセンター|ショッピングモール|モール|プラザ|タワー|ビル|ホテル|館|センター|百貨店|店))'
    ]
    for p in patterns:
        for m in re.finditer(p,t,re.I):
            v=norm(m.group(1)).strip("、。:：")
            if address and v in address:continue
            if contains_negative_context(v):continue
            if 2<=len(v)<=80:return v
    return None

def calculate_confidence(title,summary,status,prefecture,city,event_date,category,address=None,facility=None):
    s=55
    if status in ("opening","closing"):s+=5
    if prefecture:s+=5
    if city:s+=5
    if event_date:s+=10
    if category:s+=5
    if address:s+=10
    if facility:s+=4
    if any(k in ((title or "")+" "+(summary or "")) for k in ["公式","発表","プレスリリース"]):s+=10
    return min(s,98)

ADDRESS_REJECT_WORDS = [
    "オープン","OPEN","開店","閉店","出店","新店舗","ニュース","PR TIMES",
    "プレスリリース","登場","開催","フェス","イベント","公式サイト","写真・画像"
]

PLACE_WORDS = [
    "公園","駅","空港","ホール","ドーム","アリーナ","会館","広場","センター",
    "百貨店","モール","ショッピングセンター","ホテル","ビル","タワー","プラザ","館"
]

def is_plausible_street_address(address):
    """
    住所として保存してよい文字列かを厳しめに判定。
    施設名だけ・記事見出し断片・市区町村止まりは落とす。
    """
    if not address:
        return False

    a = norm(address)

    # 明らかな記事文言を含むものは却下
    if any(w.lower() in a.lower() for w in ADDRESS_REJECT_WORDS):
        return False

    # 都道府県必須
    pref = detect_prefecture(a)
    if not pref:
        return False

    # 市区町村相当が必要
    if not re.search(r'(市|区|町|村)', a):
        return False

    # 番地系の数字は必須。ただし「安岡1003」のように
    # 丁目/番/号の文字を使わない正規住所もあるため数字だけでも許容。
    has_number = bool(re.search(r'\d', a))
    if not has_number:
        return False

    # 数字が市区町村より後ろに存在すること
    muni = re.search(r'(市|区|町|村)', a)
    if muni and not re.search(r'\d', a[muni.end():]):
        return False

    # 「○○公園」など施設名だけで終わっているものを除外
    # ただし番地まで続いていれば住所として可
    for w in PLACE_WORDS:
        if a.endswith(w) and not re.search(r'\d', a[a.rfind(w)+len(w):]):
            return False

    # 末尾の不自然な記号
    if a.endswith(("-", "－", "ー", "・", "/", "：", ":")):
        return False

    # 長すぎる見出し断片は却下
    if len(a) > 120:
        return False

    return True

EVENT_ONLY_KEYWORDS = [
    "出店者募集","物産展","フェス","イベント","キッチンカー",
    "マルシェ","催事","期間限定販売","試合","秋田戦","写真・画像"
]

PERMANENT_OPEN_KEYWORDS = [
    "新店舗","新店","開店","グランドオープン","リニューアルオープン",
    "常設店","ロードサイド店舗","店舗をオープン","店をオープン",
    "号店","店舗目"
]

def is_likely_non_store_event(title, summary=""):
    t = f"{title or ''} {summary or ''}"

    # strong exclusions
    if "出店者募集" in t:
        return True

    # event/temporary appearance terms
    has_event = any(k in t for k in EVENT_ONLY_KEYWORDS)

    # "物産展に登場", sports-event vendor, etc.
    if "物産展" in t and ("登場" in t or "出店" in t):
        return True
    if ("戦" in t or "試合" in t) and "出店" in t:
        return True
    if "フェス" in t and ("出店" in t or "募集" in t):
        return True

    # If event term exists but clear permanent-store wording also exists,
    # keep it. Otherwise exclude.
    if has_event:
        if any(k in t for k in PERMANENT_OPEN_KEYWORDS):
            return False
        return True

    return False

# Override classifier with event filtering.
def classify_title(title):
    if is_likely_non_store_event(title) or is_likely_aggregate_store_article(title):
        return None,0
    if any(k in title for k in TENANT_KEYWORDS): return "tenant",65
    if any(k in title for k in CLOSE_KEYWORDS): return "closing",60
    if any(k in title for k in OPEN_KEYWORDS): return "opening",60
    return None,0

_old_extract_facility_name = extract_facility_name

def extract_facility_name(text, address=None):
    v = _old_extract_facility_name(text, address)
    if not v:
        return None

    bad = [
        "初出店","出店者","出店","募集","オープン","閉店",
        "写真・画像","ニュース","PR TIMES"
    ]
    vv = norm(v)
    if vv.startswith("の"):
        return None
    if any(x in vv for x in bad):
        return None
    if len(vv) < 2:
        return None
    return valid_facility(vv)

STORE_NAME_HINTS = [
    "店","カフェ","レストラン","食堂","ホテル","ショップ","ストア","サロン",
    "クリニック","薬局","センター","館","ミロード","モール","プラザ"
]

def extract_best_store_name_from_title(title):
    """
    Google News見出しから、現在の雑な candidate より信頼できる店舗名を拾う。
    引用符内の名称を最優先。
    """
    if not title:
        return None

    t = clean_news_title_for_name(title)

    quoted = []
    for pat in [
        r'「([^」]{2,100})」',
        r'『([^』]{2,100})』',
        r'“([^”]{2,100})”',
        r'"([^"]{2,100})"'
    ]:
        quoted += re.findall(pat, t)

    def qscore(v):
        vv = norm(v)
        s = 0
        if any(h in vv for h in STORE_NAME_HINTS): s += 5
        if "店" in vv: s += 4
        if len(vv) <= 50: s += 2
        if any(b in vv for b in ["出店者募集","初出店","新店舗","写真・画像"]): s -= 6
        return s

    if quoted:
        quoted = [norm(x) for x in quoted if 2 <= len(norm(x)) <= 100]
        quoted.sort(key=qscore, reverse=True)
        if quoted and qscore(quoted[0]) >= 4:
            return quoted[0]

    # Example: スガキヤ ... 本厚木ミロードイーストにオープン
    m = re.search(
        r'「([^」]{2,30})」.*?([A-Za-z0-9一-龥ぁ-んァ-ヶー・\s]{2,50}(?:ミロード|モール|プラザ|センター|館|イースト|ウエスト))'
        r'(?:\s*\d*階)?\s*に(?:オープン|開店)',
        t
    )
    if m:
        brand = norm(m.group(1))
        place = norm(m.group(2))
        if brand and place:
            return f"{brand} {place}店"

    return None

def clean_news_title_for_name(title):
    if not title:
        return ""
    t = re.sub(r'\s*[-｜|]\s*PR TIMES\s*$', '', title, flags=re.I)
    t = re.sub(r'\s*[-｜|]\s*[^-｜|]{2,60}$', '', t)
    return norm(t)

def is_likely_aggregate_store_article(title, summary=""):
    t = f"{title or ''} {summary or ''}"
    patterns = [
        r'国内\s*\d+\s*店舗.*?(?:達成|突破)',
        r'全国\s*\d+\s*店舗.*?(?:達成|突破)',
        r'\d+\s*店舗\s*出店達成',
        r'\d+\s*店舗\s*同時オープン',
    ]
    return any(re.search(p, t) for p in patterns)

HUB_GENERIC_NAMES = {
    "開店・閉店ポータル","開店閉店ポータル","開店閉店.com",
    "開店情報","閉店情報","新店情報","店舗情報","ShopShip",
    "ショップス","ホーム","TOP","トップ"
}

def normalize_hub_store_name(title, fallback=None):
    """
    開店閉店系の記事タイトルから店名だけを抽出。
    専用サイトでは引用符内の名前を広めに採用する。
    """
    t = norm(title or "")
    fb = norm(fallback or "")

    # Site suffix / publication date cleanup
    t = re.sub(r'\s*[-｜|]\s*(?:開店閉店\.com|ShopShip|開店・閉店ポータル).*$','',t,flags=re.I)
    t = re.sub(r'\s+20\d{2}[年/-]\d{1,2}[月/-]\d{1,2}日?\s*$','',t)
    t = re.sub(r'^(?:【|〖|［)(?:開店|閉店|復活|ニューオープン|移転)(?:】|〗|］)\s*','',t)

    # Quotes are highly reliable on hub sources.
    candidates=[]
    for pat in [
        r'「([^」]{2,90})」',
        r'『([^』]{2,90})』',
        r'“([^”]{2,90})”',
        r'"([^"]{2,90})"'
    ]:
        candidates.extend(re.findall(pat,t))

    def clean_name(v):
        v=norm(v).strip("「」『』【】[]（）()、。!！?？:：")
        v=re.sub(r'^(?:新店舗|新店|閉店情報|開店情報)\s*[:：]?\s*','',v)
        return v

    scored=[]
    for raw in candidates:
        v=clean_name(raw)
        if not v or v in HUB_GENERIC_NAMES:
            continue
        if any(x in v for x in ["出店者募集","写真・画像","初出店！","ニューオープン！"]):
            continue
        score=0
        if 2 <= len(v) <= 55: score += 5
        if any(h in v for h in STORE_NAME_HINTS): score += 3
        if "店" in v: score += 2
        # Prefer later quoted names when article introduces a store after context.
        score += min(2, candidates.index(raw) if raw in candidates else 0)
        scored.append((score,v))

    if scored:
        scored.sort(key=lambda x:x[0],reverse=True)
        return scored[0][1]

    # Common prose: "札幌市...の X が..." without quotes
    m=re.search(
        r'(?:市|区|町|村)の\s*([A-Za-z0-9一-龥ぁ-んァ-ヶー！!＆&・\-\s]{2,60}?)'
        r'\s*(?:が|は)\s*20\d{2}年',
        t
    )
    if m:
        v=clean_name(m.group(1))
        if is_good_hub_store_name(v):
            return v

    # Use already-clean fallback if it looks like an actual store name.
    if is_good_hub_store_name(fb):
        return fb

    # Plain article title may itself just be a store name.
    if is_good_hub_store_name(t):
        return t

    return None

def is_good_hub_store_name(name):
    if not name:
        return False
    n=norm(name)
    if n in HUB_GENERIC_NAMES:
        return False

    generic_fragments = [
        "開店・閉店ポータル",
        "開店閉店ポータル",
        "全国の開店・閉店情報",
        "エリアの開店・閉店情報",
        "エリアの開店閉店情報",
        "開店・閉店情報を掲載",
        "開店閉店情報を掲載",
    ]
    if any(x in n for x in generic_fragments):
        return False

    if re.fullmatch(r'.{0,20}(?:開店|閉店)(?:・|/)?(?:閉店|開店)?情報', n):
        return False
    if len(n) < 2 or len(n) > 80:
        return False
    if re.search(r'20\d{2}年\d{1,2}月\d{1,2}日',n):
        return False
    if any(x in n for x in [
        "をもって閉店","オープン！","オープン予定","閉店予定",
        "が閉店","がオープン","新店舗オープン","閉店情報 ",
        "開店情報 ","出店者募集"
    ]):
        return False
    if n.startswith(("札幌市","東京都","大阪市","京都府","北海道")) and ("の『" in n or "の「" in n):
        return False
    return True

def clean_hub_address(address, floor=None):
    if not address:
        return None
    a=address.replace("\r","\n")
    # Remove map/navigation labels accidentally captured from article layout.
    a=re.split(r'\n\s*(?:地図|MAP|Google\s*Map|アクセスマップ)\s*',a,maxsplit=1,flags=re.I)[0]
    a=re.sub(r'\s+',' ',a).strip()
    a=re.sub(r'\s+(?:地図|MAP)\s*$','',a,flags=re.I)

    # If the address regex captured only the numeric portion of an adjacent floor
    # (e.g. "...大丸札幌店 3" while floor=3F), remove that detached number.
    if floor:
        fm=re.match(r'(?:B)?(\d{1,2})(?:F|Ｆ|階)$',norm(floor),re.I)
        if fm:
            num=fm.group(1)
            a=re.sub(rf'\s+{re.escape(num)}$','',a)

    return a.strip(" ,、。")

def exact_event_date_from_text(text, status):
    """
    Exact YYYY-MM-DD only.
    Publication dates and month-only dates are never converted into event dates.
    """
    if not text:
        return None

    t=text.replace("\r","\n")

    if status=="opening":
        actions=r'(?:オープン|OPEN|開店|開業|営業開始)'
        labels=["開店日","オープン日","OPEN日","開業日","営業開始日"]
    else:
        actions=r'(?:閉店|営業終了|最終営業)'
        labels=["閉店日","営業終了日","最終営業日"]

    # Explicit labeled value on the SAME line.
    for line in [norm(x) for x in t.split("\n") if norm(x)]:
        if any(lbl in line for lbl in labels):
            m=re.search(r'(20\d{2})年\s*(\d{1,2})月\s*(\d{1,2})日',line)
            if m:
                try:
                    return date(int(m.group(1)),int(m.group(2)),int(m.group(3)))
                except Exception:
                    pass

    # Exact date and action close to each other.
    patterns=[
        rf'(20\d{{2}})年\s*(\d{{1,2}})月\s*(\d{{1,2}})日.{{0,35}}{actions}',
        rf'{actions}.{{0,35}}(20\d{{2}})年\s*(\d{{1,2}})月\s*(\d{{1,2}})日',
    ]
    for p in patterns:
        m=re.search(p,t,re.I)
        if m:
            try:
                return date(int(m.group(1)),int(m.group(2)),int(m.group(3)))
            except Exception:
                pass

    return None


