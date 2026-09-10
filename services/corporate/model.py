"""Facts only. Unknown classifications remain unknown; closure is not bankruptcy."""
import hashlib
import re
import unicodedata
from datetime import date, timedelta
from urllib.parse import urlparse

PREFECTURES='北海道 青森県 岩手県 宮城県 秋田県 山形県 福島県 茨城県 栃木県 群馬県 埼玉県 千葉県 東京都 神奈川県 新潟県 富山県 石川県 福井県 山梨県 長野県 岐阜県 静岡県 愛知県 三重県 滋賀県 京都府 大阪府 兵庫県 奈良県 和歌山県 鳥取県 島根県 岡山県 広島県 山口県 徳島県 香川県 愛媛県 高知県 福岡県 佐賀県 長崎県 熊本県 大分県 宮崎県 鹿児島県 沖縄県'.split()
INDUSTRIES={'建設業':['建設工事','建設業','土木工事','建築工事'], '製造業':['製造業','製造販売','製造を'], '卸売業':['卸売','卸業'], '小売業':['小売','販売店','スーパーを'], '飲食業':['飲食店','飲食業','レストラン','居酒屋'], '宿泊業':['ホテル経営','旅館','宿泊業'], '運輸業':['運送業','運送事業','物流アウトソーシング','貨物運送'], '不動産業':['不動産業','不動産賃貸','不動産売買'], '情報通信業':['ソフトウェア開発','システム開発','情報通信業'], 'サービス業':['美容室','ライブハウス','サービス業'], '農林水産業':['養豚','畜産','農業','酪農']}
CAUSES={'原材料高':['原材料高','原材料価格の高騰','仕入価格の高騰'], '人手不足':['人手不足','人材不足'], '売上減少':['売上減少','売り上げ減少','売上高の減少'], '資金繰り':['資金繰りが悪化','資金繰りの悪化'], '借入負担':['借入負担','返済負担'], '固定費負担':['固定費が重荷','固定費負担'], 'コロナ影響':['コロナ禍']}

def normalized_name(name):
    s=unicodedata.normalize('NFKC',name)
    s=re.sub(r'株式会社|有限会社|合同会社|\(株\)|\(有\)|[\s・]','',s)
    return s.casefold()

def identity(row):
    if row['kind']=='registration': return 'n'+row['corporate_number']
    # Without an exact corporate number, use source URL: do not silently merge namesakes.
    return 'b'+hashlib.sha256(row['source_url'].encode()).hexdigest()[:24]

def validate(row):
    if row.get('kind') not in {'bankruptcy','registration'}: raise ValueError('invalid kind')
    for k in ['company','stage','reported_date','source_url','source_name']:
        if not isinstance(row.get(k),str) or not row[k] or len(row[k])>1000: raise ValueError(k)
    date.fromisoformat(row['reported_date'])
    host=urlparse(row['source_url']).hostname
    if host not in {'n-seikei.jp','www.n-seikei.jp','www.houjin-bangou.nta.go.jp'}: raise ValueError('source host')
    if row['kind']=='registration' and not re.fullmatch(r'\d{13}',row.get('corporate_number','')): raise ValueError('corporate number')
    if row.get('prefecture','') not in PREFECTURES+['']: raise ValueError('prefecture')
    if row.get('industry','') not in list(INDUSTRIES)+['']: raise ValueError('industry')
    if len(str(row))>8000: raise ValueError('oversize row')
    return row

def stage_from_title(title):
    if re.search('破産.*(?:申請準備|申請へ|破産へ)|破産へ|自己破産申請へ',title): return '破産申請準備（報道）'
    if '破産' in title: return '破産関連（報道）'
    if '民事再生' in title: return '民事再生（報道）'
    if '特別清算' in title: return '特別清算（報道）'
    return ''

def parse_news(title, body, url, published):
    stage=stage_from_title(title)
    if not stage or re.search('一覧|件数|過去最多|倒産件数|前年|予測|リスク|ランキング',title): return None
    t=re.sub(r'【[^】]+】|〖[^〗]+〗|追報[：:]?|続報[：:]?','',title).strip()
    m=re.search(r'^(.{2,100}?)(?:が|、|／|\s[/／]\s)(?:.*?)(?:破産|民事再生|特別清算)',t)
    if not m: return None
    company=m.group(1).strip()
    company=re.sub(r'「[^」]+」','',company).strip()
    if 'の' in company: company=company.rsplit('の',1)[-1]
    company=re.sub(r'（旧[^）]*）','',company).strip(' ・')
    if len(company)<2 or len(company)>60 or re.search('企業|会社数|件|\d+%|倒産',company): return None
    pref=''
    for p in PREFECTURES:
        short=p if p=='北海道' else p[:-1]
        if re.search(r'[【〖]'+re.escape(short)+r'(?:[・】〗])',title) or p in body[:300]: pref=p; break
    # Restrict classification to the lead; avoid unrelated recommendations/footer text.
    lead=body[:600]
    industries=[k for k,words in INDUSTRIES.items() if any(w in lead for w in words)]
    title_industries=[k for k,words in INDUSTRIES.items() if any(w in title for w in words)]
    industry=title_industries[0] if len(title_industries)==1 else industries[0] if len(industries)==1 else ''
    causes=[k for k,words in CAUSES.items() if any(w in body for w in words)]
    return validate(dict(kind='bankruptcy',company=company,corporate_number='',prefecture=pref,industry=industry,stage=stage,reported_date=published,source_name='JC-NET',source_url=url,causes=causes,address='',classification_basis='記事の記載から自動分類' if industry else '',date_label='報道日'))

def parse_registration(node):
    get=lambda tag:(node.findtext(tag) or '').strip()
    if get('correct')=='1' or get('process')!='01' or get('hihyoji')=='1' or get('latest')=='0' or get('closeDate'): return None
    # Registered companies only; exclude personal businesses and non-company entities.
    if get('kind') not in {'301','302','303','304','305'}: return None
    assigned=date.fromisoformat(get('assignmentDate'))
    if assigned<date.today()-timedelta(days=14) or assigned>date.today():return None
    number=get('corporateNumber')
    return validate(dict(kind='registration',company=get('name'),corporate_number=number,prefecture=get('prefectureName'),industry='',stage='法人番号の新規指定',reported_date=get('assignmentDate'),source_name='国税庁 法人番号公表サイト',source_url='https://www.houjin-bangou.nta.go.jp/henkorireki-johoto.html?selHouzinNo='+number,causes=[],address=get('prefectureName')+get('cityName')+get('streetNumber'),date_label='法人番号指定日',classification_basis=''))
