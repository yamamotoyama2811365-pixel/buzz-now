"""Owner's per-account editorial policies; rankings never create allegations."""
import re
import unicodedata
from datetime import date, timedelta
from urllib.parse import urlsplit

VERSION = 'incident-first-20260918'
POLICIES = {
    'jleague': {
        'revision': 'jleague-news-and-guides-20260918',
        'focus': '用意済みの観戦・サイト紹介を継続し、移籍・加入・退団など新しいクラブ情報があれば優先',
        'topics': ['移籍', '加入', '退団', '契約更新', '試合情報', '観戦ガイド', 'クラブ情報', '重大な不祥事・処分'],
        'source_rule': 'クラブ・リーグの公式発表または信頼できる報道で対象と時点を照合',
        'copy_rule': '新しい情報を冒頭に置き、移籍の公式発表と報道段階を区別。不祥事は疑い・調査中・処分・確定を区別',
        'ordinary_cards_are_scandal': False,
        'routine_promotion': 'scheduled_when_no_verified_news',
    },
    'corporate': {
        'focus': '行政処分・不正報道・経営問題を優先',
        'topics': ['行政処分', '粉飾・不正会計', '詐欺・横領報道', '偽装', '重大な経営問題'],
        'source_rule': '公的機関の発表または会社名・所在地等を照合済みの掲載報道',
        'copy_rule': '発表・報道の段階を維持し、倒産を不正の証拠として扱わない',
        'fallback': '既存の倒産・閉店情報。根拠のないスキャンダル表現を加えない',
    },
}
OFFICIAL_HOSTS = {'www.mlit.go.jp', 'mlit.go.jp', 'jsite.mhlw.go.jp',
                  'www.caa.go.jp', 'caa.go.jp', 'www.fsa.go.jp', 'fsa.go.jp'}
MATCH_BASES = {'速報の出典記事', '会社名・法人番号一致', '会社名・所在地一致',
               '会社名・裁判所・事件番号一致'}
INCIDENT_TERMS = ('逮捕', '送検', '起訴', '粉飾', '不正会計', '横領', '詐欺',
                  '偽装', '行政処分', '業務停止命令', '措置命令', '課徴金',
                  'ハラスメント', '不祥事', '不正受給')


def company_key(value):
    value = unicodedata.normalize('NFKC', str(value or ''))
    return re.sub(r'株式会社|有限会社|合同会社|\(株\)|\(有\)|[\s・]', '', value).casefold()


def recent(value, today):
    try:
        published = date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return False
    return today - timedelta(days=2) <= published <= today


def public_source(url):
    try:
        parsed = urlsplit(str(url or ''))
        return parsed.scheme == 'https' and bool(parsed.hostname) and not parsed.username and not parsed.password
    except ValueError:
        return False


def corporate_evidence(row, today):
    """Return existing, attributed text only; never synthesize scandal labels."""
    p = row.get('payload') or {}
    if not recent(row.get('reported_date'), today):
        return None
    company = str(row.get('company') or p.get('company') or '')
    if len(company_key(company)) < 2:
        return None
    if row.get('kind', p.get('kind')) == 'risk':
        source = str(p.get('source_url') or '')
        stage = str(p.get('stage') or '')
        if (public_source(source) and urlsplit(source).hostname in OFFICIAL_HOSTS
                and any(t in stage for t in INCIDENT_TERMS)):
            return {'label': '行政発表', 'statement': stage,
                    'source_name': p.get('risk_agency') or p.get('source_name') or '公的機関',
                    'source_url': source, 'reported_date': str(row['reported_date'])[:10]}
    if row.get('kind', p.get('kind', 'bankruptcy')) != 'bankruptcy':
        return None
    for report in p.get('news_reports') or []:
        title = str(report.get('title') or '').strip()
        source = str(report.get('url') or '')
        if (report.get('match_basis') not in MATCH_BASES
                or not recent(report.get('published_date'), today)
                or not public_source(source)
                or report.get('publisher') != urlsplit(source).hostname
                or company_key(company) not in company_key(title)
                or not any(t in title for t in INCIDENT_TERMS)):
            continue
        # Preserve the WHOLE headline, including allegations, denials and outcomes.
        # Long headlines are skipped, never cut before a qualifier or a denial.
        if len(title) <= 65 and not re.search(r'[\r\n]|https?://', title):
            return {'label': '企業報道', 'statement': title,
                    'source_name': report['publisher'], 'source_url': source,
                    'reported_date': report['published_date']}
    return None


def select_corporate(rows, seen, today):
    for row in sorted(rows, key=lambda r: (str(r.get('reported_date') or ''), str(r['id'])), reverse=True):
        kind = row.get('kind') or (row.get('payload') or {}).get('kind') or 'bankruptcy'
        key = kind + ':' + str(row['id'])
        if key in seen:
            continue
        evidence = corporate_evidence(row, today)
        if evidence:
            return key, dict(row, editorial_evidence=evidence)
    return None


def corporate_text(row):
    evidence = row['editorial_evidence']
    company = str(row.get('company') or '')
    lines = ['【' + evidence['label'] + '】', evidence['statement']]
    if company_key(company) not in company_key(evidence['statement']):
        lines.insert(1, company)
    lines += ['出典：' + str(evidence['source_name']),
              '公表・報道日：' + str(evidence['reported_date']).replace('-', '/'),
              '詳細・出典 → https://buzz-now-1.onrender.com/corporate/company/' + str(row['id'])]
    return '\n'.join(lines)
