import unittest
import ast
from pathlib import Path
from html import escape
from urllib.parse import quote, urlencode
from datetime import date
from services.corporate.regional_editorial import summarize, render, banner

END = date(2026, 9, 13)

def row(id='a', kind='bankruptcy', day='2026-09-12', **patch):
    p = dict(kind=kind, company='事例株式会社', prefecture='北海道', reported_date=day,
             source_url='https://example.com/source', source_name='出典', stage='申請準備', entity_key=id)
    p.update(patch)
    return dict(id=id, payload=p, first_seen='2026-09-12')

class RegionalTests(unittest.TestCase):
    def test_period_and_area(self):
        rows=[row('start',day='2026-08-15'),row('end',day='2026-09-13'),row('old',day='2026-08-14'),row('future',day='2026-09-14'),row('elsewhere',prefecture='東京都')]
        self.assertEqual({r['id'] for r in summarize(rows,'北海道',END)['bankruptcy']},{'start','end'})

    def test_latest_report_not_double_counted(self):
        rows=[row('a',entity_key='same'),row('b',entity_key='same',day='2026-09-13',stage='破産開始')]
        self.assertEqual(summarize(rows,'北海道',END)['bankruptcy'][0]['id'],'b')
        self.assertEqual(len(summarize(rows,'北海道',END)['bankruptcy']),1)

    def test_registry_number_dedup(self):
        rows=[row('a',kind='registration',corporate_number='1234567890123'),row('b',kind='registration',corporate_number='1234567890123')]
        self.assertEqual(len(summarize(rows,'北海道',END)['registration']),1)

    def test_invalid_source_date_and_escaping(self):
        rows=[row('bad',source_url='javascript:alert(1)'),row('date',day='invalid'),row(company='<script>bad</script>')]
        html=render('北海道',rows,END)
        self.assertNotIn('javascript:',html)
        self.assertNotIn('<script>bad',html)
        self.assertIn('&lt;script&gt;',html)

    def test_no_records_not_regional_zero(self):
        html=render('北海道',[],END)
        self.assertIn('地域全体で発生・設立がなかったという意味ではありません',html)
        self.assertIn('設立年月日とは別',html)
        self.assertIn('集計上限',render('北海道',[],END,truncated=True))

    def test_verified_stat_only_matching_pref(self):
        self.assertIn('前年8月83件に対し2026年8月110件',render('大阪府',[],END))
        self.assertNotIn('前年8月83件',render('北海道',[],END))

    def test_banner_links_to_existing_page_sections(self):
        html=render('北海道',[],END)
        for anchor in ['bankruptcy-feature','registration-feature']:
            self.assertIn('#'+anchor,banner('北海道'))
            self.assertIn('id="'+anchor+'"',html)

    def test_listing_preserves_canonical_and_scopes_feature(self):
        # 実際のlisting関数を実行。DBと外部タグだけ境界で置換する。
        tree=ast.parse(Path('services/corporate/main.py').read_text())
        fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='listing')
        ns=dict(e=escape,quote=quote,urlencode=urlencode,ROOT='/corporate',
                records=lambda *a:dict(total=1,items=[row()]),
                breadcrumbs=lambda *a:'',kind_tabs=lambda *a:'tabs',filters=lambda *a:'filters',
                cards=lambda *a:'cards',side=lambda:'side',
                regional_banner=banner,regional_feature=lambda p:'REGIONAL_ARTICLE',
                shell=lambda title,body,path,**kw:dict(title=title,body=body,path=path,**kw))
        exec(compile(ast.Module(body=[fn],type_ignores=[]),'<listing>','exec'),ns)
        path='/area/'+quote('北海道')
        actual=ns['listing']('北海道',prefecture='北海道',path=path)
        self.assertEqual(actual['path'],path)
        self.assertIn('REGIONAL_ARTICLE',actual['body'])
        self.assertFalse(actual['noindex'])
        filtered=ns['listing']('北海道',kind='registration',prefecture='北海道',path=path)
        self.assertNotIn('REGIONAL_ARTICLE',filtered['body'])
        self.assertIn('?kind=registration',filtered['path'])
        page2=ns['listing']('北海道',prefecture='北海道',path=path,page=2)
        self.assertNotIn('REGIONAL_ARTICLE',page2['body'])
        self.assertIn('?page=2',page2['path'])
        other=ns['listing']('倒産速報',kind='bankruptcy',path='/bankruptcies')
        self.assertNotIn('REGIONAL_ARTICLE',other['body'])

if __name__=='__main__':
    unittest.main()
