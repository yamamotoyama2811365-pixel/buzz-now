from pathlib import Path
p=Path(__file__).resolve().parents[1]/'services/corporate/model.py'
text=p.read_text()
old="        for key in ('news_reports','news_checked_at','news_check_status','news_version','discovery_sources'):\n"
new="        for key in ('news_reports','news_checked_at','news_check_status','news_version','discovery_sources','event_date','event_date_label'):\n"
if new not in text:
    if old not in text:raise SystemExit('preserve-profile marker missing')
    text=text.replace(old,new,1)
p.write_text(text)
print('live event-date preservation enabled')
