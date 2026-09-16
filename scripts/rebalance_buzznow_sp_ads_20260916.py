from pathlib import Path

root=Path(__file__).resolve().parents[1]

# HOME
p=root/'templates/index.html'
text=p.read_text()
# Remove Amazon Associate placements from the rendered page.
text=text.replace('\n\n    {% include "_amazon_associate.html" %}\n','\n')
# Move the existing SP rectangle from near the top to a true mid-page position.
rect='    <!-- SP 325x250 inline; separate spot for mid-content performance. -->\n    {% include "_imobile_sp_buzznow_rectangle.html" %}\n\n'
text=text.replace(rect,'')
ranking_end='''      <div id="trendGrid" class="ranking-list">\n        <div class="loading-card">ランキング読み込み中...</div>\n      </div>\n    </section>\n'''
if rect.strip() not in text:
    if ranking_end not in text:
        raise SystemExit('home ranking marker missing')
    text=text.replace(ranking_end, ranking_end+'\n'+rect, 1)
# Reserve non-rendering anchors for two additional distinct i-mobile spots.
# These do not show blank ad boxes; once separate tags are supplied they can be filled safely.
if 'SP_INLINE_SLOT_3_PENDING' not in text:
    velocity_end='''      <p class="source-note">Velocityは、BUZZ NOWが取得したSource Scoreの変化量を1時間換算した値です。実データが蓄積するほど精度が上がります。</p>\n    </section>\n'''
    if velocity_end not in text:
        raise SystemExit('home velocity marker missing')
    text=text.replace(velocity_end, velocity_end+'\n    <!-- SP_INLINE_SLOT_3_PENDING: separate approved 325x250 tag required -->\n',1)
if 'SP_INLINE_SLOT_4_PENDING' not in text:
    learning_end='''      <div id="learningRecent" class="learning-list">まだ答え合わせ前です。</div>\n    </section>\n'''
    if learning_end not in text:
        raise SystemExit('home learning marker missing')
    text=text.replace(learning_end, learning_end+'\n    <!-- SP_INLINE_SLOT_4_PENDING: separate approved 325x250 tag required -->\n',1)
p.write_text(text)

# TREND DETAIL
p=root/'templates/trend.html'
text=p.read_text()
text=text.replace('\n  {% include "_amazon_associate.html" %}\n','\n')
# Move the existing rectangle from directly under heading to a more natural reading break.
rect_t='    <!-- SP 325x250 inline after the article heading/summary. -->\n    {% include "_imobile_sp_buzznow_rectangle.html" %}\n'
text=text.replace(rect_t,'')
marker='''    </section>\n\n    <section class="section ad-zone">'''
if rect_t.strip() not in text and marker in text:
    text=text.replace(marker, '    </section>\n\n'+rect_t+'\n    <section class="section ad-zone">', 1)
if 'SP_INLINE_SLOT_3_PENDING' not in text:
    marker2='''    <section class="section">\n      <div class="kicker">TRAFFIC POTENTIAL</div>'''
    if marker2 in text:
        text=text.replace(marker2,'    <!-- SP_INLINE_SLOT_3_PENDING: separate approved 325x250 tag required -->\n\n'+marker2,1)
if 'SP_INLINE_SLOT_4_PENDING' not in text:
    marker3='''    <aside class="section a8-advertisement" aria-label="広告">'''
    if marker3 in text:
        text=text.replace(marker3,'    <!-- SP_INLINE_SLOT_4_PENDING: separate approved 325x250 tag required -->\n\n'+marker3,1)
p.write_text(text)

# trigger after workflow creation
