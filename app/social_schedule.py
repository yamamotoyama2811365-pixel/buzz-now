"""JST posting slots shared by normal and quote posts; no catch-up bursts."""
import json
from datetime import datetime, timedelta, timezone

JST = timezone(timedelta(hours=9))
SLOTS = ((7,0,'trend'), (9,0,'trend'), (11,0,'trend'), (12,30,'character'),
         (14,0,'trend'), (16,0,'trend'), (18,0,'trend'), (19,30,'trend'),
         (21,0,'character'), (22,30,'trend'))
WINDOW_MINUTES = 30


def current_slot(now, kind):
    local = now.astimezone(JST)
    for hour, minute, role in SLOTS:
        start = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if (kind == 'mixed' or role == kind) and start <= local < start + timedelta(minutes=WINDOW_MINUTES):
            return {'key': start.strftime('%Y-%m-%dT%H:%M'), 'kind': kind,
                    'start': start, 'end': start + timedelta(minutes=WINDOW_MINUTES)}
    return None


def init(c):
    c.execute('''CREATE TABLE IF NOT EXISTS social_schedule_log (
        slot_key TEXT PRIMARY KEY, kind TEXT NOT NULL, state TEXT NOT NULL,
        attempted_at TEXT NOT NULL, post_id TEXT, reason TEXT)''')


def reserve(c, kind, now, daily_cap=10, cooldown_minutes=60, mixed=False):
    slot = current_slot(now, 'mixed' if mixed else kind)
    if slot and mixed:
        slot['kind'] = kind
    if not slot:
        return None, 'outside_posting_window'
    init(c)
    if hasattr(c, '_con'):
        c.execute('SELECT pg_advisory_xact_lock(3544101)')
    if c.execute('SELECT slot_key FROM social_schedule_log WHERE slot_key=?', (slot['key'],)).fetchone():
        return None, 'slot_already_attempted'
    if mixed:
        from . import social_mix
        if social_mix.pending(c, 'x'):
            return None, 'uncertain_submission_requires_review'
        if kind != social_mix.next_kind(c, 'x', now):
            return None, 'content_kind_not_due'
    cutoff = (now - timedelta(hours=24)).isoformat()
    # Include historical posts predating this rollout and uncertain submissions.
    normal = c.execute('SELECT COUNT(*) AS n FROM social_posts WHERE make_status=1 AND posted_at>=?', (cutoff,)).fetchone()['n']
    quotes = c.execute("SELECT COUNT(*) AS n FROM buzzing_quote_posts WHERE status='sent' AND sent_at>=?", (cutoff,)).fetchone()['n']
    extra = c.execute("SELECT COUNT(*) AS n FROM social_schedule_log WHERE attempted_at>=? AND ((kind='character' AND state='sent') OR state='reserved')", (cutoff,)).fetchone()['n']
    if int(normal or 0) + int(quotes or 0) + int(extra or 0) >= max(0, daily_cap):
        return None, 'combined_daily_cap'
    recent = (now - timedelta(minutes=max(60, cooldown_minutes))).isoformat()
    last = c.execute('''SELECT posted_at AS ts FROM social_posts WHERE make_status=1 AND posted_at>?
        UNION ALL SELECT sent_at AS ts FROM buzzing_quote_posts WHERE status='sent' AND sent_at>?
        UNION ALL SELECT attempted_at AS ts FROM social_schedule_log WHERE state IN ('sent','reserved') AND attempted_at>?
        LIMIT 1''', (recent, recent, recent)).fetchone()
    if last:
        return None, 'combined_cooldown'
    c.execute("INSERT INTO social_schedule_log(slot_key,kind,state,attempted_at) VALUES(?,?,'reserved',?)", (slot['key'],kind,now.isoformat()))
    # Persist before contacting Buffer. An ambiguous outcome is never retried.
    c.commit()
    return slot, 'ok'


def finish(c, slot, sent, post_id='', reason=''):
    c.execute('UPDATE social_schedule_log SET state=?,post_id=?,reason=? WHERE slot_key=?',
              ('sent' if sent else 'skipped', str(post_id or ''), str(reason or '')[:160], slot['key']))
    c.commit()


DAY_PROMPTS = ('見出しだけで全部わかった顔するの、捜査早すぎ。\nまず本文。コーヒーはそのあとでも逃げないって。☕', '「みんな言ってる」の、みんなって誰なん？\nそこから確認するのが、うちの仕事。', 'バズってから「前から知ってた」って言う選手権、強豪多すぎ。\n最近、先に見つけたものある？', '数字が伸びた＝中身も正しい、ではないんよ。\nそこ混ぜると捜査やり直し。', '有名だから見る、も分かるけど。\nまだ知られてない推しを見つけた時の方が、ちょっと勝った気しない？', '次の捜査先、どこ行く？\n音楽・映画・スポーツ。雑な「全部」は今日はなしで。👀', '速報追いすぎて、お昼忘れるのは普通に負け。\nごはん食べた？ うちはコーヒー休憩。', 'スクショ1枚で判決出すの、さすがにせっかち。\nその前後、確認した？', '「絶対バズる」って言い切るの簡単すぎ。\n外れた時も答え合わせできる方が、うちは好き。', 'フォロワー数で全部決めるの、ちょい雑じゃない？\n最近見つけた面白い人、こっそり教えて。', '保存して満足、そのまま放置。\n情報収集した気になってるの、うちだけじゃないよね？', '話題に乗るのは自由。でも元ネタ知らずに断定は危なっかしいって。\n一回、出典見よ。', '長い説明は飛ばすのに、揉めてるコメントは最後まで読む。\n人間の集中力、配分クセ強い。', '捜査官へのタレコミ募集。\n次に来ると思うもの、一つ教えて。根拠あると、うち喜ぶ。')
NIGHT_PROMPTS = ('夜の捜査室、開けとく。\n「それ本当？」って一回止まれる人、わりと好き。\n気になったら、また見に来て。', '今日いちばん時間溶かした話題、何？\n「気付いたら2時間」は証言としてよく聞く。', 'タイムラインがうるさい夜ほど、出典は静かに読む。\n勢いだけで乗ると、降りるの大変なんよ。', '推し語りになると急に語彙なくなるの、なんで？\nその「やばい」の中身、うちには聞かせて。', '寝る前に一個だけ見る、の一個で終わったことある？\nうちはその約束、あんま信用してない。', '「知らなかった」って言える方が、知ったかぶるより強くない？\n今日は何を初めて知った？', '捜査メモ、どれ増やす？\n話題の解説・予測の答え合わせ・うちの日常。\n忖度はいらんからね。', 'みんなが好き、とうちが好き、は別。\n流行に好みまで提出しなくていいんよ。', '断定した投稿より、訂正できる人を見てたい。\n間違えないフリ、コスパ悪すぎ。', '今日の気分、絵文字一個で供述して。\n長文の言い訳は明日聞く。👀', 'その「炎上」、誰が何に怒ってるのか説明できる？\n名前だけ覚えて帰るの、捜査としては惜しい。', '見たい情報だけ見てると、世の中ぜんぶ同意見に見える。\nたまには別の窓も開けよ。', '急に流行ったように見えて、前から積み上げてた人もいる。\nその「前から」の方も、うちは気になる。', '2週間、うちの捜査メモに付き合ってくれてありがと。\n続けてほしいやつ、微妙だったやつ、正直に聞かせて。')

def character_text(day, slot):
    prompt = DAY_PROMPTS[day] if slot['start'].hour == 12 else NIGHT_PROMPTS[day]
    return '🕵️ SNS捜査官｜BUZZ NOW公式AIキャラクター\n\n' + prompt + '\n\n#SNS捜査官 #AIキャラクター'


def trial_day(c, now):
    row = c.execute("SELECT value FROM system_state WHERE key='detective_trial_start'").fetchone()
    if not row:
        return 0
    return (now.astimezone(JST).date() - datetime.fromisoformat(row['value']).date()).days


def status():
    return {'timezone':'Asia/Tokyo', 'window_minutes':WINDOW_MINUTES,
            'slots':[{'time':f'{h:02d}:{m:02d}','kind':k} for h,m,k in SLOTS],
            'trend_slots':8,'character_slots':2,'character_trial_days':14,
            'missed_slots':'skip; no catch-up'}
