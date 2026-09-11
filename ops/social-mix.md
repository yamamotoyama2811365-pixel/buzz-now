# Investigator posts: X and Threads separately

Each channel rotates four ordinary posts and one investigator daily-life caption, giving two character posts in every completed ten-post cycle. X uses successful entries in social_schedule_log (including quote slots); Threads uses buffer_status=1, with trend_id=-1 identifying character content. Existing accepted counts are preserved. Skipped slots and definite failures never advance the sequence. The first transition may begin part-way through an existing cycle; it does not retroactively repair prior missing character posts.

Counters mean Buffer accepted, NOT verified publication on X/Threads. Unknown X reservations or new ambiguous Threads attempts pause that channel; reconcile before retrying. A Buffer 429 is not a successful post. No manual catch-up burst is created.

X keeps the existing JST windows, rolling total cap, cooldown and shared locking, but determines content from its own accepted counter, not the old time-of-day label. Threads keeps its own cap, cooldown, primary service check and advisory lock. News and character posts share each platform's limits. Quote automation is limited to one post per turn and cannot occupy a character turn.

Use the existing approved noon/night images and 28 short captions, always disclosing that the investigator is an AI character. The existing 14-day trial remains: X keeps its trial start, Threads starts its own trial on the first accepted character post. After the trial, ordinary posting resumes; review before extending. No profile, credentials, paid plan, analytics or unrelated service settings are changed.

/api/social/schedule includes mix_policy version 2, per-platform accepted counts, next_kind and uncertainty flags. Reading it never posts. Tests use fake senders and disposable local SQLite only. The production check is GET-only. Automatic posting happens only through the existing application's scheduled jobs, not a ChatGPT background task.
