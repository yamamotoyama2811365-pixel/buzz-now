# Media X accounts

Latest owner direction (2026-09-18): operate BUZZ NOW, the city/corporate account, and J.League. J.League and city/corporate X prioritize scandals, misconduct, sanctions and major management problems. See `ops/x-editorial-policy.md` for sourcing and wording rules.

| Site | Buffer account | X handle | Server configuration |
| --- | --- | --- | --- |
| Beauty | Existing | 7d2sz_biyo | BIYO_X_BUFFER_CHANNEL_ID / BIYO_X_ENABLED; not active |
| Tadage | New | Unconfirmed | TADAGE_X_BUFFER_CHANNEL_ID / TADAGE_X_ENABLED; not active |
| Otona Koi | New | Unconfirmed | OTONA_X_BUFFER_CHANNEL_ID / OTONA_X_ENABLED; not active |
| J.League | New | issho_j | JLEAGUE_X_BUFFER_CHANNEL_ID / JLEAGUE_X_ENABLED; server setup incomplete |

The existing account uses BUFFER_API_KEY. The second account must use its own BUFFER_MEDIA_API_KEY, only in deployment secrets. No fallback to the existing key is permitted. Never put credentials in the repository or chat.

J.League was connected successfully to the new Buffer account. Channel `6aacf8e7ea19ca0bde760b9f` and the first public post were verified through Buffer Sent on 2026-09-18. Seven normal site-promotion posts for September 19–25 were created, then moved to Drafts following the owner's incident-first direction. They must not be requeued automatically. The everyday 12:15 Asia/Tokyo slot remains a scheduling preference, not proof that posts are queued.

The new Buffer API key and J.League server channel settings have not been configured. `app/media_x_routes.py` is a verified one-post send adapter, not a collector or scheduler. `/api/media-x/status` exposes actual credential/channel readiness separately from editorial policy. A recorded policy does not mean unattended publication is active.

The adapter rejects reserved channels (BUZZ NOW X, city/corporate X and old Threads), duplicate channel assignments, wrong handles, disconnected channels and missing settings. The caller must durably reserve each post before sending; uncertain responses must not be retried blindly. Buffer acceptance and actual X publication are distinct.

Threads publishing remains disabled in `app/main.py`. Its channel was observed removed on 2026-09-18 and its old ID must not be reused for beauty. Beauty's X account creation is confirmed; it is not connected. Tadage and Otona account creation is unconfirmed. All four server enable flags default to false.
