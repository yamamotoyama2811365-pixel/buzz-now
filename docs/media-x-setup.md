# Four media X accounts

Approved on 2026-09-17: replace the existing Buffer Threads channel with beauty X; use a second Buffer account for J.League, Tadage and Otona Koi.

| Site | Buffer account | X handle | Channel variable | Enable variable |
| --- | --- | --- | --- | --- |
| Beauty | Existing | 7d2sz_biyo | BIYO_X_BUFFER_CHANNEL_ID | BIYO_X_ENABLED |
| Tadage | New | Unconfirmed: TADAGE_X_HANDLE | TADAGE_X_BUFFER_CHANNEL_ID | TADAGE_X_ENABLED |
| Otona Koi | New | Unconfirmed: OTONA_X_HANDLE | OTONA_X_BUFFER_CHANNEL_ID | OTONA_X_ENABLED |
| J.League | New | issho_j | JLEAGUE_X_BUFFER_CHANNEL_ID | JLEAGUE_X_ENABLED |

The existing account uses BUFFER_API_KEY. Set the second account's credential only in the deployment environment as BUFFER_MEDIA_API_KEY. Never put keys in this repository or chat. The new account has no fallback to the existing key.

Threads publishing is disabled in app/main.py. The Threads channel was observed removed from the existing Buffer account on 2026-09-18. Its old channel ID must never be reused for beauty: authorize the beauty X account in Buffer and obtain its newly assigned channel ID.

app/media_x_routes.py provides an explicit, verified send adapter. It rejects reserved channels (BUZZ NOW X, city/corporate X, old Threads), duplicate channel assignments, wrong handles, disconnected channels and missing settings. The safe read-only endpoint is /api/media-x/status. A configured credential is not proof of successful X authorization.

All four enable variables default to false. Beauty creation is confirmed by screenshot, and the owner supplied J.League's handle @issho_j on 2026-09-18. Tadage and Otona Koi account registration is not confirmed. X authorization and channel configuration for all four accounts remain pending. This change does not start a scheduler or publish any new posts. The caller must durably claim/deduplicate each post before send_verified; an uncertain result must never be blindly retried. Buffer acceptance is recorded separately from verified X publication.

Before activation, confirm account ownership, identity, destination URL and posting content; set the relevant enable variable only after these are ready. Beauty account creation is confirmed by the owner's screenshot. J.League's @issho_j was supplied by the owner; the other two X handles are not confirmed yet. J.League's final public URL must be checked before publishing links.
