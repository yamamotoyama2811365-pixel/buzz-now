# BUZZ NOW Neon production cutover — 2026-09-10

Completed at 06:23 UTC / 15:23 JST.

## Verified results
- Latest source export and all 29 public tables restored and compared by row counts plus order-independent full-row checksums.
- Final copy workflow: https://github.com/yamamotoyama2811365-pixel/buzz-now/actions/runs/34444573749
- Representative final-copy counts: trends 3,396; traffic_history 1,136,427; predictions 76,992; prediction_results 74,225; social_images 60; social_image_derivatives 44.
- Neon database size before production resumes: 348,405,760 bytes.
- Render service buzz-now-1 switched to DATABASE_BACKEND=neon and MIGRATION_MAINTENANCE=0; original DATABASE_URL retained.
- Render deployment dep-dah4p6dbedkc7394o310 live at 06:23:32 UTC, application commit e74d2727c9717970dcd9f34f580334a4bdd5e679.
- Public read-only checks passed at 06:23:33 UTC: /health reports production, backend neon, maintenance false; /ready reports database ok, REAL mode true, demo false; homepage returns HTTP 200 with HTML.
- Read-only verification workflow: https://github.com/yamamotoyama2811365-pixel/buzz-now/actions/runs/34444891868

## Data preservation
- Source Render database remains intact and default_transaction_read_only=on, confirmed through pg_db_role_setting.
- Retained Neon snapshot snap-holy-frost-b3mv1s2j is the earlier verified rehearsal snapshot, not a new snapshot of the final copy.
- Source expires on 2026-09-30. It is temporary rollback/reference protection, not indefinite backup.
- Temporary Actions dump removed at exit; no persistent dump backup claimed.
- New production writes now belong on Neon. Do not simply switch back to the old source after new writes; first reconcile data and pause writers.
- Destructive migration steps removed from the current ops workflow and replaced with public read-only verification. Historical workflow reruns must not be used.

## Migration issue resolved
The first replacement attempt exceeded the free project quota because an atomic replacement retained old and new table storage simultaneously. With explicit user approval, the rehearsal public schema was cleared in a separately committed operation after obtaining the final source dump and manifest. Reimport and comparison then succeeded. Source was not erased.

## Remaining work
- Production history pruning and image-storage changes are not implemented. See storage-retention-review-20260910.md for measured growth and the proposed policy.
- Render paid instance upgrade and API consolidation are not completed by this cutover.
- The corporate bankruptcy/new-company site remains a separate pending project.
