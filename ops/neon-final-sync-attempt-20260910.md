# Final synchronization attempt — 2026-09-10

Run: https://github.com/yamamotoyama2811365-pixel/buzz-now/actions/runs/34443185936

The source was paused and quiesced, its default set to read-only, and a consistent snapshot exported. Atomic replacement of the already populated rehearsal target hit Neon's 512 MB project size limit while copying social_images. The existing target and new table storage overlap until commit; a ~347 MB final database does not guarantee that an atomic replacement fits the free cap.

pg_restore used a single transaction and rolled back. Subsequent target reads showed 3394 trends and 60 images, database size 347480064 bytes. The retained earlier snapshot is snap-holy-frost-b3mv1s2j.

The failure handler reset the source's default_transaction_read_only. A subsequent read of pg_db_role_setting showed only TimeZone=utc for the database. Render was set back to DATABASE_BACKEND=source and MIGRATION_MAINTENANCE=0. The original DATABASE_URL was never replaced. The switch to Neon did not occur.

Do not rerun this atomic replacement on the free project unchanged. Options are an empty destination or more quota. Emptying the rehearsal target before re-import is a destructive step and needs specific confirmation under the Neon operation rules, even though it is not serving the production app and a snapshot/source copy are retained. Paid-plan changes also need a concrete cost/scope assessment.
