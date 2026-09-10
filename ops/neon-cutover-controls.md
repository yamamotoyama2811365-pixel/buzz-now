# BUZZ NOW production cutover controls

Implemented in main commit e74d2727c9717970dcd9f34f580334a4bdd5e679.

- `MIGRATION_MAINTENANCE=1`: all HTTP paths except `/health` return 503/no-store/Retry-After; startup skips all schedulers, and db() refuses access. Activate by a Render deploy and verify health reports true before taking the final snapshot. Wait for old connections/in-flight work to drain.
- `DATABASE_BACKEND=source` (default): uses the original `DATABASE_URL` without replacing its secret.
- `DATABASE_BACKEND=neon`: uses separately configured `NEON_DATABASE_URL`; a missing value prevents startup instead of falling back to SQLite.
- `/health` reports only backend name and maintenance state, never credentials.

The final synchronization workflow is restricted to the known Render source and dedicated Neon rehearsal target. It requires an idle source, sets the source's database default to read-only, takes a consistent snapshot, replaces only the rehearsal target in a single transaction, and verifies all row counts and content checksums. On failure it attempts to restore the source default; application maintenance stays active for operator review. On success the source remains read-only as a rollback copy. The earlier verified Neon snapshot is retained.

## Rollback before any new Neon writes

1. Keep production maintenance enabled.
2. Using the source connection secret in the approved migration workflow, run `ALTER DATABASE buzz_now_db RESET default_transaction_read_only` with a session explicitly configured for writes. Do not log connection strings.
3. Select `DATABASE_BACKEND=source`, then disable maintenance and deploy. The original source secret remains intact.
4. Verify `/health`, `/ready`, and homepage. Never trigger a social-post endpoint as a smoke test.

After Neon starts accepting new writes, a simple source switch would lose those new writes. Pause, reconcile data first, and treat that as a separate recovery operation.

The maintenance module passed four unit tests covering request blocking, health/lifespan passthrough, credential selection and rollback, and fail-closed configuration. The actual startup function was also executed in isolation with maintenance on; it returned before scheduler/database dependencies were touched.
