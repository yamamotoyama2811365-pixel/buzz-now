# BUZZ NOW database migration

## Scope

- Source: Render `buzz_now_db`, PostgreSQL 18, instance `dpg-daak8j142hec73ao272g-a`.
- Target: Neon project `sparkling-night-68584305`, branch `br-fancy-surf-b3h3m3yc`, database `buzz_now_db`.
- GitHub repository secrets: `MIGRATION_SOURCE_DATABASE_URL`, `MIGRATION_TARGET_DATABASE_URL` (direct connection).
- No credential belongs in code, logs, or artifacts.

## Current workflow

`.github/workflows/neon-migration-preflight.yml` runs only on this operations branch when the workflow changes. It validates the exact hosts, forces TLS, checks the target is empty, exports one read-only source snapshot and restores atomically. Every table's count and order-independent complete-row checksum are compared against that source snapshot. A successful job is a verified rehearsal, not a completed live migration. It does not update Render configuration or run collectors/social posting.

The source-preflight job succeeded on commit `04ef7199ef24dfcfe6e707c7c5dffab97614c617`. Its temporary dump was removed; it is not a retained backup. Preserve the original database and create a Neon snapshot after successful copy verification.

## Before live cutover

1. Confirm the verified copy fits the free project's capacity with growth margin.
2. Prepare maintenance controls covering scheduled collectors, editorial/social jobs, public write endpoints, traffic logging, and in-flight writes. The current primary app schedules work at startup; starting a second live instance can duplicate outbound actions.
3. Stop/drain writes, then take and verify a final consistent copy to a separate empty destination. Do not point production at this earlier rehearsal copy, which becomes stale as the source changes.
4. Save a recoverable backup and a concrete rollback path before changing connection settings. Preserve all other Render environment variables.
5. Switch the actual primary service (`buzz-now-1`) to the final Neon database; verify reads and internal writes without triggering external social posts. Resume normal scheduling once.
6. Retain the old source while verifying operation. Do not delete it or modify the open-close-map database.

The API consolidation and paid Render service are separate pending work. This branch does not change main or deploy either existing site.
