# Traffic history retention

Implementation commit: 52e4b50f1970390ea517de2213c8dd0283780f70

## Policy
- REAL mode numerical traffic metrics are forecasts, not measured visitors.
- Preserve the latest 100 detailed records per existing trend, ordered by id, matching the public API maximum.
- Archive older records into UTC daily sample counts, weighted means and maxima. Sums are internal averaging weights, not actual PV.
- Retain daily summaries for the current UTC day and previous 364 days. Raw latest-100 records remain even for inactive older topics.
- If all forecast values are unchanged, append only on the hourly heartbeat. Save changes immediately, including changes to any metric. Float32 storage rounding does not count as a change.
- Current traffic_totals is still updated each collection cycle.
- Each worker invocation archives at most 20,000 rows. A transaction-level advisory lock prevents concurrent retention workers from double counting. DELETE RETURNING feeds the aggregate upsert in one statement and transaction; a failure rolls back both.
- An active primary Neon application schedules one batch every 2 minutes, first batch 60 seconds after startup. Lock timeout 2s and statement timeout 45s.
- This is an in-process scheduler: Render Free sleeping or downtime delays processing. Paid instance upgrade is still separate work.
- Public /api/trends/{slug}/traffic/daily?days=365 returns combined raw and archived daily forecasts from one SQL snapshot. Existing /traffic?limit=100 remains compatible.
- Orphaned numerical records are conservatively retained rather than deleted without a corresponding trend.

## Deployment
- Feature switch: TRAFFIC_RETENTION_ENABLED=true on the primary service.
- Before enabling, apply ops/traffic-retention-schema.sql to the intended DB using a direct connection. The index uses CONCURRENTLY and must run outside an explicit transaction.
- To disable future cleanup and sampling suppression, set the switch false and deploy. This does not reconstruct individual archived samples.
- No startup DDL and no new public mutation endpoint.

## Verification
- PostgreSQL 18 fixture integration plus sampling unit tests: https://github.com/yamamotoyama2811365-pixel/buzz-now/actions/runs/34445761147
- Covers aggregation conservation, unchanged latest 100 rows, retry idempotence, archive-failure rollback, concurrent lock, summary expiry, heartbeat, floating-point rounding.
- Neon dev branch br-gentle-brook-b3wq50gu copied from production, expires 2026-09-11T06:30:00Z.
- Dev data test: 1,136,427 raw rows -> 1,116,427 raw +20,000 archived samples. Combined pageview forecast sum stayed 1,826,124,066 (checksum-style conservation test, not actual PV).
- Production preflight: index valid; 832,230 eligible raw records, leaving 304,197 from this snapshot after backlog processing.

## Scope and storage
- This change never deletes trends/articles, image data, source URLs, social/Threads records, or prediction evaluation records.
- Images still occupy substantial DB space. Image object storage and prediction-result lifecycle changes remain separate work.
- Normal PostgreSQL DELETE makes space reusable; it does not promise immediate reduction in database file size. No VACUUM FULL or whole-table copy is performed.
- The total database still depends on image growth, new trends and other tables; this is not an unlimited free-capacity guarantee.
