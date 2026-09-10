# Production retention result — 2026-09-10

- Main commit 52e4b50f1970390ea517de2213c8dd0283780f70 deployed to buzz-now-1.
- Deployment dep-dah503ri22cc73fq1ci0 live at 06:39:42 UTC (15:39:42 JST).
- TRAFFIC_RETENTION_ENABLED=true.
- Read-only public checks passed: homepage, Neon readiness, recent 100-sample endpoint, new daily forecast summary endpoint.
- Evidence: https://github.com/yamamotoyama2811365-pixel/buzz-now/actions/runs/34446057002
- First automatic production batch confirmed via SQL: raw rows decreased from 1,136,427 to 1,116,427; daily archive contains 20,000 samples in 1,093 trend/day groups.
- Remaining backlog is processed by the active application's every-two-minute worker. Initial total eligibility was 832,230 rows; completion of the entire backlog is not claimed here.
- Seven tests including PostgreSQL rollback, aggregate conservation and concurrent-worker exclusion passed before deployment.
- No article, image or social posting records were deleted by this feature.
- Render paid upgrade/API consolidation and image storage changes remain outstanding. Background cleanup pauses when the Render Free application sleeps.
- Operational policy and disabling instructions: traffic-retention-operations-20260910.md.
