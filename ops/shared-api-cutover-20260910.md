# Shared Open Close Map API — 2026-09-10

## Deployed
- Paid primary Render service buzz-now-1, 0.5 CPU / 512 MB, now mounts the store app at https://buzz-now-1.onrender.com/open-close.
- BUZZ NOW app and its DB selection remain unchanged.
- Store database remains the existing Neon project icy-firefly-40899552 / br-wild-union-b3mgt62w / neondb. No store data copying or DDL was performed.
- Shared code commits: cc06cb416194dd678c62cbc94840a9b771547feb and 4f7206af7e9acab702aee05d82de8d7e923fd417.
- Latest shared Render deployment dep-dah5h815efls739fkhtg live 07:14:38 UTC.
- Open Close Map client/cycle commit 597c0d29bc90c22a9890694509185fd8acfc9b1d.
- Static deployment dep-dah5i1oae00c73ai77s0 live 07:15:32 UTC.
- Updated app.js, detail.js, analytics.js, status-page.js and contact.html. Added asset version parameters in referencing pages.
- Hourly collector workflow now targets the shared API and requests fresh GitHub OIDC credentials per API invocation. No shared secrets were printed, committed or transferred through logs.

## Verified
- 14 tests pass: JWT signature/issuer/audience/expiry, exact repository ID/ref/workflow/event, both standard and immutable-ID subjects, read-only checker restrictions, unauthorized private-inbox rejection, configuration isolation, CORS, existing inquiry validation and public store presentation.
- Test run: https://github.com/yamamotoyama2811365-pixel/buzz-now/actions/runs/34448817751
- Live shared API verified against old API for one existing store; DB readiness, public endpoints, SEO HTML, CORS, GitHub-authenticated source-health and prefecture-coverage, private inbox denied without manual authorization.
- Final public client verification: https://github.com/yamamotoyama2811365-pixel/open-close-map/actions/runs/34449095269
- Both sites available; five deployed public client files point to the shared API.
- Observed memory samples around deployment were below 138 MB per instance versus 512 MiB limit. This is not a full collector peak-load test.

## Remaining before retiring old API
Do not suspend or delete open-close-map-api yet.
1. DONE: user saved the five static rewrite destinations. Public sitemap and store/area/category requests with unique probe rewrite-34449807455 were observed in the paid shared service access logs with HTTP 200 at 07:24:52–53 UTC. robots.txt is served successfully with the correct canonical sitemap URL; an existing static robots.txt takes precedence over a rewrite.
2. Preserve manual operator access: the original ADMIN_KEY still works at the old service. The shared service supports OPEN_CLOSE_ADMIN_KEY, but that key is not set because the connection cannot read the old service secret. Copy it directly between Render environment screens if manual operator access is required. Never paste it into a chat or repository. GitHub collector identity intentionally cannot access private inquiries or destructive maintenance.
3. Confirm one scheduled full cycle on the new shared endpoint. Read-only checks did not run collection or create inquiries.
4. DONE: public canonical store/area/category pages, sitemap containing 2,005 canonical URLs, robots.txt, client API URLs and shared database readiness passed workflow https://github.com/yamamotoyama2811365-pixel/open-close-map/actions/runs/34449807455 (job 102782735211). Keep old compute until items 2 and 3 are completed.

## Rollback and future updates
- Revert the frontend/workflow cutover commit to restore old API consumers; existing DB and old service are retained.
- Set OPEN_CLOSE_ENABLED=false to disable the mounted API if needed; this does not affect BUZZ NOW database selection.
- Backend code was vendored from open-close-map commit 60a43d903d33188086ad5799e0a8c8f496f2d4ec. Future backend edits must also update services/open_close in buzz-now deliberately and pass shared tests.
- Corporate-signal remains separate pending work; it has not been deployed by this change.
