# Verified Neon copy — 2026-09-10

- Workflow: https://github.com/yamamotoyama2811365-pixel/buzz-now/actions/runs/34442066095
- Verified workflow commit: f476f5bb04f082deb122666535c6ce9401ccdd1d
- Result: success; all 29 public tables matched the same exported source snapshot by row count and order-independent full-row checksum.
- Neon project: sparkling-night-68584305; branch: br-fancy-surf-b3h3m3yc; database: buzz_now_db.
- Post-import database size: 347275264 bytes (not a guarantee of future quota headroom).
- Trends: 3394 rows; traffic_history: 1133031 rows; social_images: 60 rows.
- Retained Neon snapshot: snap-holy-frost-b3mv1s2j (buzz-now-verified-import-20260910).

## Still pending

The production application has NOT been switched. Render's original DB still receives updates, so this verified import is a rehearsal/backup, not a current live replica. A final write-quiesced synchronization, recovery plan, connection switch, and application checks are required. Do not restart the copy workflow against this nonempty target: it will refuse to overwrite. Source DB and existing services have not been deleted or reconfigured. Paid Render consolidation has not been performed.
