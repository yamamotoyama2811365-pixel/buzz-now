# Open Close Map on the shared service

Vendored from yamamotoyama2811365-pixel/open-close-map commit 60a43d903d33188086ad5799e0a8c8f496f2d4ec (backend Python files only).

The host app mounts this app at /open-close when OPEN_CLOSE_ENABLED=true. DB and all settings use OPEN_CLOSE_ prefixes to avoid mixing BUZZ NOW and store credentials. OPEN_CLOSE_DATABASE_URL must point to the existing store Neon project, never BUZZ NOW. No mounted startup DDL runs; existing stores/site_inquiries schemas are required.

Changes relative to upstream: namespaced imports/environment, /ready, shared hosting health marker, GitHub OIDC for the exact collector workflow and a separate read-only check workflow. Collector logic, public response shapes and canonical site origin are preserved. When updating upstream collectors, re-vendor deliberately and rerun compatibility tests.

GitHub identities can only call full-cycle, source-health and prefecture-coverage, with pinned repository ID, branch, workflow, event, issuer, audience and signature verification. They cannot access private inquiries or destructive maintenance endpoints. An optional OPEN_CLOSE_ADMIN_KEY can support manual operator access; never reuse a BUZZ NOW key. Existing manual operator access at the old store service remains available during migration.

The original backend and static SEO rewrite routes are retained until their consumers have moved and been verified. Do not suspend the old service while Render static rewrites still target it.
