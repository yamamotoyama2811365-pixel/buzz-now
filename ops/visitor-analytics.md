# BUZZ NOW first-party visitor analytics

This measures browser pageviews independently of Google Analytics and independently of the existing modeled `traffic_totals`, `traffic_history` and `traffic_daily_archive`. Never combine those counters with these observations.

## Deployment

The feature-branch workflow runs the idempotent integration script, unit and browser-source tests, existing regression tests, and an application import. It commits only the verified wiring to `feat/visitor-analytics-20260911`. Review that branch before merging. The integration adds the collector to app.main and a deferred script to index.html and trend.html; it does not modify existing GA4 or advertising tags, or corporate/open-close data.

At normal startup on the primary service, two additive tables are initialized in the existing database. `VISITOR_ANALYTICS_ENABLED=false` disables ingestion. No new paid subscription or server is provisioned. Existing database/service quotas still apply.

## Data and limitations

`visitor_pageviews_daily`: Japan-local day, decoded page pathname, allowlisted source, test flag, aggregate view count. `visitor_analytics_state`: operational start timestamp only. No IP, cookies, session or visitor IDs, user agents, search terms, full URLs or referrer URLs are stored by this collector. A random document nonce is retained in bounded RAM for 10 minutes only for duplicate suppression; it is not a visitor/session identifier. Existing GA4/ad tags and platform request logs are separate and unchanged.

Only home and existing /trend/ pages are accepted. Internal navigation is its own source, never counted as another X/Google entry. X/t.co and explicit X tags are grouped as x. Google and Yahoo referrers indicate source, not proof of organic-search causality; known paid markers are separated. Missing referrers may include unidentifiable social traffic. Known bots, webdriver, DNT and GPC are excluded; bot exclusion is not perfect. No unique-user, session, dwell-time or GA4 engagement metric is inferred. Tracking begins only after deployment; no retrospective reconstruction is possible.

## Reading results privately through Neon

Always filter `is_test=0`. Diagnostics with `?bn_analytics_test=1` or POST test=true are separate. The public status endpoint exposes configuration/health only, not counts. Compare the same page's X counts against later Google counts to flag a possible propagation signal, not causation or an individual user's path. No scheduled ChatGPT reporting or autonomous SEO changes are enabled by this implementation.

```sql
SELECT day, source, SUM(views) AS pageviews
FROM visitor_pageviews_daily WHERE is_test=0
GROUP BY day, source ORDER BY day DESC, pageviews DESC;
```
