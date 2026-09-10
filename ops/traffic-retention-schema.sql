CREATE TABLE IF NOT EXISTS traffic_daily_archive (
    trend_id BIGINT NOT NULL,
    day TEXT NOT NULL,
    samples BIGINT NOT NULL,
    first_recorded_at TEXT NOT NULL,
    last_recorded_at TEXT NOT NULL,
    impressions_sum DOUBLE PRECISION NOT NULL, impressions_max DOUBLE PRECISION NOT NULL,
clicks_sum DOUBLE PRECISION NOT NULL, clicks_max DOUBLE PRECISION NOT NULL,
pageviews_sum DOUBLE PRECISION NOT NULL, pageviews_max DOUBLE PRECISION NOT NULL,
ctr_sum DOUBLE PRECISION NOT NULL, ctr_max DOUBLE PRECISION NOT NULL,
traffic_potential_sum DOUBLE PRECISION NOT NULL, traffic_potential_max DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (trend_id,day)
);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_traffic_history_trend_id
               ON traffic_history(trend_id,id DESC);
