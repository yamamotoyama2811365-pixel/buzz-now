CREATE TABLE IF NOT EXISTS corporate_corrections (
 id text PRIMARY KEY,
 event_id text NOT NULL REFERENCES corporate_events(id),
 details text NOT NULL CHECK (length(details) BETWEEN 10 AND 2000),
 status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','resolved')),
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS corporate_corrections_date ON corporate_corrections(created_at);
