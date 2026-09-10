CREATE TABLE IF NOT EXISTS corporate_events (
 id text PRIMARY KEY,
 kind text NOT NULL CHECK (kind IN ('bankruptcy','registration')),
 company text NOT NULL,
 corporate_number text NOT NULL DEFAULT '',
 prefecture text NOT NULL DEFAULT '',
 industry text NOT NULL DEFAULT '',
 stage text NOT NULL,
 reported_date date NOT NULL,
 payload jsonb NOT NULL,
 first_seen timestamptz NOT NULL DEFAULT now(),
 published boolean NOT NULL DEFAULT true,
 updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS corporate_events_date ON corporate_events(kind,reported_date DESC,id);
CREATE INDEX IF NOT EXISTS corporate_events_area ON corporate_events(prefecture,industry,reported_date DESC);
CREATE TABLE IF NOT EXISTS corporate_runs (
 source text PRIMARY KEY,
 checked_at timestamptz NOT NULL DEFAULT now(),
 status text NOT NULL,
 received integer NOT NULL DEFAULT 0,
 detail text NOT NULL DEFAULT ''
);

ALTER TABLE corporate_events ADD COLUMN IF NOT EXISTS published boolean NOT NULL DEFAULT true;

CREATE TABLE IF NOT EXISTS corporate_corrections (
 id text PRIMARY KEY,
 event_id text NOT NULL REFERENCES corporate_events(id),
 details text NOT NULL CHECK (length(details) BETWEEN 10 AND 2000),
 status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','resolved')),
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS corporate_corrections_date ON corporate_corrections(created_at);
