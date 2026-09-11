-- Additive migration: OPEN CLOSE MAP database only. No existing tables changed.
CREATE TABLE IF NOT EXISTS public.ocm_page_views_daily (
    day date NOT NULL,
    page text NOT NULL CHECK (length(page) <= 250 AND page LIKE '/%'),
    source text NOT NULL CHECK (source IN ('google','yahoo','bing','direct_or_unknown','referral','internal')),
    is_test boolean NOT NULL DEFAULT FALSE,
    views bigint NOT NULL DEFAULT 0 CHECK (views >= 0),
    PRIMARY KEY (day,page,source,is_test)
);
CREATE TABLE IF NOT EXISTS public.ocm_pv_metadata (
    singleton boolean PRIMARY KEY DEFAULT TRUE CHECK (singleton=TRUE),
    started_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO public.ocm_pv_metadata(singleton) VALUES(TRUE) ON CONFLICT DO NOTHING;
