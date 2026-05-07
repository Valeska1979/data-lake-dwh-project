-- 001_init_analytics.sql
-- Owner: S1
-- Adds analytics.dim_date to the shared dwl-shared-pg database.
--
-- The 'analytics' schema itself already exists (created via psql in W1, see
-- S1_AWS_RUNBOOK.md §11). This migration ONLY adds the conformed date
-- dimension that every cross-domain mart in W4 will join to.
--
-- Schema must match docs/GLUE_CATALOG_REGISTRY.md §2.5 column-for-column so
-- that the gold-zone Parquet at s3://dwl-datapowerchords-gold/analytics/
-- dim_date/dim_date.parquet loads cleanly via 003_dim_date_load.py.
--
-- Cross-team unblocker: this migration must run before
--   * S2's oil.fact_prices migration (FK: date_sk → analytics.dim_date)
--   * S3's analytics.mart_* tables (every mart joins on date_sk)
-- so file an early WhatsApp note when it's loaded.
--
-- Idempotent: re-running is a no-op (CREATE TABLE IF NOT EXISTS, plus the
-- 003 loader uses ON CONFLICT DO UPDATE).

BEGIN;

CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_sk            INTEGER     PRIMARY KEY,                          -- YYYYMMDD as int (e.g. 20260423)
    full_date          DATE        NOT NULL UNIQUE,
    year               SMALLINT    NOT NULL,
    quarter            SMALLINT    NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month              SMALLINT    NOT NULL CHECK (month   BETWEEN 1 AND 12),
    month_name         TEXT        NOT NULL,
    day                SMALLINT    NOT NULL CHECK (day     BETWEEN 1 AND 31),
    day_of_week        SMALLINT    NOT NULL CHECK (day_of_week BETWEEN 0 AND 6),  -- 0 = Sunday (per spec)
    day_name           TEXT        NOT NULL,
    week_of_year       SMALLINT    NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),
    is_weekend         BOOLEAN     NOT NULL,
    is_us_holiday      BOOLEAN     NOT NULL,
    is_us_trading_day  BOOLEAN     NOT NULL,
    fiscal_year        SMALLINT    NOT NULL
);

COMMENT ON TABLE  analytics.dim_date                    IS 'Conformed date dimension, 2025-01-01 to 2030-12-31, ~2,191 rows. Owned by S1; shared across stocks, oil, news_events fact tables and analytics.mart_* tables. Loaded from gold Parquet via 003_dim_date_load.py.';
COMMENT ON COLUMN analytics.dim_date.date_sk            IS 'Integer surrogate key in YYYYMMDD form. FK target for every fact table.';
COMMENT ON COLUMN analytics.dim_date.day_of_week        IS '0 = Sunday, 1 = Monday, ..., 6 = Saturday (matches docs/GLUE_CATALOG_REGISTRY.md §2.5).';
COMMENT ON COLUMN analytics.dim_date.is_us_holiday      IS 'TRUE for any day NYSE is closed for a non-weekend reason: federal holidays + Good Friday.';
COMMENT ON COLUMN analytics.dim_date.is_us_trading_day  IS 'TRUE iff NYSE is open. Use this for trading-window filters in mart queries (avoids the weekend + holiday gap-fill question).';

-- Indexes for the two query patterns marts will hit:
--   1. Range scan on full_date (Q1, Q2 timeline charts):
--        SELECT ... FROM stocks.fact_prices f JOIN analytics.dim_date d ON f.date_sk = d.date_sk WHERE d.full_date BETWEEN ...
--      The PK on date_sk handles the JOIN; full_date UNIQUE handles the predicate.
--   2. Trading-day filter (Q4 event-impact analysis ignoring weekends/holidays):
--        WHERE is_us_trading_day = TRUE AND full_date BETWEEN ...
--      A partial index on full_date filtered by is_us_trading_day gives a small,
--      fast scan when answering "what's the next/previous trading day".
CREATE INDEX IF NOT EXISTS idx_dim_date_trading
    ON analytics.dim_date(full_date)
    WHERE is_us_trading_day;

COMMIT;

-- Verification (run interactively after applying):
--   SELECT COUNT(*), MIN(full_date), MAX(full_date) FROM analytics.dim_date;
--     expect: 0  NULL  NULL    (table exists but empty until 003_dim_date_load.py runs)
--   \d analytics.dim_date
--     expect: 14 columns matching catalog registry §2.5
