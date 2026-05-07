-- 004_init_stocks.sql
-- Owner: S1
-- Creates stocks.fact_prices and stocks.dim_ticker in dwl-shared-pg.
--
-- The 'stocks' schema itself was created via psql in W1 (see
-- S1_AWS_RUNBOOK.md §11) and is owned by dwl_admin. This migration only
-- adds the two tables.
--
-- Prerequisites:
--   - 001_init_analytics.sql must have been applied (fact_prices.date_sk
--     references analytics.dim_date(date_sk), which won't exist otherwise).
--
-- Idempotent: re-running is a no-op via CREATE TABLE IF NOT EXISTS plus
-- IF NOT EXISTS on each index.
--
-- Cross-team unblocker: this migration mirrors what S2 will do for
-- oil.fact_prices and oil.dim_series, and what S3 will do for
-- news_events.fact_events. The patterns to copy: deterministic surrogate
-- keys, FK to analytics.dim_date(date_sk), UNIQUE on the natural key,
-- and indexes on the dominant scan dimensions.

BEGIN;

-- ─── stocks.dim_ticker ─────────────────────────────────────────────────
-- SCD Type 2 dimension for the 15-ticker D2 universe.
--
-- ticker_sk is explicitly assigned in 005_dim_ticker_seed.sql (NOT auto-
-- generated via SERIAL) so it matches the static TICKER_SK_MAP baked into
-- stocks/lambdas/transform/lambda_function.py (XOM=1, CVX=2, ..., SPY=15
-- in D2-universe order). Re-running the W3 transform writes gold Parquet
-- with these exact ticker_sk values, and the W4 load Lambda's INSERT
-- statements will resolve their FK against this dim seamlessly.
--
-- For the 6.5-week project scope, no ticker renames or delistings are
-- expected. Each ticker therefore has exactly one row with valid_to = NULL
-- ("currently in effect"). If a real corporate action ever happens
-- post-submission, the SCD2 update pattern is: UPDATE the existing row's
-- valid_to to the change timestamp, then INSERT a new row with a fresh
-- ticker_sk and the new attributes.

CREATE TABLE IF NOT EXISTS stocks.dim_ticker (
    ticker_sk     BIGINT       PRIMARY KEY,
    ticker        TEXT         NOT NULL,
    company_name  TEXT,
    sector        TEXT,
    subsector     TEXT,
    country       TEXT,
    exchange      TEXT,
    is_benchmark  BOOLEAN      NOT NULL DEFAULT FALSE,
    valid_from    TIMESTAMPTZ  NOT NULL,
    valid_to      TIMESTAMPTZ,
    UNIQUE (ticker, valid_from)
);

CREATE INDEX IF NOT EXISTS idx_dim_ticker_current
    ON stocks.dim_ticker(ticker)
    WHERE valid_to IS NULL;

COMMENT ON TABLE  stocks.dim_ticker IS 'SCD Type 2 dimension for the 15-ticker D2 universe. ticker_sk explicitly assigned 1..15 in D2 order to match TICKER_SK_MAP in the W3 transform Lambda. valid_to IS NULL means current row.';
COMMENT ON COLUMN stocks.dim_ticker.subsector    IS 'One of: major, e_and_p, refiner, services, tanker, benchmark.';
COMMENT ON COLUMN stocks.dim_ticker.is_benchmark IS 'TRUE for the 3 benchmark instruments XLE / USO / SPY.';

-- ─── stocks.fact_prices ────────────────────────────────────────────────
-- One row per ticker per US trading day. Loaded from gold Parquet at
-- s3://dwl-datapowerchords-gold/stocks/fact_stock_prices/ via the
-- dwl-stocks-load Lambda (W4).
--
-- price_sk is the deterministic surrogate computed by the W3 transform
-- Lambda: ticker_sk * 10^9 + date_sk. The 10^9 multiplier exceeds any
-- date_sk value (YYYYMMDD <= 20301231) so collisions are impossible.
-- Re-loading the same gold partition produces identical price_sk values,
-- which combined with the UNIQUE (ticker_sk, trade_date) constraint and
-- the W4 load Lambda's ON CONFLICT (price_sk) DO UPDATE upsert, makes the
-- entire pipeline idempotent.

CREATE TABLE IF NOT EXISTS stocks.fact_prices (
    price_sk          BIGINT       PRIMARY KEY,
    ticker_sk         BIGINT       NOT NULL REFERENCES stocks.dim_ticker(ticker_sk),
    date_sk           INTEGER      NOT NULL REFERENCES analytics.dim_date(date_sk),
    trade_date        DATE         NOT NULL,
    open              NUMERIC(18,6),
    high              NUMERIC(18,6),
    low               NUMERIC(18,6),
    close             NUMERIC(18,6),
    adj_close         NUMERIC(18,6),
    volume            BIGINT,
    log_return        NUMERIC(18,8),
    ingest_timestamp  TIMESTAMPTZ  NOT NULL,
    source            TEXT         NOT NULL DEFAULT 'yfinance',
    UNIQUE (ticker_sk, trade_date)
);

-- Indexes for the dominant query patterns:
--   * Q1/Q2 cross-domain marts join on date_sk (sub-millisecond JOIN against dim_date PK)
--   * Q2 divergence-analysis scans "all tickers for a trade_date range" → trade_date btree
--   * Per-ticker zoom in Tableau dashboards → ticker_sk filter
-- The PK on price_sk handles the upsert path.
CREATE INDEX IF NOT EXISTS idx_fact_prices_date_sk    ON stocks.fact_prices(date_sk);
CREATE INDEX IF NOT EXISTS idx_fact_prices_trade_date ON stocks.fact_prices(trade_date);
CREATE INDEX IF NOT EXISTS idx_fact_prices_ticker_sk  ON stocks.fact_prices(ticker_sk);

COMMENT ON TABLE  stocks.fact_prices             IS 'Stock-price fact, one row per ticker per US trading day, loaded from s3://dwl-datapowerchords-gold/stocks/fact_stock_prices/.';
COMMENT ON COLUMN stocks.fact_prices.price_sk    IS 'Deterministic surrogate: ticker_sk * 10^9 + date_sk. Re-loading the same gold partition is a no-op upsert.';
COMMENT ON COLUMN stocks.fact_prices.log_return  IS 'ln(adj_close[t] / adj_close[t-1]). NULL for the first trading day in each ticker''s series (no prior close to compare against).';
COMMENT ON COLUMN stocks.fact_prices.source      IS 'Constant ''yfinance'' since W2 D18 (was ''alpha_vantage'' pre-D18).';

COMMIT;

-- Verification (run interactively after applying):
--   \d stocks.dim_ticker
--   \d stocks.fact_prices
--   SELECT COUNT(*) FROM stocks.dim_ticker;             -- expect 0 until 005_dim_ticker_seed.sql runs
--   SELECT COUNT(*) FROM stocks.fact_prices;            -- expect 0 until dwl-stocks-load Lambda runs
--   -- FK sanity:
--   SELECT conname, pg_get_constraintdef(oid)
--   FROM   pg_constraint
--   WHERE  conrelid = 'stocks.fact_prices'::regclass
--     AND  contype = 'f';
--   -- expect 2 FKs: one to stocks.dim_ticker(ticker_sk), one to analytics.dim_date(date_sk)
