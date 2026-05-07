-- 005_dim_ticker_seed.sql
-- Owner: S1
-- Seeds stocks.dim_ticker with the 15 D2-universe tickers.
--
-- ticker_sk values MUST match the static TICKER_SK_MAP in
-- stocks/lambdas/transform/lambda_function.py (XOM=1 ... SPY=15 in D2 order)
-- so the gold Parquet rows produced by the W3 transform Lambda load
-- cleanly into stocks.fact_prices via the W4 dwl-stocks-load Lambda.
-- DO NOT change the ticker_sk values without simultaneously updating
-- TICKER_SK_MAP in the transform Lambda AND re-running the full backfill
-- against the gold zone.
--
-- Prerequisite: 004_init_stocks.sql must have been applied.
--
-- Idempotent: ON CONFLICT (ticker_sk) DO UPDATE so re-running this
-- refreshes the descriptive attributes (company_name, sector, etc.) but
-- preserves the SCD2 valid_from / valid_to fields untouched.

BEGIN;

INSERT INTO stocks.dim_ticker (
    ticker_sk, ticker, company_name, sector, subsector, country, exchange,
    is_benchmark, valid_from, valid_to
) VALUES
    -- Tier 1: major integrated oil & gas
    ( 1, 'XOM',  'Exxon Mobil Corporation',          'Energy', 'major',     'US', 'NYSE',   FALSE, '2025-01-01 00:00:00+00', NULL),
    ( 2, 'CVX',  'Chevron Corporation',              'Energy', 'major',     'US', 'NYSE',   FALSE, '2025-01-01 00:00:00+00', NULL),
    ( 3, 'SHEL', 'Shell plc',                        'Energy', 'major',     'GB', 'NYSE',   FALSE, '2025-01-01 00:00:00+00', NULL),
    -- Tier 2: exploration & production (E&P)
    ( 4, 'COP',  'ConocoPhillips',                   'Energy', 'e_and_p',   'US', 'NYSE',   FALSE, '2025-01-01 00:00:00+00', NULL),
    ( 5, 'OXY',  'Occidental Petroleum Corporation', 'Energy', 'e_and_p',   'US', 'NYSE',   FALSE, '2025-01-01 00:00:00+00', NULL),
    ( 6, 'DVN',  'Devon Energy Corporation',         'Energy', 'e_and_p',   'US', 'NYSE',   FALSE, '2025-01-01 00:00:00+00', NULL),
    -- Tier 3: refiners
    ( 7, 'VLO',  'Valero Energy Corporation',        'Energy', 'refiner',   'US', 'NYSE',   FALSE, '2025-01-01 00:00:00+00', NULL),
    ( 8, 'MPC',  'Marathon Petroleum Corporation',   'Energy', 'refiner',   'US', 'NYSE',   FALSE, '2025-01-01 00:00:00+00', NULL),
    -- Tier 4: oilfield services
    ( 9, 'SLB',  'Schlumberger Limited',             'Energy', 'services',  'US', 'NYSE',   FALSE, '2025-01-01 00:00:00+00', NULL),
    (10, 'HAL',  'Halliburton Company',              'Energy', 'services',  'US', 'NYSE',   FALSE, '2025-01-01 00:00:00+00', NULL),
    (11, 'BKR',  'Baker Hughes Company',             'Energy', 'services',  'US', 'NASDAQ', FALSE, '2025-01-01 00:00:00+00', NULL),
    -- Tier 5: tankers
    (12, 'FRO',  'Frontline plc',                    'Energy', 'tanker',    'CY', 'NYSE',   FALSE, '2025-01-01 00:00:00+00', NULL),
    -- Tier 6: benchmarks (ETFs)
    (13, 'XLE',  'Energy Select Sector SPDR Fund',   'Energy', 'benchmark', 'US', 'NYSE',   TRUE,  '2025-01-01 00:00:00+00', NULL),
    (14, 'USO',  'United States Oil Fund LP',        'Energy', 'benchmark', 'US', 'NYSE',   TRUE,  '2025-01-01 00:00:00+00', NULL),
    (15, 'SPY',  'SPDR S&P 500 ETF Trust',           'Other',  'benchmark', 'US', 'NYSE',   TRUE,  '2025-01-01 00:00:00+00', NULL)
ON CONFLICT (ticker_sk) DO UPDATE SET
    ticker        = EXCLUDED.ticker,
    company_name  = EXCLUDED.company_name,
    sector        = EXCLUDED.sector,
    subsector     = EXCLUDED.subsector,
    country       = EXCLUDED.country,
    exchange      = EXCLUDED.exchange,
    is_benchmark  = EXCLUDED.is_benchmark;
-- Note: valid_from / valid_to are NOT updated on conflict. They're SCD2-managed,
-- not part of the static reference data. A real corporate action gets a NEW
-- row with a different ticker_sk and a fresh valid_from, not an UPDATE here.

COMMIT;

-- Verification (run interactively after applying):
--   SELECT ticker_sk, ticker, sector, subsector, is_benchmark
--   FROM   stocks.dim_ticker
--   ORDER  BY ticker_sk;
--     expect: 15 rows, ticker_sk 1..15 in D2-universe order, three TRUE benchmarks (XLE/USO/SPY)
--
--   SELECT COUNT(*) FROM stocks.dim_ticker WHERE valid_to IS NULL;
--     expect: 15  (all current)
