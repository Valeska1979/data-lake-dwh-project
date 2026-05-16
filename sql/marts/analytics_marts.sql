-- ── mart 1: daily signals ─────────────────────────────────────
-- One row per day per ticker.
-- Full continuous time series including quiet days (event_count=0).
-- Primary input for regression analysis.

CREATE OR REPLACE VIEW analytics.mart_daily_signals AS
SELECT
    -- date
    d.full_date                         AS trade_date,
    d.year,
    d.month,
    d.day_name,
    d.is_weekend,
    d.is_us_trading_day,

    -- stock
    t.ticker,
    t.company_name,
    sp.open                             AS stock_open,
    sp.close                            AS stock_close,
    sp.adj_close                        AS stock_adj_close,
    sp.volume                           AS stock_volume,
    sp.log_return                       AS stock_log_return,

    -- conflict signals
    COALESCE(c.event_count, 0)          AS event_count,
    COALESCE(c.max_severity, 0)         AS max_severity,
    COALESCE(c.avg_severity, 0)         AS avg_severity,
    COALESCE(c.has_conflict, FALSE)     AS has_conflict,
    c.top_event_summary,
    c.top_event_category,
    COALESCE(c.sanctions_count, 0)      AS sanctions_count,
    COALESCE(c.military_count, 0)       AS military_count,
    COALESCE(c.security_count, 0)       AS security_count,
    COALESCE(c.nuclear_count, 0)        AS nuclear_count,
    COALESCE(c.diplomatic_count, 0)     AS diplomatic_count,
    COALESCE(c.sanctions_max_sev, 0)    AS sanctions_max_sev,
    COALESCE(c.military_max_sev, 0)     AS military_max_sev,
    COALESCE(c.security_max_sev, 0)     AS security_max_sev,
    COALESCE(c.nuclear_max_sev, 0)      AS nuclear_max_sev,
    COALESCE(c.diplomatic_max_sev, 0)   AS diplomatic_max_sev,
    COALESCE(c.hormuz_flag, FALSE)      AS hormuz_flag,

    -- oil prices
    brent.price                         AS brent_price,
    wti.price                           AS wti_price

FROM analytics.dim_date d

-- stocks — inner join so we only get trading days
JOIN stocks.fact_prices sp   ON sp.date_sk = d.date_sk
JOIN stocks.dim_ticker  t    ON t.ticker_sk = sp.ticker_sk

-- conflict — left join so trading days with no events get zeros
LEFT JOIN news_events.fact_conflict_daily c
    ON c.event_date = d.full_date

-- oil — left join, filter to Brent and WTI separately
LEFT JOIN oil.fact_prices brent
    ON brent.date_sk = d.date_sk AND brent.oil_type = 'Brent'
LEFT JOIN oil.fact_prices wti
    ON wti.date_sk  = d.date_sk AND wti.oil_type  = 'WTI'

WHERE d.full_date BETWEEN '2025-01-01' AND CURRENT_DATE;


-- ── mart 2: event study ───────────────────────────────────────
-- One row per high-severity day (max_severity >= 3) per ticker.
-- Includes same-day, +1 day, +2 day stock and oil returns.
-- Used for event study analysis in thesis.

CREATE OR REPLACE VIEW analytics.mart_event_study AS
WITH trading_days AS (
    SELECT
        sp.date_sk,
        d.full_date                                             AS trade_date,
        t.ticker,
        t.company_name,
        sp.close                                                AS stock_close,
        LAG(sp.log_return, 2) OVER (
            PARTITION BY t.ticker ORDER BY d.full_date
        )                                                       AS stock_return_minus2d,
        LAG(sp.log_return, 1) OVER (
            PARTITION BY t.ticker ORDER BY d.full_date
        )                                                       AS stock_return_minus1d,
        sp.log_return                                           AS stock_return_0d,
        LEAD(sp.log_return, 1) OVER (
            PARTITION BY t.ticker ORDER BY d.full_date
        )                                                       AS stock_return_1d,
        LEAD(sp.log_return, 2) OVER (
            PARTITION BY t.ticker ORDER BY d.full_date
        )                                                       AS stock_return_2d
    FROM stocks.fact_prices sp
    JOIN analytics.dim_date d ON d.date_sk = sp.date_sk
    JOIN stocks.dim_ticker  t ON t.ticker_sk = sp.ticker_sk
),
oil_days AS (
    SELECT
        d.full_date                                             AS trade_date,
        oil_type,
        price,
        ROUND(
            (price - LAG(price, 2) OVER (PARTITION BY oil_type ORDER BY d.full_date))
            / NULLIF(LAG(price, 2) OVER (PARTITION BY oil_type ORDER BY d.full_date), 0) * 100, 4
        )                                                       AS price_pct_change_minus2d,
        ROUND(
            (price - LAG(price, 1) OVER (PARTITION BY oil_type ORDER BY d.full_date))
            / NULLIF(LAG(price, 1) OVER (PARTITION BY oil_type ORDER BY d.full_date), 0) * 100, 4
        )                                                       AS price_pct_change_minus1d,
        ROUND(
            (LEAD(price, 1) OVER (PARTITION BY oil_type ORDER BY d.full_date) - price)
            / NULLIF(price, 0) * 100, 4
        )                                                       AS price_pct_change_1d,
        ROUND(
            (LEAD(price, 2) OVER (PARTITION BY oil_type ORDER BY d.full_date) - price)
            / NULLIF(price, 0) * 100, 4
        )                                                       AS price_pct_change_2d
    FROM oil.fact_prices op
    JOIN analytics.dim_date d ON d.date_sk = op.date_sk
)
SELECT
    c.event_date,
    c.max_severity,
    c.top_event_summary,
    c.top_event_category,
    c.event_count,
    c.hormuz_flag,
    c.sanctions_count,
    c.military_count,
    c.security_count,
    c.nuclear_count,
    c.diplomatic_count,
    td.ticker,
    td.company_name,
    td.stock_close,
    td.stock_return_minus2d,
    td.stock_return_minus1d,
    td.stock_return_0d,
    td.stock_return_1d,
    td.stock_return_2d,
    brent.price                         AS brent_price,
    brent.price_pct_change_minus2d      AS brent_pct_change_minus2d,
    brent.price_pct_change_minus1d      AS brent_pct_change_minus1d,
    brent.price_pct_change_1d           AS brent_pct_change_1d,
    brent.price_pct_change_2d           AS brent_pct_change_2d,
    wti.price                           AS wti_price,
    wti.price_pct_change_minus2d        AS wti_pct_change_minus2d,
    wti.price_pct_change_minus1d        AS wti_pct_change_minus1d,
    wti.price_pct_change_1d             AS wti_pct_change_1d,
    wti.price_pct_change_2d             AS wti_pct_change_2d
FROM news_events.fact_conflict_daily c
JOIN trading_days td     ON td.trade_date = c.event_date
LEFT JOIN oil_days brent ON brent.trade_date = c.event_date AND brent.oil_type = 'Brent'
LEFT JOIN oil_days wti   ON wti.trade_date  = c.event_date AND wti.oil_type  = 'WTI'
WHERE c.max_severity >= 3
ORDER BY c.event_date, td.ticker;

-- ── mart 3: category impact ───────────────────────────────────
-- One row per event category per ticker.
-- Average stock and oil returns on days when that category was top event.
-- Answers: which type of conflict moves markets most?

CREATE OR REPLACE VIEW analytics.mart_category_impact AS
WITH base AS (
    SELECT
        c.top_event_category,
        c.max_severity,
        c.hormuz_flag,
        t.ticker,
        t.company_name,
        sp.log_return                   AS stock_return,
        brent.price                     AS brent_price,
        wti.price                       AS wti_price
    FROM news_events.fact_conflict_daily c
    JOIN stocks.fact_prices sp   ON sp.date_sk = (
        SELECT date_sk FROM analytics.dim_date WHERE full_date = c.event_date
    )
    JOIN stocks.dim_ticker  t    ON t.ticker_sk = sp.ticker_sk
    LEFT JOIN oil.fact_prices brent
        ON brent.date_sk = (
            SELECT date_sk FROM analytics.dim_date WHERE full_date = c.event_date
        ) AND brent.oil_type = 'Brent'
    LEFT JOIN oil.fact_prices wti
        ON wti.date_sk = (
            SELECT date_sk FROM analytics.dim_date WHERE full_date = c.event_date
        ) AND wti.oil_type = 'WTI'
    WHERE c.has_conflict = TRUE
    AND c.top_event_category IS NOT NULL
    AND c.top_event_category != ''
)
SELECT
    top_event_category,
    ticker,
    company_name,
    COUNT(*)                            AS event_days,
    ROUND(AVG(max_severity), 2)         AS avg_severity,
    ROUND(AVG(stock_return) * 100, 4)   AS avg_stock_return_pct,
    ROUND(MIN(stock_return) * 100, 4)   AS min_stock_return_pct,
    ROUND(MAX(stock_return) * 100, 4)   AS max_stock_return_pct,
    ROUND(STDDEV(stock_return) * 100, 4) AS stddev_stock_return_pct,
    ROUND(AVG(brent_price), 2)          AS avg_brent_price,
    ROUND(AVG(wti_price), 2)            AS avg_wti_price,
    SUM(CASE WHEN hormuz_flag THEN 1 ELSE 0 END) AS hormuz_event_days
FROM base
GROUP BY top_event_category, ticker, company_name
ORDER BY top_event_category, ticker;
