-- ═══════════════════════════════════════════════════════════════════════════
-- Database : fuel_intelligence
-- Run this script ONCE in the Redshift Query Editor before first pipeline run.
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Tables created:
--   DIMENSION: dim_date, dim_fuel_type, dim_crude_oil, dim_fx_rates
--   FACT:      fact_fuel_prices, fact_fuel_monthly_summary
--   VIEW:      vw_fuel_vs_crude  (used by QuickSight BI dashboard)
--
-- Run order: run the whole script at once — dependencies handled by IF NOT EXISTS
-- ═══════════════════════════════════════════════════════════════════════════


-- ═══════════════════════════════════════════════════════════════════════════
-- DIMENSION TABLES
-- ═══════════════════════════════════════════════════════════════════════════

-- dim_date
-- Time dimension for slicing BI charts by year / quarter / month.
-- is_crisis_period flags the 2022 Sri Lanka economic crisis months.
CREATE TABLE IF NOT EXISTS dim_date (
    date_key         DATE         NOT NULL,
    year             SMALLINT     NOT NULL,
    quarter          SMALLINT     NOT NULL,
    month            SMALLINT     NOT NULL,
    month_name       VARCHAR(10)  NOT NULL,
    is_crisis_period BOOLEAN      DEFAULT FALSE,
    PRIMARY KEY (date_key)
)
DISTSTYLE ALL
SORTKEY (date_key);

-- Populate dim_date 2015-01-01 → 2030-12-01 using generate_series
-- FIX vs original: stl_scan is a superuser-only system table and fails for admin users.
-- generate_series() works for all Redshift users.
INSERT INTO dim_date
SELECT
    DATEADD(month, gs.n, DATE '2015-01-01')                              AS date_key,
    EXTRACT(YEAR    FROM DATEADD(month, gs.n, DATE '2015-01-01'))::SMALLINT AS year,
    EXTRACT(QUARTER FROM DATEADD(month, gs.n, DATE '2015-01-01'))::SMALLINT AS quarter,
    EXTRACT(MONTH   FROM DATEADD(month, gs.n, DATE '2015-01-01'))::SMALLINT AS month,
    TO_CHAR(DATEADD(month, gs.n, DATE '2015-01-01'), 'Mon')              AS month_name,
    CASE
        WHEN DATEADD(month, gs.n, DATE '2015-01-01')
             BETWEEN DATE '2022-01-01' AND DATE '2022-12-31'
        THEN TRUE
        ELSE FALSE
    END                                                                  AS is_crisis_period
FROM (SELECT ROW_NUMBER() OVER () - 1 AS n FROM generate_series(1, 192)) gs
-- 192 = 16 years × 12 months (2015 → 2030)
WHERE NOT EXISTS (SELECT 1 FROM dim_date LIMIT 1);
-- The WHERE clause prevents re-inserting if the table already has data


-- dim_fuel_type
-- Reference table for the 5 main fuel products sold in Sri Lanka.
CREATE TABLE IF NOT EXISTS dim_fuel_type (
    fuel_type_key VARCHAR(30)  NOT NULL,
    fuel_category VARCHAR(20)  NOT NULL,
    description   VARCHAR(150),
    PRIMARY KEY (fuel_type_key)
)
DISTSTYLE ALL;

-- Insert only if table is empty (idempotent)
INSERT INTO dim_fuel_type
SELECT * FROM (
    VALUES
    ('LP_95',         'PETROL',   'Octane 95 — premium petrol, Lanka IOC brand'),
    ('LP_92',         'PETROL',   'Octane 92 — standard petrol, most common grade'),
    ('AUTO_DIESEL',   'DIESEL',   'LAD — Lanka Auto Diesel, automotive use'),
    ('SUPER_DIESEL',  'DIESEL',   'LSD — Lanka Super Diesel, low-sulphur grade'),
    ('KEROSENE',      'KEROSENE', 'LK — Lanka Kerosene, domestic cooking use'),
    ('ILO_KEROSENE',  'KEROSENE', 'LIK — Illuminating Kerosene, lighting use'),
    ('FURNACE_800',   'FURNACE',  'FUR 800 — Furnace oil 800 cSt, industrial'),
    ('FURNACE_1500',  'FURNACE',  'FUR 1500 — Furnace oil 1500 cSt, industrial')
) AS v(fuel_type_key, fuel_category, description)
WHERE NOT EXISTS (SELECT 1 FROM dim_fuel_type LIMIT 1);


-- dim_crude_oil
-- Monthly Brent crude spot prices sourced from EIA API.
-- Loaded by the Glue ETL job on each pipeline run.
CREATE TABLE IF NOT EXISTS dim_crude_oil (
    period               DATE          NOT NULL,
    brent_usd_per_barrel DECIMAL(8,2)  NOT NULL,
    PRIMARY KEY (period)
)
DISTKEY (period)
SORTKEY (period);


-- dim_fx_rates
-- Monthly average USD/LKR exchange rates sourced from CBSL.
-- Loaded by the Glue ETL job on each pipeline run.
CREATE TABLE IF NOT EXISTS dim_fx_rates (
    period        DATE           NOT NULL,
    usd_lkr_rate  DECIMAL(10,4)  NOT NULL,
    PRIMARY KEY (period)
)
DISTKEY (period)
SORTKEY (period);


-- ═══════════════════════════════════════════════════════════════════════════
-- FACT TABLES
-- ═══════════════════════════════════════════════════════════════════════════

-- fact_fuel_prices
-- One row per (month, fuel_type). Central fact table of the star schema.
--
-- FIX vs original: removed GENERATED ALWAYS AS (crude_cost_pct).
-- Redshift does NOT support GENERATED ALWAYS AS computed columns.
-- crude_cost_pct is now computed in the Glue ETL job and stored as a plain column.
CREATE TABLE IF NOT EXISTS fact_fuel_prices (
    fact_id              BIGINT IDENTITY(1,1),
    period               DATE           NOT NULL,
    fuel_type            VARCHAR(30)    NOT NULL,
    price_lkr            DECIMAL(10,2)  NOT NULL,
    price_usd            DECIMAL(10,4),
    usd_lkr_rate         DECIMAL(10,4),
    brent_usd_per_barrel DECIMAL(8,2),
    crude_cost_pct       DECIMAL(5,2),    -- computed in Glue ETL: (Brent/159)/price_usd * 100
    PRIMARY KEY (fact_id)
)
DISTKEY (period)
SORTKEY (period, fuel_type);


-- fact_fuel_monthly_summary
-- Pre-aggregated summary for fast QuickSight dashboard queries.
-- Stores yearly/monthly averages grouped by fuel type.
CREATE TABLE IF NOT EXISTS fact_fuel_monthly_summary (
    year           SMALLINT       NOT NULL,
    month          SMALLINT       NOT NULL,
    fuel_type      VARCHAR(30)    NOT NULL,
    avg_price_lkr  DECIMAL(10,2),
    avg_brent_usd  DECIMAL(8,2),
    avg_usd_lkr    DECIMAL(10,4),
    record_count   INTEGER,
    PRIMARY KEY (year, month, fuel_type)
)
DISTSTYLE ALL
SORTKEY (year, month);


-- ═══════════════════════════════════════════════════════════════════════════
-- BI VIEW — used directly by Amazon QuickSight
-- ═══════════════════════════════════════════════════════════════════════════

-- vw_fuel_vs_crude
-- Joins fact_fuel_prices with dim_date and dim_fuel_type.
-- Adds month-over-month price change percentage (window function).
-- QuickSight connects to this view — not the raw fact table.
CREATE OR REPLACE VIEW vw_fuel_vs_crude AS
SELECT
    f.period,
    d.year,
    d.quarter,
    d.month_name,
    d.is_crisis_period,
    f.fuel_type,
    ft.fuel_category,
    f.price_lkr,
    f.price_usd,
    f.brent_usd_per_barrel,
    f.usd_lkr_rate,
    f.crude_cost_pct,
    -- Previous month price for the same fuel type
    LAG(f.price_lkr) OVER (
        PARTITION BY f.fuel_type
        ORDER BY f.period
    )                                                           AS prev_month_price_lkr,
    -- Month-over-month % price change
    ROUND(
        (
            f.price_lkr
            - LAG(f.price_lkr) OVER (PARTITION BY f.fuel_type ORDER BY f.period)
        )
        / NULLIF(
            LAG(f.price_lkr) OVER (PARTITION BY f.fuel_type ORDER BY f.period),
            0
          )
        * 100,
        2
    )                                                           AS mom_price_change_pct
FROM fact_fuel_prices   f
JOIN dim_date            d  ON f.period    = d.date_key
JOIN dim_fuel_type       ft ON f.fuel_type = ft.fuel_type_key;


-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFICATION QUERIES
-- Run these after the first pipeline execution to confirm data loaded.
-- ═══════════════════════════════════════════════════════════════════════════

-- Check row counts:
-- SELECT 'dim_crude_oil'             AS tbl, COUNT(*) AS rows FROM dim_crude_oil
-- UNION ALL SELECT 'dim_fx_rates',            COUNT(*) FROM dim_fx_rates
-- UNION ALL SELECT 'fact_fuel_prices',         COUNT(*) FROM fact_fuel_prices
-- UNION ALL SELECT 'fact_fuel_monthly_summary',COUNT(*) FROM fact_fuel_monthly_summary;

-- Preview the BI view:
-- SELECT * FROM vw_fuel_vs_crude ORDER BY period DESC LIMIT 20;