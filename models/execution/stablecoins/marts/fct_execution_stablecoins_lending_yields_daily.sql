{{
    config(
        materialized='table',
        tags=['production','execution','stablecoins','yields']
    )
}}

WITH

-- Filter Aave yields for stablecoins only
aave_yields AS (
    SELECT 
        y.date,
        y.token_address,
        y.symbol,
        y.protocol,
        y.apy_daily
    FROM {{ ref('int_execution_yields_aave_daily') }} y
    INNER JOIN {{ ref('tokens_whitelist') }} w
        ON lower(w.address) = y.token_address
    WHERE w.token_class = 'STABLECOIN'
),

-- Filter Spark yields for stablecoins only
spark_yields AS (
    SELECT 
        y.date,
        y.token_address,
        y.symbol,
        y.protocol,
        y.apy_daily
    FROM {{ ref('int_execution_yields_spark_daily') }} y
    INNER JOIN {{ ref('tokens_whitelist') }} w
        ON lower(w.address) = y.token_address
    WHERE w.token_class = 'STABLECOIN'
),

-- Filter Agave yields for stablecoins only
agave_yields AS (
    SELECT 
        y.date,
        y.token_address,
        y.symbol,
        y.protocol,
        y.apy_daily
    FROM {{ ref('int_execution_yields_agave_daily') }} y
    INNER JOIN {{ ref('tokens_whitelist') }} w
        ON lower(w.address) = y.token_address
    WHERE w.token_class = 'STABLECOIN'
),

-- Union all protocols
all_yields AS (
    SELECT * FROM aave_yields
    UNION ALL
    SELECT * FROM spark_yields
    UNION ALL
    SELECT * FROM agave_yields
),

-- Get date range and unique protocol+token combinations
date_range AS (
    SELECT 
        MIN(date) AS min_date,
        MAX(date) AS max_date
    FROM all_yields
),

protocol_tokens AS (
    SELECT DISTINCT
        protocol,
        token_address,
        symbol
    FROM all_yields
),

-- Create calendar: all dates for each protocol+token combination
calendar AS (
    SELECT
        pt.protocol,
        pt.token_address,
        pt.symbol,
        addDays(dr.min_date, offset) AS date
    FROM protocol_tokens pt
    CROSS JOIN date_range dr
    ARRAY JOIN range(toUInt64(dateDiff('day', dr.min_date, dr.max_date) + 1)) AS offset
),

-- Forward fill: use last known value for missing days
filled_yields AS (
    SELECT
        c.date,
        c.protocol,
        c.token_address,
        c.symbol,
        -- Forward fill: get the last known apy_daily value up to this date
        argMax(ay.apy_daily, ay.date) AS apy_daily
    FROM calendar c
    LEFT JOIN all_yields ay
        ON ay.protocol = c.protocol
        AND ay.token_address = c.token_address
        AND ay.date <= c.date
    GROUP BY c.date, c.protocol, c.token_address, c.symbol
),

-- Calculate moving averages per protocol + token combination on filled data
with_ma AS (
    SELECT
        date,
        protocol,
        token_address,
        symbol,
        apy_daily,
        ROUND(
            avg(apy_daily) OVER (
                PARTITION BY protocol, token_address 
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 2
        ) AS apy_7DMA,
        ROUND(
            avg(apy_daily) OVER (
                PARTITION BY protocol, token_address 
                ORDER BY date 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ), 2
        ) AS apy_30DMA
    FROM filled_yields
    WHERE apy_daily IS NOT NULL  -- Only include rows where we have at least one value
)

SELECT
    date,
    protocol,
    token_address,
    symbol,
    apy_daily,
    apy_7DMA,
    apy_30DMA
FROM with_ma
ORDER BY date, protocol, token_address

