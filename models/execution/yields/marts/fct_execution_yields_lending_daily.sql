{{
    config(
        materialized='table',
        tags=['production','execution','yields','lending']
    )
}}

WITH

-- Get all Aave yields (no token_class filter)
aave_yields AS (
    SELECT 
        y.date,
        y.token_address,
        y.symbol,
        y.protocol,
        y.apy_daily,
        w.token_class
    FROM {{ ref('int_execution_yields_aave_daily') }} y
    INNER JOIN {{ ref('tokens_whitelist') }} w
        ON lower(w.address) = y.token_address
),

-- Get all Spark yields (no token_class filter)
spark_yields AS (
    SELECT 
        y.date,
        y.token_address,
        y.symbol,
        y.protocol,
        y.apy_daily,
        w.token_class
    FROM {{ ref('int_execution_yields_spark_daily') }} y
    INNER JOIN {{ ref('tokens_whitelist') }} w
        ON lower(w.address) = y.token_address
),

-- Get all Agave yields (no token_class filter)
agave_yields AS (
    SELECT 
        y.date,
        y.token_address,
        y.symbol,
        y.protocol,
        y.apy_daily,
        w.token_class
    FROM {{ ref('int_execution_yields_agave_daily') }} y
    INNER JOIN {{ ref('tokens_whitelist') }} w
        ON lower(w.address) = y.token_address
),

-- Union all protocols
all_yields AS (
    SELECT * FROM aave_yields
    UNION ALL
    SELECT * FROM spark_yields
    UNION ALL
    SELECT * FROM agave_yields
),

-- Calculate moving averages per protocol + token combination
-- Data is already dense from intermediate tables
with_ma AS (
    SELECT
        date,
        protocol,
        token_address,
        symbol,
        token_class,
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
    FROM all_yields
    WHERE apy_daily IS NOT NULL
)

SELECT
    date,
    protocol,
    token_address,
    symbol,
    token_class,
    apy_daily,
    apy_7DMA,
    apy_30DMA
FROM with_ma
ORDER BY date, protocol, token_address
