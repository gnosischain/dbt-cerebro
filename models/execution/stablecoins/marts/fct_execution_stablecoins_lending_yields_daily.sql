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

-- Calculate moving averages per protocol + token combination
-- Data is already dense from intermediate tables
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
    FROM all_yields
    WHERE apy_daily IS NOT NULL
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

