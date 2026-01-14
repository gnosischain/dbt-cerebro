{{
    config(
        materialized='table',
        tags=['production','execution','yields','lending']
    )
}}

WITH

WITH

-- Get all Aave yields (no token_class filter)
aave_yields AS (
    SELECT 
        y.date,
        y.token_address,
        y.symbol,
        y.protocol,
        y.apy_daily,
        y.borrow_apy_variable_daily,
        w.token_class
    FROM {{ ref('int_execution_yields_aave_daily') }} y
    INNER JOIN {{ ref('tokens_whitelist') }} w
        ON lower(w.address) = y.token_address
),

-- Calculate moving averages and spread per protocol + token combination
with_ma AS (
    SELECT
        date,
        protocol,
        token_address,
        symbol,
        token_class,
        apy_daily,
        borrow_apy_variable_daily,
        -- Calculate spread: borrow APY - lend APY
        CASE 
            WHEN borrow_apy_variable_daily IS NOT NULL AND apy_daily IS NOT NULL
            THEN ROUND(borrow_apy_variable_daily - apy_daily, 2)
            ELSE NULL
        END AS spread_variable,
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
    FROM aave_yields
    WHERE apy_daily IS NOT NULL
)

SELECT
    date,
    protocol,
    token_address,
    symbol,
    token_class,
    apy_daily,
    borrow_apy_variable_daily,
    spread_variable,
    apy_7DMA,
    apy_30DMA
FROM with_ma
ORDER BY date, protocol, token_address
