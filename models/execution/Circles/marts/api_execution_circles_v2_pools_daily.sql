{{
  config(
    materialized='view',
    tags=['production','execution','circles_v2','api:circles_v2_pools_daily','granularity:daily']
  )
}}

-- Daily liquidity/market metrics for the main Circles DEX pools (seed circles_liquidity_pools).
-- One row per (date, pool). Volume / swaps / fees come straight from the trade-level
-- int_execution_pools_dex_trades (so pools whose backing token has no USD price still
-- get USD volume via the priced leg); TVL is LEFT-joined from int_execution_pools_metrics_daily
-- and is therefore NULL for pools whose reserves can't be priced (unpriced CRC-backing token).
-- Uniswap V3 fee = swap volume x fee tier; all tracked pools are the 1% tier.
WITH p AS (
    SELECT lower(pool_address) AS pool_address, label FROM {{ ref('circles_liquidity_pools') }}
),
trades AS (
    SELECT
        toDate(block_timestamp) AS date,
        lower(pool_address)     AS pool_address,
        any(protocol)           AS protocol,
        sum(amount_usd)         AS volume_usd,
        count()                 AS swap_count
    FROM {{ ref('int_execution_pools_dex_trades') }}
    WHERE toDate(block_timestamp) < today()
    GROUP BY date, pool_address
)
SELECT
    t.date                          AS date,
    p.label                         AS pool,
    t.pool_address                  AS pool_address,
    t.protocol                      AS protocol,
    -- TVL is a reserve estimate from the shared pool-metrics model; on tiny pools
    -- it can briefly go slightly negative from timing/rounding — clamp to 0 (never < 0).
    if(m.tvl_usd < 0, 0, m.tvl_usd)  AS tvl_usd,
    t.volume_usd                    AS volume_usd,
    round(t.volume_usd * 0.01, 6)   AS fees_usd,
    t.swap_count                    AS swap_count
FROM trades t
INNER JOIN p ON p.pool_address = t.pool_address
LEFT JOIN {{ ref('int_execution_pools_metrics_daily') }} m
    ON lower(m.pool_address) = t.pool_address AND m.date = t.date
