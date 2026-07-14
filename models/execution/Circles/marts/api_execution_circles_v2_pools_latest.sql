{{
  config(
    materialized='view',
    tags=['production','execution','circles_v2','api:circles_v2_pools_latest','granularity:latest']
  )
}}

-- One row per main Circles DEX pool: latest TVL plus trailing-7d volume, trades, traders and fees.
-- Backs the Liquidity tab KPI tiles and the pools leaderboard.
WITH p AS (
    SELECT lower(pool_address) AS pool_address, label FROM {{ ref('circles_liquidity_pools') }}
),
d AS (
    SELECT * FROM {{ ref('api_execution_circles_v2_pools_daily') }}
),
-- true (not double-counted) distinct traders over the trailing 7 days, computed from raw trades
tr AS (
    SELECT
        lower(t.pool_address)                    AS pool_address,
        uniqExact(coalesce(t.taker, t.tx_from))  AS traders_7d
    FROM {{ ref('int_execution_pools_dex_trades') }} t
    WHERE toDate(t.block_timestamp) >= today() - 7
      AND toDate(t.block_timestamp) <  today()
      AND lower(t.pool_address) IN (SELECT pool_address FROM p)
    GROUP BY lower(t.pool_address)
),
-- self-contained TVL: latest day of the daily reserves model (reserves from event
-- deltas x ASOF prices); single source of truth shared with the Reserves-over-time chart.
tv AS (
    SELECT pool_address, argMax(tvl_usd, date) AS tvl_usd
    FROM {{ ref('api_execution_circles_v2_pools_reserves_daily') }}
    GROUP BY pool_address
)
SELECT
    p.pool_address                              AS pool_address,
    p.label                                     AS pool,
    any(d.protocol)                             AS protocol,
    any(tv.tvl_usd)                             AS tvl_usd,
    sumIf(d.volume_usd, d.date >= today() - 7)  AS volume_7d,
    sumIf(d.swap_count, d.date >= today() - 7)  AS trades_7d,
    coalesce(any(tr.traders_7d), 0)             AS traders_7d,
    sumIf(d.fees_usd,   d.date >= today() - 7)  AS fees_7d
FROM p
LEFT JOIN d  ON d.pool_address  = p.pool_address
LEFT JOIN tr ON tr.pool_address = p.pool_address
LEFT JOIN tv ON tv.pool_address = p.pool_address
GROUP BY p.pool_address, p.label
