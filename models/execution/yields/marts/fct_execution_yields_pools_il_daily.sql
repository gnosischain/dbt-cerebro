{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, pool_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'yields', 'pools', 'daily']
    )
}}

{#-
  Pool-level Loss Versus Rebalancing (LVR) from actual swap flows.

  LVR measures the cost LPs pay to arbitrageurs — the value extracted via
  swap rebalancing. Sign convention: negative = loss (arbitrage extracted value),
  positive = gain (swaps were favorable to the pool).

  Swap events already embed the concentrated-liquidity math executed by the
  pool contract, so this captures V3 amplification without per-position tracking.

  Each day's swap flows are valued at that day's token prices (consistent with
  how fees_usd is computed), then summed over the 7-day window.

  swap_flow_usd_d  =  amount0_d / 10^dec0 × P0_d  +  amount1_d / 10^dec1 × P1_d
  swap_flow_usd_7d =  Σ(swap_flow_usd_d)  over 7-day window
  lvr_apr_7d       =  -(swap_flow_usd_7d / avg_tvl_7d) × (365 / 7) × 100
-#}

SELECT
    t.day AS date,
    t.protocol,
    t.pool_address,
    CASE
        WHEN t.days_in_window < 3 THEN NULL
        WHEN t.tvl_usd_7d_avg IS NULL OR t.tvl_usd_7d_avg <= 0 THEN NULL
        ELSE -t.swap_flow_usd_7d
             / t.tvl_usd_7d_avg * (365.0 / 7.0) * 100.0
    END AS lvr_apr_7d
FROM (
    SELECT
        s.day,
        s.protocol,
        s.pool_address,
        sum(
            (s.swap_amount0_raw / pow(10, s.decimals0)) * s.price0_usd
            + (s.swap_amount1_raw / pow(10, s.decimals1)) * s.price1_usd
        ) OVER (
            PARTITION BY s.protocol, s.pool_address
            ORDER BY s.day
            RANGE BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS swap_flow_usd_7d,
        avg(s.tvl_usd) OVER (
            PARTITION BY s.protocol, s.pool_address
            ORDER BY s.day
            RANGE BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS tvl_usd_7d_avg,
        sum(s.fees_usd_daily) OVER (
            PARTITION BY s.protocol, s.pool_address
            ORDER BY s.day
            RANGE BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS fees_usd_7d,
        count() OVER (
            PARTITION BY s.protocol, s.pool_address
            ORDER BY s.day
            RANGE BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS days_in_window
    FROM {{ ref('int_execution_yields_pools_il_swap_flows_daily') }} s
) t
WHERE t.day < today()
