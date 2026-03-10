{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, pool_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['dev', 'execution', 'yields', 'pools', 'daily']
    )
}}

{#-
  Pool-level impermanent loss from actual swap rebalancing flows.

  IL = net value of swap flows (at row-date prices) minus fees earned.
  Swap events already embed the concentrated-liquidity math executed by the
  pool contract, so this captures V3 amplification without per-position tracking.

  swap_flow_value  =  Σ(amount0 × P0_today + amount1 × P1_today)   -- includes fees
  pure_IL          =  swap_flow_value − fees
  il_apr_7d        =  (pure_IL / avg_tvl_7d) × (365 / days) × 100
-#}

SELECT
    t.day AS date,
    t.protocol,
    t.pool_address,
    CASE
        WHEN t.days_in_window < 3 THEN NULL
        WHEN t.tvl_usd_7d_avg IS NULL OR t.tvl_usd_7d_avg <= 0 THEN NULL
        ELSE (
            (
                (t.swap_amount0_raw_7d / pow(10, t.decimals0)) * t.price0_usd
                + (t.swap_amount1_raw_7d / pow(10, t.decimals1)) * t.price1_usd
                - t.fees_usd_7d
            ) / t.tvl_usd_7d_avg
        ) * (365.0 / t.days_in_window) * 100.0
    END AS il_apr_7d
FROM (
    SELECT
        s.day,
        s.protocol,
        s.pool_address,
        s.decimals0,
        s.price0_usd,
        s.decimals1,
        s.price1_usd,
        sum(s.swap_amount0_raw) OVER (
            PARTITION BY s.protocol, s.pool_address
            ORDER BY s.day
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS swap_amount0_raw_7d,
        sum(s.swap_amount1_raw) OVER (
            PARTITION BY s.protocol, s.pool_address
            ORDER BY s.day
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS swap_amount1_raw_7d,
        avg(s.tvl_usd) OVER (
            PARTITION BY s.protocol, s.pool_address
            ORDER BY s.day
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS tvl_usd_7d_avg,
        sum(s.fees_usd_daily) OVER (
            PARTITION BY s.protocol, s.pool_address
            ORDER BY s.day
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS fees_usd_7d,
        count() OVER (
            PARTITION BY s.protocol, s.pool_address
            ORDER BY s.day
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS days_in_window
    FROM {{ ref('int_execution_yields_pools_il_swap_flows_daily') }} s
) t
WHERE t.day < today()
