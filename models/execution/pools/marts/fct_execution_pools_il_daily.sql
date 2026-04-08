{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, pool_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'pools', 'daily']
    )
}}

{#- Model documentation in schema.yml -#}

SELECT
    t.day AS date,
    t.protocol,
    t.pool_address,
    CASE
        WHEN t.days_in_window < 3 THEN NULL
        WHEN t.tvl_usd_7d_avg IS NULL OR t.tvl_usd_7d_avg <= 0 THEN NULL
        ELSE (t.swap_flow_usd_7d - t.fees_usd_7d)
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
    FROM {{ ref('int_execution_pools_il_swap_flows_daily') }} s
) t
WHERE t.day < today()
