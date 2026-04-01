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

{#-
  Pool-level daily metrics: TVL, fees, volume, swap count, and 7D trailing
  fee APR. Aggregates token-level TVL from the enriched model, joins fees
  and swap counts, then applies rolling window functions.

  Materialized as a table to avoid ClickHouse 25.10 query analyzer issues
  with window-function CTEs in downstream joins.
-#}

WITH

pool_tvl_daily AS (
    SELECT
        date,
        protocol,
        pool_address,
        sum(tvl_component_usd) AS tvl_usd
    FROM {{ ref('int_execution_pools_enriched_daily') }}
    GROUP BY date, protocol, pool_address
),

fees_volume_daily AS (
    SELECT
        date,
        protocol,
        pool_address,
        sum(fees_usd) AS fees_usd_daily,
        sum(volume_usd) AS volume_usd_daily
    FROM {{ ref('int_execution_pools_fees_daily') }}
    WHERE date < today()
    GROUP BY date, protocol, pool_address
),

swap_counts_daily AS (
    SELECT
        toDate(toStartOfDay(block_timestamp)) AS date,
        'Uniswap V3' AS protocol,
        concat('0x', replaceAll(lower(contract_address), '0x', '')) AS pool_address,
        count(*) AS swap_count
    FROM {{ ref('contracts_UniswapV3_Pool_events') }}
    WHERE event_name = 'Swap'
      AND block_timestamp < today()
    GROUP BY date, protocol, pool_address

    UNION ALL

    SELECT
        toDate(toStartOfDay(block_timestamp)) AS date,
        'Swapr V3' AS protocol,
        concat('0x', replaceAll(lower(contract_address), '0x', '')) AS pool_address,
        count(*) AS swap_count
    FROM {{ ref('contracts_Swapr_v3_AlgebraPool_events') }}
    WHERE event_name = 'Swap'
      AND block_timestamp < today()
    GROUP BY date, protocol, pool_address

    UNION ALL

    SELECT
        toDate(toStartOfDay(block_timestamp)) AS date,
        'Balancer V3' AS protocol,
        concat('0x', replaceAll(lower(decoded_params['pool']), '0x', '')) AS pool_address,
        count(*) AS swap_count
    FROM {{ ref('contracts_BalancerV3_Vault_events') }}
    WHERE event_name = 'Swap'
      AND decoded_params['pool'] IS NOT NULL
      AND block_timestamp < today()
    GROUP BY date, protocol, pool_address
),

pool_metrics_daily AS (
    SELECT
        t.date AS date,
        t.protocol AS protocol,
        t.pool_address AS pool_address,
        t.tvl_usd AS tvl_usd,
        coalesce(f.fees_usd_daily, 0) AS fees_usd_daily,
        coalesce(f.volume_usd_daily, 0) AS volume_usd_daily,
        coalesce(sc.swap_count, 0) AS swap_count,
        sum(coalesce(f.fees_usd_daily, 0)) OVER (
            PARTITION BY t.protocol, t.pool_address
            ORDER BY t.date
            RANGE BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS fees_usd_7d,
        avg(t.tvl_usd) OVER (
            PARTITION BY t.protocol, t.pool_address
            ORDER BY t.date
            RANGE BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS tvl_usd_7d_avg,
        count() OVER (
            PARTITION BY t.protocol, t.pool_address
            ORDER BY t.date
            RANGE BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS days_in_window
    FROM pool_tvl_daily t
    LEFT JOIN fees_volume_daily f
      ON f.date = t.date
     AND f.protocol = t.protocol
     AND f.pool_address = t.pool_address
    LEFT JOIN swap_counts_daily sc
      ON sc.date = t.date
     AND sc.protocol = t.protocol
     AND sc.pool_address = t.pool_address
)

SELECT
    date,
    protocol,
    pool_address,
    tvl_usd,
    fees_usd_daily,
    volume_usd_daily,
    swap_count,
    multiIf(
        protocol NOT IN ('Uniswap V3', 'Swapr V3', 'Balancer V3'), NULL,
        days_in_window < 3, NULL,
        tvl_usd_7d_avg <= 0, NULL,
        (fees_usd_7d / tvl_usd_7d_avg) * (365.0 / 7.0) * 100.0
    ) AS fee_apr_7d
FROM pool_metrics_daily
