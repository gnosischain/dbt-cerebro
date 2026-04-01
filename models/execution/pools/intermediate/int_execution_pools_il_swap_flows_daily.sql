{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(day, protocol, pool_address)',
        partition_by='toStartOfMonth(day)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'pools', 'il', 'intermediate']
    )
}}

{#-
  Daily pool-level base data for IL computation: swap flows, fees, TVL, and
  token prices/decimals. Materialized to avoid deep CTE chains that trigger
  ClickHouse 25.10 query analyzer issues with window functions.
-#}

WITH

token_prices_by_address AS (
    SELECT
        tm.token_address,
        coalesce(tm.decimals, 18) AS decimals,
        p.date AS day,
        p.price_usd
    FROM {{ ref('stg_pools__tokens_meta') }} tm
    INNER JOIN {{ ref('stg_pools__token_prices_daily') }} p
        ON p.token = tm.token
    WHERE tm.token IS NOT NULL
),

pool_tvl_daily AS (
    SELECT
        date AS day,
        protocol,
        pool_address,
        pool_address_no0x,
        sum(tvl_component_usd) AS tvl_usd
    FROM {{ ref('int_execution_pools_enriched_daily') }}
    WHERE protocol IN ('Uniswap V3', 'Swapr V3')
    GROUP BY date, protocol, pool_address, pool_address_no0x
),

uniswap_v3_swap_flows AS (
    SELECT
        toDate(toStartOfDay(e.block_timestamp)) AS day,
        'Uniswap V3' AS protocol,
        replaceAll(lower(e.contract_address), '0x', '') AS pool_address_no0x,
        toInt256OrNull(e.decoded_params['amount0']) AS amount0,
        toInt256OrNull(e.decoded_params['amount1']) AS amount1
    FROM {{ ref('contracts_UniswapV3_Pool_events') }} e
    WHERE e.event_name = 'Swap'
      AND e.block_timestamp < today()
      AND e.decoded_params['amount0'] IS NOT NULL
      AND e.decoded_params['amount1'] IS NOT NULL
),

swapr_v3_swap_flows AS (
    SELECT
        toDate(toStartOfDay(e.block_timestamp)) AS day,
        'Swapr V3' AS protocol,
        replaceAll(lower(e.contract_address), '0x', '') AS pool_address_no0x,
        toInt256OrNull(e.decoded_params['amount0']) AS amount0,
        toInt256OrNull(e.decoded_params['amount1']) AS amount1
    FROM {{ ref('contracts_Swapr_v3_AlgebraPool_events') }} e
    WHERE e.event_name = 'Swap'
      AND e.block_timestamp < today()
      AND e.decoded_params['amount0'] IS NOT NULL
      AND e.decoded_params['amount1'] IS NOT NULL
),

swap_flows_daily AS (
    SELECT
        day,
        protocol,
        pool_address_no0x,
        toFloat64(sum(amount0)) AS swap_amount0_raw,
        toFloat64(sum(amount1)) AS swap_amount1_raw
    FROM (
        SELECT * FROM uniswap_v3_swap_flows
        UNION ALL
        SELECT * FROM swapr_v3_swap_flows
    )
    WHERE amount0 IS NOT NULL AND amount1 IS NOT NULL
    GROUP BY day, protocol, pool_address_no0x
),

fees_daily AS (
    SELECT
        toDate(date) AS day,
        protocol,
        pool_address,
        sum(fees_usd) AS fees_usd_daily
    FROM {{ ref('int_execution_pools_fees_daily') }}
    WHERE date < today()
    GROUP BY toDate(date), protocol, pool_address
)

SELECT
    tvl.day AS day,
    tvl.protocol AS protocol,
    tvl.pool_address AS pool_address,
    tvl.tvl_usd AS tvl_usd,
    coalesce(sf.swap_amount0_raw, 0) AS swap_amount0_raw,
    coalesce(sf.swap_amount1_raw, 0) AS swap_amount1_raw,
    coalesce(f.fees_usd_daily, 0) AS fees_usd_daily,
    coalesce(tp0.decimals, 18) AS decimals0,
    tp0.price_usd AS price0_usd,
    coalesce(tp1.decimals, 18) AS decimals1,
    tp1.price_usd AS price1_usd
FROM pool_tvl_daily tvl
INNER JOIN {{ ref('stg_pools__v3_pool_registry') }} m
    ON m.protocol = tvl.protocol
   AND m.pool_address = tvl.pool_address
LEFT JOIN swap_flows_daily sf
    ON sf.day = tvl.day
   AND sf.protocol = tvl.protocol
   AND sf.pool_address_no0x = tvl.pool_address_no0x
LEFT JOIN fees_daily f
    ON f.day = tvl.day
   AND f.protocol = tvl.protocol
   AND f.pool_address = tvl.pool_address
LEFT JOIN token_prices_by_address tp0
    ON tp0.token_address = m.token0_address
   AND tp0.day = tvl.day
LEFT JOIN token_prices_by_address tp1
    ON tp1.token_address = m.token1_address
   AND tp1.day = tvl.day
WHERE tvl.day < today()
