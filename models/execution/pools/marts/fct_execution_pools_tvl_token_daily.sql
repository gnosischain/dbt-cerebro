{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, pool_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'pools', 'daily']
    )
}}

{#- Model documentation in schema.yml -#}

WITH

pools AS (
    SELECT DISTINCT
        date AS date,
        protocol AS protocol,
        pool_address AS pool_address,
        pool AS pool,
        token AS ref_token
    FROM {{ ref('fct_execution_pools_daily') }}
    WHERE date < today()
),

pool_token_symbols AS (
    SELECT
        protocol,
        pool_address,
        arrayElement(tokens_sorted, 1) AS token0_symbol,
        arrayElement(tokens_sorted, 2) AS token1_symbol
    FROM (
        SELECT
            protocol,
            pool_address,
            arraySort(groupUniqArray(token)) AS tokens_sorted
        FROM {{ ref('int_execution_pools_balances_daily') }}
        WHERE token IS NOT NULL AND token != ''
        GROUP BY protocol, pool_address
    )
)

SELECT
    be.date AS date,
    be.protocol AS protocol,
    be.pool_address AS pool_address,
    be.token_address AS token_address,
    be.token AS series,
    be.reserve_amount AS token_amount,
    be.tvl_component_usd AS tvl_usd,
    be.tvl_component_usd / nullIf(p0.price_usd, 0) AS tvl_in_token0,
    be.tvl_component_usd / nullIf(p1.price_usd, 0) AS tvl_in_token1,
    pts.token0_symbol AS token0_symbol,
    pts.token1_symbol AS token1_symbol,
    po.ref_token AS ref_token,
    po.pool AS pool
FROM {{ ref('int_execution_pools_balances_daily') }} be
INNER JOIN pools po
    ON po.date = be.date
   AND po.protocol = be.protocol
   AND po.pool_address = be.pool_address
INNER JOIN pool_token_symbols pts
    ON pts.protocol = be.protocol
   AND pts.pool_address = be.pool_address
LEFT JOIN {{ ref('stg_pools__token_prices_daily') }} p0
    ON p0.token = pts.token0_symbol
   AND p0.date = be.date
LEFT JOIN {{ ref('stg_pools__token_prices_daily') }} p1
    ON p1.token = pts.token1_symbol
   AND p1.date = be.date
WHERE be.token IS NOT NULL
  AND be.token != ''
  AND be.date < today()
