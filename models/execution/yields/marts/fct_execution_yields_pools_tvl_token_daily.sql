{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, protocol, pool_address, token_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['dev', 'execution', 'yields', 'pools', 'daily']
    )
}}

{#-
  Per-token TVL composition within pools, with server-side denomination.

  Three TVL columns are pre-computed:
    tvl_usd        – component TVL in USD
    tvl_in_token0  – component TVL denominated in pool's token0 (cross-rate)
    tvl_in_token1  – component TVL denominated in pool's token1 (cross-rate)

  NOTE: All CTE columns use explicit AS aliases to work around ClickHouse 25.10
  query analyzer bug where qualified names (e.g. b.date) are preserved during
  CTE inlining, making outer alias resolution fail.
-#}

WITH

pools AS (
    SELECT DISTINCT
        date AS date,
        protocol AS protocol,
        pool_address AS pool_address,
        pool AS pool,
        token AS ref_token
    FROM {{ ref('fct_execution_yields_pools_daily') }}
    WHERE date < today()
),

balances_base AS (
    SELECT
        toDate(b.date) AS date,
        b.protocol AS protocol,
        multiIf(
            startsWith(lower(b.pool_address), '0x'),
            lower(b.pool_address),
            concat('0x', lower(b.pool_address))
        ) AS pool_address,
        lower(b.token_address) AS token_address,
        b.token_amount AS token_amount
    FROM {{ ref('int_execution_pools_balances_daily') }} b
    WHERE b.date < today()
      AND b.protocol IN ('Uniswap V3', 'Swapr V3')
),

token_meta AS (
    SELECT
        lower(address) AS token_address,
        nullIf(upper(trimBoth(symbol)), '') AS token_symbol,
        decimals AS decimals,
        date_start AS date_start,
        date_end AS date_end
    FROM {{ ref('tokens_whitelist') }}
),

prices AS (
    SELECT
        toDate(date) AS date,
        nullIf(upper(trimBoth(symbol)), '') AS token_symbol,
        toFloat64(price) AS price_usd
    FROM {{ ref('int_execution_token_prices_daily') }}
    WHERE date < today()
),

uniswap_v3_pools AS (
    SELECT DISTINCT
        replaceAll(lower(decoded_params['pool']), '0x', '') AS pool_address_no0x,
        lower(decoded_params['token0']) AS token0_address,
        lower(decoded_params['token1']) AS token1_address,
        'Uniswap V3' AS protocol
    FROM {{ ref('contracts_UniswapV3_Factory_events') }}
    WHERE event_name = 'PoolCreated'
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['token0'] IS NOT NULL
      AND decoded_params['token1'] IS NOT NULL
),

swapr_v3_pools AS (
    SELECT DISTINCT
        replaceAll(lower(decoded_params['pool']), '0x', '') AS pool_address_no0x,
        lower(decoded_params['token0']) AS token0_address,
        lower(decoded_params['token1']) AS token1_address,
        'Swapr V3' AS protocol
    FROM {{ ref('contracts_Swapr_v3_AlgebraFactory_events') }}
    WHERE event_name = 'Pool'
      AND decoded_params['pool'] IS NOT NULL
      AND decoded_params['token0'] IS NOT NULL
      AND decoded_params['token1'] IS NOT NULL
),

v3_pool_meta AS (
    SELECT * FROM uniswap_v3_pools
    UNION ALL
    SELECT * FROM swapr_v3_pools
),

pool_token_symbols AS (
    SELECT
        m.protocol AS protocol,
        m.pool_address_no0x AS pool_address_no0x,
        tm0.token_symbol AS token0_symbol,
        tm1.token_symbol AS token1_symbol
    FROM v3_pool_meta m
    LEFT JOIN token_meta tm0
        ON tm0.token_address = m.token0_address
    LEFT JOIN token_meta tm1
        ON tm1.token_address = m.token1_address
),

balances_enriched AS (
    SELECT
        b.date AS date,
        b.pool_address AS pool_address,
        b.protocol AS protocol,
        b.token_address AS token_address,
        tm.token_symbol AS series,
        b.token_amount AS token_amount,
        b.token_amount * p.price_usd AS tvl_usd
    FROM balances_base b
    LEFT JOIN token_meta tm
      ON tm.token_address = b.token_address
     AND b.date >= toDate(tm.date_start)
     AND (tm.date_end IS NULL OR b.date < toDate(tm.date_end))
    LEFT JOIN prices p
      ON p.token_symbol = tm.token_symbol
     AND p.date = b.date
    WHERE tm.token_symbol IS NOT NULL
      AND tm.token_symbol != ''
)

SELECT
    be.date AS date,
    be.protocol AS protocol,
    be.pool_address AS pool_address,
    be.token_address AS token_address,
    be.series AS series,
    be.token_amount AS token_amount,
    be.tvl_usd AS tvl_usd,
    be.tvl_usd / nullIf(p0.price_usd, 0) AS tvl_in_token0,
    be.tvl_usd / nullIf(p1.price_usd, 0) AS tvl_in_token1,
    pts.token0_symbol AS token0_symbol,
    pts.token1_symbol AS token1_symbol,
    po.ref_token AS ref_token,
    po.pool AS pool
FROM balances_enriched be
INNER JOIN pools po
    ON po.date = be.date
   AND po.protocol = be.protocol
   AND po.pool_address = be.pool_address
INNER JOIN pool_token_symbols pts
    ON pts.protocol = be.protocol
   AND pts.pool_address_no0x = replaceAll(be.pool_address, '0x', '')
LEFT JOIN prices p0
    ON p0.token_symbol = pts.token0_symbol
   AND p0.date = be.date
LEFT JOIN prices p1
    ON p1.token_symbol = pts.token1_symbol
   AND p1.date = be.date
WHERE be.date < today()
