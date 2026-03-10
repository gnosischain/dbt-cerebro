{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(day, protocol, pool_address)',
        partition_by='toStartOfMonth(day)',
        settings={'allow_nullable_key': 1},
        tags=['dev', 'execution', 'yields', 'pools', 'il', 'intermediate']
    )
}}

{#-
  Daily pool-level base data for IL computation: swap flows, fees, TVL, and
  token prices/decimals. Materialized to avoid deep CTE chains that trigger
  ClickHouse 25.10 query analyzer issues with window functions.
-#}

WITH

constants AS (
    SELECT
        toUInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967') AS max_int256,
        toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639936') AS two_256
),

v3_pool_meta AS (
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

    UNION ALL

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

token_prices_by_address AS (
    SELECT
        lower(w.address) AS token_address,
        coalesce(w.decimals, 18) AS decimals,
        toDate(p.date) AS day,
        toFloat64(p.price) AS price_usd
    FROM {{ ref('tokens_whitelist') }} w
    INNER JOIN {{ ref('int_execution_token_prices_daily') }} p
        ON p.symbol = w.symbol
    WHERE p.date < today()
      AND w.symbol IS NOT NULL
      AND w.symbol != ''
    GROUP BY lower(w.address), coalesce(w.decimals, 18), toDate(p.date), toFloat64(p.price)
),

pool_tvl_daily AS (
    SELECT
        toDate(b.date) AS day,
        b.protocol AS protocol,
        multiIf(
            startsWith(lower(b.pool_address), '0x'),
            lower(b.pool_address),
            concat('0x', lower(b.pool_address))
        ) AS pool_address,
        replaceAll(lower(b.pool_address), '0x', '') AS pool_address_no0x,
        sum(b.token_amount * coalesce(tp.price_usd, 0)) AS tvl_usd
    FROM {{ ref('int_execution_pools_balances_daily') }} b
    LEFT JOIN token_prices_by_address tp
        ON tp.token_address = lower(b.token_address)
       AND tp.day = toDate(b.date)
    WHERE b.protocol IN ('Uniswap V3', 'Swapr V3')
      AND b.date < today()
    GROUP BY toDate(b.date), b.protocol, pool_address, pool_address_no0x
),

uniswap_v3_swap_flows AS (
    SELECT
        toDate(toStartOfDay(e.block_timestamp)) AS day,
        'Uniswap V3' AS protocol,
        replaceAll(lower(e.contract_address), '0x', '') AS pool_address_no0x,
        if(
            toUInt256OrNull(e.decoded_params['amount0']) > (SELECT max_int256 FROM constants),
            -toInt256((SELECT two_256 FROM constants) - toUInt256OrNull(e.decoded_params['amount0'])),
            toInt256(toUInt256OrNull(e.decoded_params['amount0']))
        ) AS amount0,
        if(
            toUInt256OrNull(e.decoded_params['amount1']) > (SELECT max_int256 FROM constants),
            -toInt256((SELECT two_256 FROM constants) - toUInt256OrNull(e.decoded_params['amount1'])),
            toInt256(toUInt256OrNull(e.decoded_params['amount1']))
        ) AS amount1
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
        if(
            toUInt256OrNull(e.decoded_params['amount0']) > (SELECT max_int256 FROM constants),
            -toInt256((SELECT two_256 FROM constants) - toUInt256OrNull(e.decoded_params['amount0'])),
            toInt256(toUInt256OrNull(e.decoded_params['amount0']))
        ) AS amount0,
        if(
            toUInt256OrNull(e.decoded_params['amount1']) > (SELECT max_int256 FROM constants),
            -toInt256((SELECT two_256 FROM constants) - toUInt256OrNull(e.decoded_params['amount1'])),
            toInt256(toUInt256OrNull(e.decoded_params['amount1']))
        ) AS amount1
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
    FROM {{ ref('int_execution_yields_pools_fees_daily') }}
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
INNER JOIN v3_pool_meta m
    ON m.protocol = tvl.protocol
   AND m.pool_address_no0x = tvl.pool_address_no0x
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
