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

WITH

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
        toDate(p.date) AS day,
        toFloat64(p.price) AS price_usd
    FROM {{ ref('tokens_whitelist') }} w
    INNER JOIN {{ ref('int_execution_token_prices_daily') }} p
        ON p.symbol = w.symbol
    WHERE p.date < today()
      AND w.symbol IS NOT NULL
      AND w.symbol != ''
    GROUP BY lower(w.address), toDate(p.date), toFloat64(p.price)
),

pool_daily_prices AS (
    SELECT
        toDate(bal.date) AS day,
        bal.protocol AS protocol,
        multiIf(
            startsWith(lower(bal.pool_address), '0x'),
            lower(bal.pool_address),
            concat('0x', lower(bal.pool_address))
        ) AS pool_address,
        any(p0.price_usd) AS price0,
        any(p1.price_usd) AS price1
    FROM {{ ref('int_execution_pools_balances_daily') }} bal
    INNER JOIN v3_pool_meta m
        ON m.protocol = bal.protocol
       AND m.pool_address_no0x = replaceAll(lower(bal.pool_address), '0x', '')
    LEFT JOIN token_prices_by_address p0
        ON p0.token_address = m.token0_address
       AND p0.day = toDate(bal.date)
    LEFT JOIN token_prices_by_address p1
        ON p1.token_address = m.token1_address
       AND p1.day = toDate(bal.date)
    WHERE bal.date < today()
      AND bal.protocol IN ('Uniswap V3', 'Swapr V3')
    GROUP BY toDate(bal.date), bal.protocol, pool_address
)

SELECT
    day AS date,
    protocol,
    pool_address,
    CASE
        WHEN ratio_now IS NULL OR ratio_7d_ago IS NULL OR ratio_7d_ago = 0 THEN NULL
        ELSE (2.0 * sqrt(ratio_now / ratio_7d_ago)
              / (1.0 + ratio_now / ratio_7d_ago) - 1.0) * (365.0 / 7.0) * 100.0
    END AS il_apr_7d
FROM (
    SELECT
        day,
        protocol,
        pool_address,
        if(price1 > 0, price0 / price1, NULL) AS ratio_now,
        lagInFrame(if(price1 > 0, price0 / price1, NULL), 7) OVER (
            PARTITION BY protocol, pool_address
            ORDER BY day
        ) AS ratio_7d_ago
    FROM pool_daily_prices
)
WHERE day < today()
