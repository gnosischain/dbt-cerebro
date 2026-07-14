{{ config(materialized='view', tags=['production','execution','circles_v2','api:circles_v2_pool_explorer_liquidity_events','granularity:snapshot','tier1']) }}
-- Individual liquidity events (Mint = Add, Burn = Remove) for the main Uniswap V3
-- Circles pools, one row per event, with each token amount added/removed, the USD
-- value of the event, and the LP (position owner). Scoped by pool_address in the
-- Pool Explorer. Read from the raw decoded pool events (which carry amount0/amount1
-- and owner in a single row) and deduped on the event key; token symbols resolved
-- via the pool registry and prices via daily ASOF carry-forward (CRC legs from the
-- crc20 price model, stable legs from the oracle) — identical pricing to the
-- reserves model.
WITH
seed AS (
    SELECT lower(pool_address) AS pool_address
    FROM {{ ref('circles_liquidity_pools') }}
    WHERE protocol = 'Uniswap V3'
),
reg AS (
    SELECT pool_address_no0x, lower(token0_address) AS token0, lower(token1_address) AS token1
    FROM {{ ref('stg_pools__v3_pool_registry') }}
    WHERE protocol = 'Uniswap V3'
),
ev AS (
    SELECT
        lower(concat('0x', e.contract_address))                              AS pool_address,
        e.transaction_hash                                                   AS tx_hash,
        e.log_index                                                          AS log_index,
        any(e.block_timestamp)                                               AS ts,
        toDate(any(e.block_timestamp))                                       AS date,
        any(e.event_name)                                                    AS event_type,
        any(lower(e.decoded_params['owner']))                                AS lp,
        any(reg.token0)                                                      AS token0_address,
        any(reg.token1)                                                      AS token1_address,
        any(toFloat64(toUInt256OrZero(e.decoded_params['amount0'])) / 1e18)  AS amount0,
        any(toFloat64(toUInt256OrZero(e.decoded_params['amount1'])) / 1e18)  AS amount1
    FROM {{ ref('contracts_UniswapV3_Pool_events') }} e
    INNER JOIN reg ON reg.pool_address_no0x = lower(e.contract_address)
    WHERE lower(concat('0x', e.contract_address)) IN (SELECT pool_address FROM seed)
      AND e.event_name IN ('Mint', 'Burn')
    GROUP BY pool_address, tx_hash, log_index
),
prices AS (
    SELECT lower(crc20_token) AS token_address, date, price_median_usd AS price
    FROM {{ ref('api_execution_circles_v2_crc20_prices_daily') }}
    WHERE price_median_usd IS NOT NULL
      AND lower(crc20_token) IN ('0x78bab8d5ea6b72f8375cc21436857815210f7d02',
                                 '0xa0ea681f5685bfa6857d776b5acbf3d51bbecc9a')
    UNION ALL
    SELECT lower(token_address) AS token_address, date, price_usd AS price
    FROM {{ ref('int_execution_pools_balances_daily') }}
    WHERE price_usd IS NOT NULL
      AND lower(token_address) IN ('0xaf204776c7245bf4147c2612bf6e5972ee483701',
                                   '0x420ca0f9b9b604ce0fd9c18ef134c705e5fa3430')
),
prices_sorted AS (
    SELECT token_address, date, avg(price) AS price FROM prices GROUP BY token_address, date
),
-- Earliest observed price per token, used to value events that predate the
-- token's first market price (e.g. the pool-creation mint one day before the
-- first trade) rather than silently valuing that leg at $0.
first_price AS (
    SELECT token_address, argMin(price, date) AS price0 FROM prices_sorted GROUP BY token_address
),
legs AS (
    SELECT tx_hash, log_index, date, token0_address AS token_address, amount0 AS amount FROM ev
    UNION ALL
    SELECT tx_hash, log_index, date, token1_address AS token_address, amount1 AS amount FROM ev
),
legs_asof AS (
    SELECT l.tx_hash AS tx_hash, l.log_index AS log_index, l.token_address AS token_address,
           l.amount AS amount, p.price AS asof_price
    FROM legs l
    ASOF LEFT JOIN prices_sorted p
      ON p.token_address = l.token_address AND p.date <= l.date
),
legs_usd AS (
    SELECT la.tx_hash AS tx_hash, la.log_index AS log_index,
           sum(la.amount * coalesce(la.asof_price, fp.price0)) AS amount_usd
    FROM legs_asof la
    LEFT JOIN first_price fp ON fp.token_address = la.token_address
    GROUP BY la.tx_hash, la.log_index
)
SELECT
    ev.pool_address                                       AS pool_address,
    ev.ts                                                 AS ts,
    ev.tx_hash                                            AS tx_hash,
    multiIf(ev.event_type = 'Mint', 'Add', 'Remove')      AS event_kind,
    multiIf(ev.token0_address = '0xaf204776c7245bf4147c2612bf6e5972ee483701', 'sDAI',
            ev.token0_address = '0x420ca0f9b9b604ce0fd9c18ef134c705e5fa3430', 'EURe',
            ev.token0_address = '0x78bab8d5ea6b72f8375cc21436857815210f7d02', 's-gCRC',
            ev.token0_address = '0xa0ea681f5685bfa6857d776b5acbf3d51bbecc9a', 's-CBG',
            substring(ev.token0_address, 1, 8))           AS token0_symbol,
    ev.amount0                                            AS amount0,
    multiIf(ev.token1_address = '0xaf204776c7245bf4147c2612bf6e5972ee483701', 'sDAI',
            ev.token1_address = '0x420ca0f9b9b604ce0fd9c18ef134c705e5fa3430', 'EURe',
            ev.token1_address = '0x78bab8d5ea6b72f8375cc21436857815210f7d02', 's-gCRC',
            ev.token1_address = '0xa0ea681f5685bfa6857d776b5acbf3d51bbecc9a', 's-CBG',
            substring(ev.token1_address, 1, 8))           AS token1_symbol,
    ev.amount1                                            AS amount1,
    coalesce(u.amount_usd, 0)                             AS amount_usd,
    ev.lp                                                 AS lp
FROM ev
LEFT JOIN legs_usd u ON u.tx_hash = ev.tx_hash AND u.log_index = ev.log_index
