{{
  config(
    materialized='table',
    tags=['production','execution','circles_v2','api:circles_v2_pools_reserves_token_daily','granularity:daily']
  )
}}

-- Daily per-(pool, token) reserve + USD value for the main Circles DEX pools.
-- Base model behind the TVL rollup, the Reserves-over-time chart and the reserves card.
--   * Uniswap V3 pools: reserves = cumulative net token flow from event deltas
--     (stg_pools__uniswap_v3_events, fresh view; matches on-chain balanceOf within a few %).
--     The shared balances model is NOT used for UV3 here because it mis-handles Burn events
--     for some of these pools (e.g. 0x0967 computes a negative balance).
--   * Balancer V3 pool: reserves from the shared vault-tracked balances (accurate).
-- Prices via daily ASOF carry-forward: CRC legs (s-gCRC/s-CBG) from the crc20 price model,
-- stable legs (sDAI/EURe) from the oracle. Current incomplete day excluded.

WITH
seed AS (
    SELECT lower(pool_address) AS pool_address, label, protocol FROM {{ ref('circles_liquidity_pools') }}
),
reg AS (
    SELECT pool_address_no0x, lower(token0_address) AS token0, lower(token1_address) AS token1
    FROM {{ ref('stg_pools__v3_pool_registry') }}
    WHERE protocol = 'Uniswap V3'
),
uv3_delta AS (
    SELECT
        toDate(e.block_timestamp)                                     AS date,
        lower(concat('0x', e.pool_address))                           AS pool_address,
        multiIf(e.token_position = 'token0', reg.token0, reg.token1)  AS token_address,
        sum(toFloat64(e.delta_amount_raw)) / 1e18                     AS delta
    FROM {{ ref('stg_pools__uniswap_v3_events') }} e
    INNER JOIN reg ON reg.pool_address_no0x = e.pool_address
    WHERE lower(concat('0x', e.pool_address)) IN (SELECT pool_address FROM seed WHERE protocol = 'Uniswap V3')
      AND e.block_timestamp < today()
    GROUP BY date, pool_address, token_address
),
uv3_span AS (
    SELECT pool_address, token_address, min(date) AS first_day FROM uv3_delta GROUP BY pool_address, token_address
),
uv3_spine AS (
    SELECT pool_address, token_address, first_day + toIntervalDay(n) AS date
    FROM uv3_span ARRAY JOIN range(0, toUInt32(today() - first_day)) AS n
),
uv3_reserves AS (
    SELECT
        sp.pool_address, sp.token_address, sp.date,
        sum(coalesce(d.delta, 0)) OVER (
            PARTITION BY sp.pool_address, sp.token_address
            ORDER BY sp.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS reserve
    FROM uv3_spine sp
    LEFT JOIN uv3_delta d
      ON d.pool_address = sp.pool_address AND d.token_address = sp.token_address AND d.date = sp.date
),
bal_reserves AS (
    SELECT lower(pool_address) AS pool_address, lower(token_address) AS token_address, date,
           reserve_amount AS reserve
    FROM {{ ref('int_execution_pools_balances_daily') }}
    WHERE protocol = 'Balancer V3'
      AND lower(pool_address) IN (SELECT pool_address FROM seed WHERE protocol = 'Balancer V3')
      AND date < today()
),
all_reserves AS (
    SELECT pool_address, token_address, date, reserve FROM uv3_reserves
    UNION ALL
    SELECT pool_address, token_address, date, reserve FROM bal_reserves
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
-- Earliest observed price per token, used to value reserve days that predate the
-- token's first market price (e.g. the pool's first day, before its first trade)
-- rather than leaving that leg unpriced.
first_price AS (
    SELECT token_address, argMin(price, date) AS price0 FROM prices_sorted GROUP BY token_address
),
reserves_priced AS (
    SELECT
        r.date          AS date,
        r.pool_address  AS pool_address,
        r.token_address AS token_address,
        r.reserve       AS reserve,
        p.price         AS asof_price
    FROM all_reserves r
    ASOF LEFT JOIN prices_sorted p
      ON p.token_address = r.token_address AND p.date <= r.date
)
SELECT
    rp.date                                                                       AS date,
    s.label                                                                       AS pool,
    rp.pool_address                                                               AS pool_address,
    rp.token_address                                                              AS token_address,
    multiIf(rp.token_address = '0xaf204776c7245bf4147c2612bf6e5972ee483701', 'sDAI',
            rp.token_address = '0x420ca0f9b9b604ce0fd9c18ef134c705e5fa3430', 'EURe',
            rp.token_address = '0x78bab8d5ea6b72f8375cc21436857815210f7d02', 's-gCRC',
            rp.token_address = '0xa0ea681f5685bfa6857d776b5acbf3d51bbecc9a', 's-CBG',
            substring(rp.token_address, 1, 8))                                    AS token_symbol,
    rp.reserve                                                                    AS reserve,
    coalesce(rp.asof_price, fp.price0)                                            AS price_usd,
    rp.reserve * coalesce(rp.asof_price, fp.price0)                               AS tvl_usd
FROM reserves_priced rp
INNER JOIN seed s ON s.pool_address = rp.pool_address
LEFT JOIN first_price fp ON fp.token_address = rp.token_address
