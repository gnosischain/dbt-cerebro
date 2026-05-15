{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, crc20_token)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'prices']
    )
}}

-- Volume-weighted consolidated daily price per CRC20 token across all pools.
-- Tokens without any DEX trades on a given day have no row (no market price to report).
SELECT
    date,
    crc20_token,
    avatar,
    crc20_symbol                                                    AS symbol,
    circles_type,
    -- Volume-weighted average price in USD across all pools
    sum(price_avg_usd * crc_volume) / NULLIF(sum(crc_volume), 0)   AS price_vwap_usd,
    median(price_median_usd)                                        AS price_median_usd,
    sum(crc_volume)                                                 AS total_crc_volume,
    sum(crc_bought_volume)                                          AS total_crc_bought_volume,
    sum(crc_sold_volume)                                            AS total_crc_sold_volume,
    sum(trade_count)                                                AS trade_count,
    count()                                                         AS pool_count
FROM {{ ref('fct_execution_circles_v2_crc20_prices_daily') }}
GROUP BY date, crc20_token, avatar, crc20_symbol, circles_type
ORDER BY date DESC, total_crc_volume DESC
