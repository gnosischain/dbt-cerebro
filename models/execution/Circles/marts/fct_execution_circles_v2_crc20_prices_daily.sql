{{
    config(
        materialized='table',
        engine='ReplacingMergeTree()',
        order_by='(date, crc20_token, backing_token, pool_address)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'prices']
    )
}}

SELECT
    toDate(block_timestamp)             AS date,
    crc20_token,
    backing_token,
    backing_token_symbol,
    pool_address,
    protocol,
    avatar,
    circles_type,
    crc20_symbol,
    avg(price_in_backing)               AS price_avg_in_backing,
    median(price_in_backing)            AS price_median_in_backing,
    quantile(0.1)(price_in_backing)     AS price_p10_in_backing,
    quantile(0.9)(price_in_backing)     AS price_p90_in_backing,
    sum(crc_amount)                     AS crc_volume,
    sum(crc_bought_amount)              AS crc_bought_volume,
    sum(crc_sold_amount)                AS crc_sold_volume,
    count()                             AS trade_count,
    -- USD price derived directly from amount_usd / crc_amount; no extra ASOF join needed
    -- because int_execution_pools_dex_trades already resolved the backing token price.
    avg(amount_usd / NULLIF(crc_amount, 0))    AS price_avg_usd,
    median(amount_usd / NULLIF(crc_amount, 0)) AS price_median_usd
FROM {{ ref('int_execution_circles_v2_crc20_prices_raw') }}
GROUP BY
    date, crc20_token, backing_token, backing_token_symbol,
    pool_address, protocol, avatar, circles_type, crc20_symbol
