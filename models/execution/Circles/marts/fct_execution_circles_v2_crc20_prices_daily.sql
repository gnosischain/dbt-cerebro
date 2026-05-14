{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(date, crc20_token, backing_token, pool_address)',
        unique_key='(date, crc20_token, backing_token, pool_address)',
        partition_by='toStartOfMonth(date)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'prices']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

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
    count()                             AS trade_count,
    -- USD price derived directly from amount_usd / crc_amount; no extra ASOF join needed
    -- because int_execution_pools_dex_trades already resolved the backing token price.
    avg(amount_usd / NULLIF(crc_amount, 0))    AS price_avg_usd,
    median(amount_usd / NULLIF(crc_amount, 0)) AS price_median_usd
FROM {{ ref('int_execution_circles_v2_crc20_prices_raw') }}
{% if start_month and end_month %}
WHERE toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
  AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
{% elif is_incremental() %}
WHERE toDate(block_timestamp) >= (SELECT addDays(max(date), -3) FROM {{ this }})
{% endif %}
GROUP BY
    date, crc20_token, backing_token, backing_token_symbol,
    pool_address, protocol, avatar, circles_type, crc20_symbol
