{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'prices']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

WITH

trades AS (
    SELECT *
    FROM {{ ref('int_execution_pools_dex_trades') }}
    {% if start_month and end_month %}
    WHERE toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
    {% else %}
    {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp') }}
    {% endif %}
),

wrappers AS (
    SELECT wrapper_address, avatar, circles_type, symbol AS crc20_symbol
    FROM {{ ref('int_execution_circles_v2_wrapper_tokens') }}
)

SELECT
    t.block_number,
    t.block_timestamp,
    t.transaction_hash,
    t.log_index,
    t.pool_address,
    t.protocol,
    coalesce(wb.wrapper_address, ws.wrapper_address)        AS crc20_token,
    coalesce(wb.avatar,          ws.avatar)                 AS avatar,
    coalesce(wb.circles_type,    ws.circles_type)           AS circles_type,
    coalesce(wb.crc20_symbol,    ws.crc20_symbol)           AS crc20_symbol,
    if(wb.wrapper_address IS NOT NULL, t.token_sold_address,  t.token_bought_address) AS backing_token,
    if(wb.wrapper_address IS NOT NULL, t.token_sold_symbol,   t.token_bought_symbol)  AS backing_token_symbol,
    if(wb.wrapper_address IS NOT NULL, t.amount_bought, t.amount_sold)   AS crc_amount,
    if(wb.wrapper_address IS NOT NULL, t.amount_sold,   t.amount_bought) AS backing_amount,
    if(wb.wrapper_address IS NOT NULL, t.amount_bought, 0)               AS crc_bought_amount,
    if(ws.wrapper_address IS NOT NULL, t.amount_sold,   0)               AS crc_sold_amount,
    if(wb.wrapper_address IS NOT NULL, t.amount_sold,   t.amount_bought)
        / NULLIF(if(wb.wrapper_address IS NOT NULL, t.amount_bought, t.amount_sold), 0)
        AS price_in_backing,
    t.amount_usd
FROM trades t
LEFT JOIN wrappers wb ON wb.wrapper_address = t.token_bought_address
LEFT JOIN wrappers ws ON ws.wrapper_address = t.token_sold_address
WHERE (wb.wrapper_address IS NOT NULL OR ws.wrapper_address IS NOT NULL)
  AND t.amount_bought > 1e-4
  AND t.amount_sold   > 1e-4
