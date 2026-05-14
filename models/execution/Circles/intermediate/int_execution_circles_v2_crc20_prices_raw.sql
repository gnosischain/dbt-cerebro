{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        unique_key='(block_timestamp, transaction_hash, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'prices']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

-- Balancer V2 trades are already captured via the single vault contract and need no
-- whitelist entry. Uniswap V3 CRC20 pools are covered by the 4 entries added to
-- contracts_whitelist.csv. Both flow through int_execution_pools_dex_trades here.

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
    -- Determine which leg is the CRC20 token and which is the backing token
    coalesce(wb.wrapper_address, ws.wrapper_address)        AS crc20_token,
    coalesce(wb.avatar,          ws.avatar)                 AS avatar,
    coalesce(wb.circles_type,    ws.circles_type)           AS circles_type,
    coalesce(wb.crc20_symbol,    ws.crc20_symbol)           AS crc20_symbol,
    if(wb.wrapper_address IS NOT NULL, t.token_sold_address,  t.token_bought_address) AS backing_token,
    if(wb.wrapper_address IS NOT NULL, t.token_sold_symbol,   t.token_bought_symbol)  AS backing_token_symbol,
    -- Amounts
    if(wb.wrapper_address IS NOT NULL, t.amount_bought, t.amount_sold)   AS crc_amount,
    if(wb.wrapper_address IS NOT NULL, t.amount_sold,   t.amount_bought) AS backing_amount,
    -- Executed price: backing units per 1 CRC
    if(wb.wrapper_address IS NOT NULL, t.amount_sold,   t.amount_bought)
        / NULLIF(if(wb.wrapper_address IS NOT NULL, t.amount_bought, t.amount_sold), 0)
        AS price_in_backing,
    -- USD value of the trade (computed in int_execution_pools_dex_trades via backing token price)
    t.amount_usd
FROM trades t
LEFT JOIN wrappers wb ON wb.wrapper_address = t.token_bought_address
LEFT JOIN wrappers ws ON ws.wrapper_address = t.token_sold_address
WHERE (wb.wrapper_address IS NOT NULL OR ws.wrapper_address IS NOT NULL)
  AND t.amount_bought > 0
  AND t.amount_sold   > 0
