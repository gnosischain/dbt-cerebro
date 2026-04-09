{{ config(materialized='view') }}

{#- Thin wrapper that maps CoW Protocol trades to the standard dex trade schema
    used by int_execution_pools_dex_trades_raw. Drops CoW-specific columns
    (fee_amount_raw, order_uid) to match the union interface. -#}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    log_index,
    protocol,
    pool_address,
    token_bought_address,
    amount_bought_raw,
    token_sold_address,
    amount_sold_raw - fee_amount_raw                                                 AS amount_sold_raw,
    taker
FROM {{ ref('stg_cow__trades') }}
{% if start_month and end_month %}
WHERE toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
  AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
{% endif %}
