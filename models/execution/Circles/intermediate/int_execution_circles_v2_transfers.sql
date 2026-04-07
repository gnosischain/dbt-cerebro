{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index, batch_index)',
        unique_key='(transaction_hash, log_index, batch_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'transfers']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}
-- Hub ERC-1155 transfers (always in demurrage units)
SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    batch_index,
    operator,
    from_address,
    to_address,
    token_address,
    amount_raw,
    amount_raw AS amount_demurraged_raw,
    'demurrage' AS unit_type,
    transfer_type
FROM {{ ref('int_execution_circles_v2_hub_transfers') }}
WHERE 1 = 1
  {% if start_month and end_month %}
    AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
    AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
  {% endif %}

UNION ALL

-- Wrapper ERC-20 transfers (static amounts converted to demurrage)
SELECT
    wt.block_number,
    wt.block_timestamp,
    wt.transaction_hash,
    wt.transaction_index,
    wt.log_index,
    0 AS batch_index,
    '' AS operator,
    wt.from_address,
    wt.to_address,
    wt.token_address,
    wt.amount_raw,
    if(w.circles_type = 1,
       toUInt256(
           multiplyDecimal(
               toDecimal256(wt.amount_raw, 0),
               {{ circles_demurrage_factor('1602720000', 'toUInt64(toUnixTimestamp(wt.block_timestamp))') }},
               0
           )
       ),
       wt.amount_raw
    ) AS amount_demurraged_raw,
    if(w.circles_type = 1, 'static', 'demurrage') AS unit_type,
    'CrcV2_ERC20WrapperTransfer' AS transfer_type
FROM {{ ref('int_execution_circles_v2_wrapper_transfers') }} wt
INNER JOIN {{ ref('int_execution_circles_v2_wrappers') }} w
    ON wt.token_address = w.wrapper_address
WHERE 1 = 1
  {% if start_month and end_month %}
    AND toStartOfMonth(toDate(wt.block_timestamp)) >= toDate('{{ start_month }}')
    AND toStartOfMonth(toDate(wt.block_timestamp)) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter(source_field='wt.block_timestamp', destination_field='block_timestamp', add_and=true) }}
  {% endif %}
