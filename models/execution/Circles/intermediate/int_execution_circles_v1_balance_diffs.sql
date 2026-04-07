{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, account, token_address)',
        unique_key='(transaction_hash, log_index, batch_index, account, token_address, token_id)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v1', 'balances']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}
SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    batch_index,
    from_address AS account,
    to_address AS counterparty,
    token_id,
    token_address,
    -toInt256(amount_raw) AS delta_raw,
    transfer_type
FROM {{ ref('int_execution_circles_v1_transfers') }}
WHERE 1 = 1
  {% if start_month and end_month %}
    AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
    AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
  {% endif %}

UNION ALL

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    batch_index,
    to_address AS account,
    from_address AS counterparty,
    token_id,
    token_address,
    toInt256(amount_raw) AS delta_raw,
    transfer_type
FROM {{ ref('int_execution_circles_v1_transfers') }}
WHERE 1 = 1
  {% if start_month and end_month %}
    AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
    AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
  {% endif %}
