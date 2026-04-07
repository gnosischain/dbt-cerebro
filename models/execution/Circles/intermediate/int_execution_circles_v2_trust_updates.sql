{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, truster, trustee)',
        unique_key='(block_timestamp, truster, trustee)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'trusts']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}
SELECT
    block_number,
    block_timestamp,
    lower(transaction_hash) AS transaction_hash,
    transaction_index,
    log_index,
    lower(decoded_params['truster']) AS truster,
    lower(decoded_params['trustee']) AS trustee,
    fromUnixTimestamp(toUInt256OrZero(decoded_params['expiryTime'])) AS expiry_time
FROM {{ ref('contracts_circles_v2_Hub_events') }}
WHERE event_name = 'Trust'
  {% if start_month and end_month %}
    AND toStartOfMonth(toDate(block_timestamp)) >= toDate('{{ start_month }}')
    AND toStartOfMonth(toDate(block_timestamp)) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
  {% endif %}
