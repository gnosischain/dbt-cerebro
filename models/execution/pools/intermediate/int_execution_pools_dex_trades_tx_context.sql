{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'pools', 'trades', 'intermediate']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

SELECT DISTINCT
    t.block_timestamp,
    t.transaction_hash,
    lower(t.from_address) AS tx_from,
    lower(t.to_address)   AS tx_to
FROM {{ source('execution', 'transactions') }} t
WHERE t.transaction_hash IN (
    SELECT DISTINCT transaction_hash
    FROM {{ ref('int_execution_pools_dex_trades_raw') }}
    {% if start_month and end_month %}
    WHERE toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp') }}
    {% endif %}
)
{% if start_month and end_month %}
AND t.block_timestamp >= toDate('{{ start_month }}') - INTERVAL 1 DAY
AND t.block_timestamp <= toDate('{{ end_month }}') + INTERVAL 32 DAY
{% else %}
  {# Whole-month rebuild: the IN-list (trades_raw) returns COMPLETE months under
     insert_overwrite, so this transactions filter must cover the same months. A
     day-level (max-3) bound left most of the rebuilt month with NULL tx_from. #}
  {{ apply_monthly_incremental_filter('t.block_timestamp', 'block_timestamp', add_and=True) }}
{% endif %}
