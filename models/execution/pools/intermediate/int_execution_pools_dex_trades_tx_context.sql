{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash)',
        unique_key='(block_timestamp, transaction_hash)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'pools', 'trades', 'intermediate']
    )
}}

{#- Model documentation in schema.yml -#}

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
{% elif is_incremental() %}
AND t.block_timestamp >= (
    SELECT addDays(max(toDate(block_timestamp)), -3)
    FROM {{ this }}
)
{% endif %}
