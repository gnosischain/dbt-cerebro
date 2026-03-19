{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    engine='ReplacingMergeTree()',
    order_by='address_hash',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','transactions']
  )
}}

{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

{% set txn_pre_filter %}
    block_timestamp < today()
    AND from_address IS NOT NULL
    AND success = 1
    {% if start_month and end_month %}
      AND toStartOfMonth(block_timestamp) >= toDate('{{ start_month }}')
      AND toStartOfMonth(block_timestamp) <= toDate('{{ end_month }}')
    {% elif is_incremental() %}
      AND toStartOfMonth(block_timestamp) >= (
        SELECT toStartOfMonth(max(first_seen_date))
        FROM {{ this }}
      )
    {% endif %}
{% endset %}

WITH deduped_transactions AS (
    SELECT
        block_timestamp,
        CONCAT('0x', from_address) AS from_address
    FROM (
        {{ dedup_source(
            source_ref=source('execution', 'transactions'),
            partition_by='block_number, transaction_index',
            columns='block_timestamp, from_address',
            pre_filter=txn_pre_filter
        ) }}
    )
),

new_addresses AS (
    SELECT
        cityHash64(lower(from_address)) AS address_hash,
        min(toDate(block_timestamp))    AS first_seen_date
    FROM deduped_transactions
    GROUP BY address_hash
)

SELECT
    address_hash,
    first_seen_date
FROM new_addresses
