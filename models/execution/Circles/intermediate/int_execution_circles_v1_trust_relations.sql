{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if var('start_month', none) else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(valid_from, truster, trustee)',
        unique_key='(transaction_hash, log_index)',
        partition_by='toStartOfMonth(valid_from)',
        settings={'allow_nullable_key': 1},
        tags=['dev', 'execution', 'circles_v1', 'trusts']
    )
}}

{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}
WITH ordered AS (
    SELECT
        block_number,
        block_timestamp,
        transaction_hash,
        transaction_index,
        log_index,
        truster,
        trustee,
        trust_limit,
        trust_value,
        updated_at,
        lead(toUnixTimestamp(block_timestamp)) OVER (
            PARTITION BY truster, trustee
            ORDER BY block_number, transaction_index, log_index
        ) AS next_update_ts
    FROM {{ ref('int_execution_circles_v1_trust_updates') }}
),
intervalized AS (
    SELECT
        block_number,
        block_timestamp,
        transaction_hash,
        transaction_index,
        log_index,
        truster,
        trustee,
        trust_limit,
        trust_value,
        updated_at,
        block_timestamp AS valid_from,
        if(next_update_ts > 0, toDateTime(next_update_ts), CAST(NULL AS Nullable(DateTime))) AS valid_to
    FROM ordered
)

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    truster,
    trustee,
    trust_value,
    trust_limit,
    valid_from,
    valid_to,
    toUInt8(trust_limit > 0) AS is_active,
    updated_at
FROM intervalized
WHERE valid_to IS NULL OR valid_to > valid_from
  {% if start_month and end_month %}
    AND toStartOfMonth(toDate(valid_from)) >= toDate('{{ start_month }}')
    AND toStartOfMonth(toDate(valid_from)) <= toDate('{{ end_month }}')
  {% else %}
    {{ apply_monthly_incremental_filter(source_field='valid_from', destination_field='valid_from', add_and=true) }}
  {% endif %}
