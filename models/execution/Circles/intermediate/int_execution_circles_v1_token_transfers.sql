{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        unique_key='(block_number, transaction_index, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'transfers']
    )
}}

{% set logs_pre_filter %}
    topic0 = 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
    AND lower(concat('0x', address)) IN (
        SELECT token
        FROM {{ ref('int_execution_circles_tokens') }}
        WHERE version = 1
    )
    {{ apply_monthly_incremental_filter('block_timestamp', 'block_timestamp', add_and=true) }}
{% endset %}

WITH deduped_logs AS (
    SELECT
        block_number,
        transaction_index,
        log_index,
        CONCAT('0x', transaction_hash) AS transaction_hash,
        CONCAT('0x', address) AS address,
        topic1,
        topic2,
        data,
        block_timestamp
    FROM (
        {{ dedup_source(
            source_ref=source('execution', 'logs'),
            partition_by='block_number, transaction_index, log_index',
            columns='block_number, transaction_index, log_index, transaction_hash, address, topic1, topic2, data, block_timestamp',
            pre_filter=logs_pre_filter
        ) }}
    )
)

SELECT
    block_number,
    block_timestamp,
    lower(transaction_hash) AS transaction_hash,
    transaction_index,
    log_index,
    lower(address) AS token_address,
    lower(concat('0x', substring(topic1, 25, 40))) AS from_address,
    lower(concat('0x', substring(topic2, 25, 40))) AS to_address,
    reinterpretAsUInt256(reverse(unhex(replaceAll(data, '0x', '')))) AS amount_raw
FROM deduped_logs
