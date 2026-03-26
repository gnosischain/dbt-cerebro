{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(valid_from, version, truster, trustee)',
        unique_key='(version, transaction_hash, log_index)',
        partition_by='toStartOfMonth(valid_from)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'trusts']
    )
}}

WITH ordered AS (
    SELECT
        block_number,
        block_timestamp,
        transaction_hash,
        transaction_index,
        log_index,
        version,
        truster,
        trustee,
        trust_limit,
        expiry_time,
        trust_value,
        updated_at,
        lead(toUnixTimestamp(block_timestamp)) OVER (
            PARTITION BY version, truster, trustee
            ORDER BY block_number, transaction_index, log_index
        ) AS next_update_ts
    FROM {{ ref('int_execution_circles_trust_updates') }}
),
intervalized AS (
    SELECT
        block_number,
        block_timestamp,
        transaction_hash,
        transaction_index,
        log_index,
        version,
        truster,
        trustee,
        trust_limit,
        expiry_time,
        trust_value,
        updated_at,
        block_timestamp AS valid_from,
        multiIf(
            version = 1 AND next_update_ts > 0, toDateTime(next_update_ts),
            version = 1, CAST(NULL AS Nullable(DateTime)),
            expiry_time IS NOT NULL AND expiry_time > 0 AND next_update_ts > 0, toDateTime(least(toInt64(expiry_time), next_update_ts)),
            expiry_time IS NOT NULL AND expiry_time > 0, toDateTime(toInt64(expiry_time)),
            next_update_ts > 0, toDateTime(next_update_ts),
            CAST(NULL AS Nullable(DateTime))
        ) AS valid_to
    FROM ordered
)

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    version,
    truster,
    trustee,
    trust_value,
    trust_limit,
    expiry_time,
    valid_from,
    valid_to,
    if(
        version = 1,
        toUInt8(trust_limit > 0),
        toUInt8(expiry_time IS NOT NULL AND expiry_time > toUInt256(toUnixTimestamp(valid_from)))
    ) AS is_active,
    updated_at
FROM intervalized
WHERE valid_to IS NULL OR valid_to > valid_from
  {{ apply_monthly_incremental_filter(source_field='valid_from', destination_field='valid_from', add_and=true) }}
