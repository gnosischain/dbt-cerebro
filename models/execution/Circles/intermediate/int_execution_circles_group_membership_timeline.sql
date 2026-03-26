{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(valid_from, group_address, member)',
        unique_key='(transaction_hash, log_index, group_address, member)',
        partition_by='toStartOfMonth(valid_from)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'groups']
    )
}}

WITH latest_avatar_type AS (
    SELECT
        avatar,
        argMax(avatar_type, tuple(block_number, transaction_index, log_index)) AS avatar_type
    FROM {{ ref('int_execution_circles_avatars') }}
    GROUP BY 1
),
groups AS (
    SELECT DISTINCT group_address
    FROM {{ ref('int_execution_circles_group_registrations') }}
),
timeline AS (
    SELECT
        t.block_number,
        t.block_timestamp,
        t.transaction_hash,
        t.transaction_index,
        t.log_index,
        t.truster AS group_address,
        t.trustee AS member,
        t.trust_value,
        t.expiry_time,
        t.valid_from,
        t.valid_to
    FROM {{ ref('int_execution_circles_trust_relations') }} t
    INNER JOIN groups g
        ON t.truster = g.group_address
    WHERE t.version = 2
      AND t.is_active = 1
      {{ apply_monthly_incremental_filter(source_field='valid_from', destination_field='valid_from', add_and=true) }}
)

SELECT
    t.block_number,
    t.block_timestamp,
    t.transaction_hash,
    t.transaction_index,
    t.log_index,
    t.group_address,
    t.member,
    coalesce(a.avatar_type, 'Unknown') AS member_type,
    t.trust_value,
    t.expiry_time,
    t.valid_from,
    t.valid_to
FROM timeline t
LEFT JOIN latest_avatar_type a
    ON t.member = a.avatar
