{{ 
    config(
        materialized='incremental',
        incremental_strategy='append',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, transaction_hash, log_index)',
        unique_key='(block_number, transaction_index, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'avatars']
    )
}}

SELECT
    block_number,
    block_timestamp,
    lower(transaction_hash) AS transaction_hash,
    transaction_index,
    log_index,
    1 AS version,
    CASE
        WHEN event_name = 'Signup' THEN 'Human'
        WHEN event_name = 'OrganizationSignup' THEN 'Org'
        ELSE 'Unknown'
    END AS avatar_type,
    CAST(NULL AS Nullable(String)) AS invited_by,
    lower(
        CASE
            WHEN event_name = 'Signup' THEN decoded_params['user']
            ELSE decoded_params['organization']
        END
    ) AS avatar,
    lower(
        CASE
            WHEN event_name = 'Signup' THEN decoded_params['token']
            ELSE NULL
        END
    ) AS token_id,
    CAST(NULL AS Nullable(String)) AS name,
    CAST(NULL AS Nullable(String)) AS cid_v0_digest,
    event_name AS source_event_name
FROM {{ ref('contracts_circles_v1_Hub_events') }}
WHERE
    event_name IN ('Signup', 'OrganizationSignup')
    {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
