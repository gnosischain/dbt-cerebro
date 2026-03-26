{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        engine='ReplacingMergeTree()',
        order_by='(block_timestamp, version, avatar)',
        unique_key='(version, transaction_hash, log_index)',
        partition_by='toStartOfMonth(block_timestamp)',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles', 'avatars']
    )
}}

SELECT * FROM {{ ref('int_execution_circles_v1_avatars') }}
WHERE 1 = 1
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
UNION ALL
SELECT * FROM {{ ref('int_execution_circles_v2_avatars') }}
WHERE 1 = 1
  {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='block_timestamp', add_and=true) }}
