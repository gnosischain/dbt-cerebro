{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_avatars_current', 'granularity:snapshot']
    )
}}

SELECT
    block_number,
    block_timestamp,
    transaction_hash,
    transaction_index,
    log_index,
    avatar_type,
    invited_by,
    avatar,
    token_id,
    name
FROM {{ ref('int_execution_circles_v2_avatars') }}
ORDER BY block_timestamp DESC
