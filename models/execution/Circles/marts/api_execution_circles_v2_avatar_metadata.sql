{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_avatar_metadata', 'granularity:snapshot']
    )
}}

SELECT
    avatar,
    avatar_type,
    invited_by,
    name,
    token_id,
    block_timestamp AS registered_at
FROM {{ ref('int_execution_circles_v2_avatars') }}
WHERE avatar IS NOT NULL
