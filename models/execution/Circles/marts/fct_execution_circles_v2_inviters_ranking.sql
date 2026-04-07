{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='rank',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2', 'avatars']
    )
}}

SELECT
    invited_by,
    count() AS invite_count,
    min(block_timestamp) AS first_invite_ts,
    max(block_timestamp) AS last_invite_ts,
    row_number() OVER (ORDER BY count() DESC) AS rank
FROM {{ ref('int_execution_circles_v2_avatars') }}
WHERE avatar_type = 'Human'
  AND invited_by IS NOT NULL
  AND invited_by != {{ circles_zero_address() }}
GROUP BY invited_by
ORDER BY invite_count DESC
