{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'tier1',
              'api:circles_v2_relative_trust_score_daily',
              'granularity:daily']
    )
}}

SELECT
    date,
    avatar,
    relative_trust_score,
    targets_reached,
    total_targets,
    penetration_rate
FROM {{ ref('int_execution_circles_v2_relative_trust_score_daily') }}
WHERE avatar IS NOT NULL
