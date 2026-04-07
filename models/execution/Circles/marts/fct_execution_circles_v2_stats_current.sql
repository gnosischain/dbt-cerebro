{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='measure',
        settings={'allow_nullable_key': 1},
        tags=['production', 'execution', 'circles_v2']
    )
}}

SELECT 'avatar_count_v2' AS measure, toUInt64(count()) AS value
FROM {{ ref('int_execution_circles_v2_avatars') }}

UNION ALL

SELECT 'human_count_v2', toUInt64(countIf(avatar_type = 'Human'))
FROM {{ ref('int_execution_circles_v2_avatars') }}

UNION ALL

SELECT 'group_count_v2', toUInt64(countIf(avatar_type = 'Group'))
FROM {{ ref('int_execution_circles_v2_avatars') }}

UNION ALL

SELECT 'org_count_v2', toUInt64(countIf(avatar_type = 'Organization'))
FROM {{ ref('int_execution_circles_v2_avatars') }}

UNION ALL

SELECT 'active_trust_count_v2', toUInt64(count())
FROM {{ ref('fct_execution_circles_v2_trust_relations_current') }}

UNION ALL

SELECT 'token_count_v2', toUInt64(countDistinct(token_address))
FROM {{ ref('int_execution_circles_v2_balances_daily') }}
WHERE date = yesterday()

UNION ALL

SELECT 'wrapper_count_v2', toUInt64(count())
FROM {{ ref('int_execution_circles_v2_wrappers') }}
