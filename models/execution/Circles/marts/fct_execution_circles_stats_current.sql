{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'stats']
    )
}}

SELECT 'avatar_count_v1' AS measure, toUInt64(count()) AS value
FROM {{ ref('fct_execution_circles_avatars_current') }}
WHERE version = 1

UNION ALL

SELECT 'organization_count_v1' AS measure, toUInt64(count())
FROM {{ ref('fct_execution_circles_avatars_current') }}
WHERE version = 1
  AND avatar_type = 'Org'

UNION ALL

SELECT 'human_count_v1' AS measure, toUInt64(count())
FROM {{ ref('fct_execution_circles_avatars_current') }}
WHERE version = 1
  AND avatar_type = 'Human'

UNION ALL

SELECT 'avatar_count_v2' AS measure, toUInt64(count())
FROM {{ ref('fct_execution_circles_avatars_current') }}
WHERE version = 2

UNION ALL

SELECT 'organization_count_v2' AS measure, toUInt64(count())
FROM {{ ref('fct_execution_circles_avatars_current') }}
WHERE version = 2
  AND avatar_type = 'Org'

UNION ALL

SELECT 'human_count_v2' AS measure, toUInt64(count())
FROM {{ ref('fct_execution_circles_avatars_current') }}
WHERE version = 2
  AND avatar_type = 'Human'

UNION ALL

SELECT 'group_count_v2' AS measure, toUInt64(count())
FROM {{ ref('fct_execution_circles_avatars_current') }}
WHERE version = 2
  AND avatar_type = 'Group'

UNION ALL

SELECT 'trust_count_v1' AS measure, toUInt64(count())
FROM {{ ref('fct_execution_circles_trust_relations_current') }}
WHERE version = 1

UNION ALL

SELECT 'trust_count_v2' AS measure, toUInt64(count())
FROM {{ ref('fct_execution_circles_trust_relations_current') }}
WHERE version = 2

UNION ALL

SELECT 'token_count_v1' AS measure, toUInt64(count())
FROM {{ ref('fct_execution_circles_tokens_current') }}
WHERE version = 1

UNION ALL

SELECT 'token_count_v2' AS measure, toUInt64(count())
FROM {{ ref('fct_execution_circles_tokens_current') }}
WHERE version = 2

UNION ALL

SELECT 'circles_transfer_count_v1' AS measure, toUInt64(count())
FROM {{ ref('int_execution_circles_v1_token_transfers') }}

UNION ALL

SELECT 'circles_transfer_count_v2' AS measure, toUInt64(count())
FROM {{ ref('int_execution_circles_v2_transfers') }}

UNION ALL

SELECT 'erc20_wrapper_token_count_v2' AS measure, toUInt64(count())
FROM {{ ref('int_execution_circles_wrappers') }}
