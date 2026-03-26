{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'trusts', 'api']
    )
}}

WITH latest_avatars AS (
    SELECT *
    FROM {{ ref('fct_execution_circles_avatars_current') }}
)

SELECT
    ts.*,
    a.avatar_type,
    a.name,
    a.cid_v0_digest
FROM {{ ref('fct_execution_circles_trust_scores_current') }} ts
LEFT JOIN latest_avatars a
    ON ts.avatar = a.avatar
