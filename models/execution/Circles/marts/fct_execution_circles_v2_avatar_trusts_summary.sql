{{
    config(
        materialized='table',
        tags=['production','execution','circles','v2','avatar','mart']
    )
}}

-- Latest per-avatar trust summary: trusts given and trusts received as of
-- the last complete day. Materialised daily from fct_avatar_trusts_daily;
-- the matching api_ is a thin passthrough.

WITH latest AS (
    SELECT max(day) AS d
    FROM {{ ref('fct_execution_circles_v2_avatar_trusts_daily') }}
    WHERE day < today()
)

SELECT
    t.avatar,
    t.trusts_given_count,
    t.trusts_received_count
FROM {{ ref('fct_execution_circles_v2_avatar_trusts_daily') }} t
CROSS JOIN latest
WHERE t.day = latest.d AND t.avatar IS NOT NULL
