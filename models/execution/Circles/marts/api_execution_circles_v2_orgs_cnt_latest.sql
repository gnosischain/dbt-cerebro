{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_orgs_cnt', 'granularity:latest']
    )
}}

WITH current AS (
    SELECT total AS value
    FROM {{ ref('fct_execution_circles_v2_avatars') }}
    WHERE avatar_type = 'Org' AND date = (SELECT max(date) FROM {{ ref('fct_execution_circles_v2_avatars') }} WHERE date < today())
),
prior AS (
    SELECT total AS value
    FROM {{ ref('fct_execution_circles_v2_avatars') }}
    WHERE avatar_type = 'Org' AND date = (SELECT max(date) FROM {{ ref('fct_execution_circles_v2_avatars') }} WHERE date < today()) - 7
)

SELECT
    c.value AS total,
    round((c.value - p.value) / p.value * 100, 1) AS change_pct
FROM current c
CROSS JOIN prior p
