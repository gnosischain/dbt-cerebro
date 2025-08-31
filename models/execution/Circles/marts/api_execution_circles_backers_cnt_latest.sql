{{ 
    config(
        materialized='view',
        tags=['production','execution','circles','backers']
    )
}}

WITH

backers_latest AS (
    SELECT 
        total
    FROM {{ ref('fct_execution_circles_backing') }}
    WHERE 
        date = (SELECT max(date) FROM {{ ref('fct_execution_circles_backing') }})
),

backers_7d AS (
    SELECT 
        total
    FROM {{ ref('fct_execution_circles_backing') }}
    WHERE 
        date = subtractDays((SELECT max(date) FROM {{ ref('fct_execution_circles_backing') }}), 7)
)

SELECT
    t1.total AS total
    ,IF(t1.total=0 AND t2.total=0, 0, ROUND((COALESCE(t1.total / NULLIF(t2.total, 0), 0) - 1) * 100, 1)) AS change_pct
FROM backers_latest t1
CROSS JOIN backers_7d t2
