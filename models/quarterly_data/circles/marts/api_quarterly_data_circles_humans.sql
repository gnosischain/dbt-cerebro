{{
    config(
        materialized='view',
        tags=['production', 'quarterly_data', 'tier0', 'api:circles_humans', 'granularity:quarterly']
    )
}}

SELECT
    toStartOfQuarter(date) AS quarter,
    argMax(total, date) AS registered_humans
FROM {{ ref('fct_execution_circles_v2_avatars') }}
WHERE avatar_type = 'Human'
  AND date < today()
GROUP BY quarter
ORDER BY quarter
