{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_active_minters', 'granularity:daily']
    )
}}

SELECT
    date,
    active_minters
FROM {{ ref('fct_execution_circles_v2_active_minters_daily') }}
WHERE date < today()
ORDER BY date DESC
