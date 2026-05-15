{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:circles_v2_minter_cohort', 'granularity:daily']
    )
}}

SELECT
    date,
    cohort,
    cohort_order,
    cnt
FROM {{ ref('fct_execution_circles_v2_minter_cohort_daily') }}
WHERE date < today()
ORDER BY date DESC, cohort_order
