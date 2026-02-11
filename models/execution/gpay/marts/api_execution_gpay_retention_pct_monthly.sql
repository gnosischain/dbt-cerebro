{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_retention_pct_monthly','granularity:monthly']
  )
}}

SELECT
    toString(activity_month) AS x,
    toString(cohort_month)   AS y,
    retention_pct            AS value
FROM {{ ref('fct_execution_gpay_retention_monthly') }}
ORDER BY y, x
