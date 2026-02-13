{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_retention_volume_monthly','granularity:monthly']
  )
}}

SELECT
    toString(activity_month) AS date,
    toString(cohort_month)   AS label,
    amount_usd               AS value
FROM {{ ref('fct_execution_gpay_retention_monthly') }}
ORDER BY date, label
