{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_cashback_cohort_retention_monthly','granularity:monthly']
  )
}}

SELECT
    toString(activity_month) AS x,
    toString(cohort_month)   AS y,
    retention_pct,
    users                    AS value_abs,
    amount_retention_pct,
    amount_usd               AS value_usd
FROM {{ ref('fct_execution_gpay_cashback_cohort_retention_monthly') }}
ORDER BY y, x
