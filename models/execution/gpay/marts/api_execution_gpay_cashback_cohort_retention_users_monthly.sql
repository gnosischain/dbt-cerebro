{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_cashback_cohort_retention_users_monthly','granularity:monthly']
  )
}}

SELECT
    toString(activity_month) AS date,
    toString(cohort_month)   AS label,
    users                    AS value
FROM {{ ref('fct_execution_gpay_cashback_cohort_retention_monthly') }}
ORDER BY date, label
