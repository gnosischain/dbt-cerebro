{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_churn_rates_monthly','granularity:monthly']
  )
}}

SELECT
    scope,
    toString(month) AS month,
    churn_rate,
    retention_rate
FROM {{ ref('fct_execution_gpay_churn_monthly') }}
ORDER BY scope, month
