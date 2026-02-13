{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_balance_cohorts_value_daily','granularity:daily']
  )
}}

SELECT
    date,
    balance_bucket AS label,
    value_usd      AS value
FROM {{ ref('fct_execution_gpay_balance_cohorts_daily') }}
ORDER BY date, label
