{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_balance_cohorts_value_daily','granularity:daily']
  )
}}

SELECT
    date,
    symbol         AS token,
    cohort_unit,
    balance_bucket AS label,
    value_native,
    value_usd
FROM {{ ref('fct_execution_gpay_balance_cohorts_daily') }}
ORDER BY date, token, cohort_unit, label
