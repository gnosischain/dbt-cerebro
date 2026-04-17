{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:execution_lending_balance_cohorts_holders', 'granularity:daily']
  )
}}

SELECT
  date,
  protocol,
  symbol AS token,
  cohort_unit,
  balance_bucket AS label,
  holders_in_bucket AS value
FROM {{ ref('int_execution_lending_aave_balance_cohorts_daily') }}
WHERE date < today()
ORDER BY date, protocol, token, cohort_unit, label
