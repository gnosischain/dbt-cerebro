{{
  config(
    materialized='view',
    tags=['dev','execution','tier1','api:yields_lending_balance_cohorts_holders', 'granularity:daily']
  )
}}

SELECT
  date,
  symbol AS token,
  cohort_unit,
  balance_bucket AS label,
  holders_in_bucket AS value
FROM {{ ref('int_execution_yields_aave_balance_cohorts_daily') }}
WHERE date < today()
ORDER BY date, token, cohort_unit, label
