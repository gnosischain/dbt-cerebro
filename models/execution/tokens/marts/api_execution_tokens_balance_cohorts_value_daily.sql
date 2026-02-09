{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:balance_cohorts_amount_per_token', 'granularity:daily']
  )
}}

SELECT
  date,
  symbol                         AS token,   
  balance_bucket                 AS label,   
  value_native_in_bucket         AS value_native,
  value_usd_in_bucket            AS value_usd
FROM {{ ref('int_execution_tokens_balance_cohorts_daily') }}
WHERE date < today()
ORDER BY
  date,
  token,
  label