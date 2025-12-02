{{
  config(
    materialized='view',
    tags=['production','execution','tokens','balance_cohorts','api']
  )
}}

SELECT
  date,
  symbol,
  balance_bucket,
  holders_in_bucket,
  value_usd_in_bucket
FROM {{ ref('fct_execution_tokens_balance_cohorts_daily') }}
WHERE date < today()
ORDER BY date, token_address, balance_bucket