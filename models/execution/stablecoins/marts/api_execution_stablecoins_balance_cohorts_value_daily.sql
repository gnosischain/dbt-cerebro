{{
  config(
    materialized='view',
    tags=['dev','execution','tier1','api:stablecoins_balance_cohorts_value', 'granularity:daily']
  )
}}

SELECT
  date,
  symbol AS token,
  balance_bucket AS label,
  value_usd_in_bucket AS value
FROM {{ ref('int_execution_tokens_balance_cohorts_daily') }}
WHERE date < today()
  AND token_class = 'STABLECOIN'
ORDER BY
  date,
  token,
  label

