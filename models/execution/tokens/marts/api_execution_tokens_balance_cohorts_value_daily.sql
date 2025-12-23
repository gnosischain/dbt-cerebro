{{
  config(
    materialized='view',
    tags=['dev','execution','tier1','api:balance_cohorts_amount_per_token', 'granularity:daily']
  )
}}

SELECT
  date,
  symbol                         AS token,   
  balance_bucket                 AS label,   
  value_usd_in_bucket            AS value    
FROM {{ ref('int_execution_tokens_balance_cohorts_daily') }}
WHERE date < today()
ORDER BY
  date,
  token,
  label