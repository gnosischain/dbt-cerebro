{{
  config(
    materialized='view',
    tags=['dev','execution','tier1','api:balance_cohorts_holders_per_token', 'granularity:daily']
  )
}}

SELECT
  date,
  symbol                         AS token,   
  balance_bucket                 AS label,   
  holders_in_bucket              AS value    
FROM {{ ref('int_execution_tokens_balance_cohorts_daily') }}
WHERE date < today()
ORDER BY
  date,
  token,
  label