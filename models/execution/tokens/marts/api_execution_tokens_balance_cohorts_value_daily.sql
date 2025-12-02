{{
  config(
    materialized='view',
    tags=['production','execution','tokens','balance_cohorts','api']
  )
}}

SELECT
  date,
  symbol                         AS token,           
  balance_bucket                 AS label,           
  sum(value_usd_in_bucket)       AS value            
FROM {{ ref('fct_execution_tokens_balance_cohorts_daily') }}
WHERE date < today()
GROUP BY
  date,
  token,
  label
ORDER BY
  date,
  token,
  label