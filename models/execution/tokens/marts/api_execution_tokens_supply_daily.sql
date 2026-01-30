{{
  config(
    materialized='view',
    tags=['production','execution','tier1','api:tokens_supply', 'granularity:daily']
  )
}}

SELECT
  date,
  symbol      AS token,
  token_class,
  supply      AS value
FROM {{ ref('fct_execution_tokens_metrics_daily') }}
WHERE date < today()
ORDER BY
  date,
  token