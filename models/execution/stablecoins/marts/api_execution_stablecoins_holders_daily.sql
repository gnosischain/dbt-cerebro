{{
  config(
    materialized='view',
    tags=['dev','execution','tier1','api:stablecoins_holders', 'granularity:daily']
  )
}}

SELECT
  date,
  symbol AS token,
  token_class,
  holders AS value
FROM {{ ref('fct_execution_tokens_metrics_daily') }}
WHERE date < today()
  AND token_class = 'STABLECOIN'
ORDER BY
  date,
  token

