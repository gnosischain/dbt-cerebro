{{
    config(
        materialized='view',
        tags=['dev','execution','tier1','api:yields_pools', 'metric:tvl_usd', 'granularity:daily']
    )
}}

SELECT
    date,
    token,
    pool AS label,
    'TVL (USD)' AS tvl_type,
    tvl_usd AS value
FROM {{ ref('fct_execution_yields_pools_daily') }}
WHERE tvl_usd IS NOT NULL
  AND token IS NOT NULL
  AND token != ''
  AND date < today()
ORDER BY date DESC, token, label

