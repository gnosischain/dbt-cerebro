{{
    config(
        materialized='view',
        tags=['dev','execution','tier1','api:yields_pools', 'metric:fees_usd', 'granularity:daily']
    )
}}

SELECT
    date,
    token,
    pool AS label,
    fees_usd_daily AS value
FROM {{ ref('fct_execution_yields_pools_daily') }}
WHERE fees_usd_daily IS NOT NULL
  AND token IS NOT NULL
  AND token != ''
  AND date < today()
ORDER BY date DESC, token, label
