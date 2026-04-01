{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:yields_pools', 'metric:volume_usd', 'granularity:daily']
    )
}}

SELECT
    date,
    token,
    pool AS label,
    'Volume (USD)' AS volume_type,
    volume_usd_daily AS value
FROM {{ ref('fct_execution_pools_daily') }}
WHERE volume_usd_daily IS NOT NULL
  AND token IS NOT NULL
  AND token != ''
  AND date < today()
ORDER BY date DESC, token, label
