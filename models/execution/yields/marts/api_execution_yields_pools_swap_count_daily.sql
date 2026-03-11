{{
    config(
        materialized='view',
        tags=['dev','execution','tier1','api:yields_pools', 'metric:swap_count', 'granularity:daily']
    )
}}

SELECT
    date,
    token,
    pool AS label,
    swap_count AS value
FROM {{ ref('fct_execution_yields_pools_daily') }}
WHERE swap_count IS NOT NULL
  AND token IS NOT NULL
  AND token != ''
  AND date < today()
ORDER BY date DESC, token, label
