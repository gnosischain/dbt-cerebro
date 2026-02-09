{{
    config(
        materialized='view',
        tags=['dev', 'execution', 'tier1', 'api:yields_pools', 'metric:tvl_by_pool', 'granularity:snapshot']
    )
}}

WITH latest_date AS (
    SELECT max(date) AS max_date
    FROM {{ ref('fct_execution_yields_pools_daily') }}
    WHERE date < today()
)

SELECT
    f.token,
    f.pool AS label,
    f.tvl_usd AS value
FROM {{ ref('fct_execution_yields_pools_daily') }} f
CROSS JOIN latest_date d
WHERE f.date = d.max_date
  AND f.token IS NOT NULL
  AND f.token != ''
  AND f.tvl_usd IS NOT NULL
  AND f.tvl_usd > 0
ORDER BY f.token, f.tvl_usd DESC
