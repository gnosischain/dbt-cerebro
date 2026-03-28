{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:yields_pools', 'metric:net_apr', 'granularity:daily']
    )
}}

SELECT
    date,
    token,
    pool AS label,
    fee_apr_7d,
    lvr_apr_7d,
    net_apr_7d
FROM {{ ref('fct_execution_yields_pools_daily') }}
WHERE fee_apr_7d IS NOT NULL
  AND token IS NOT NULL
  AND token != ''
  AND date < today()
ORDER BY date DESC, token, label
