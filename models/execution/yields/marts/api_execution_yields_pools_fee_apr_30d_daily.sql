{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:yields_pools', 'metric:fee_apr_30d', 'granularity:daily']
    )
}}

SELECT
    date,
    token,
    pool AS label,
    'Fee APR (30D trailing)' AS apy_type,
    fee_apr_30d AS value
FROM {{ ref('fct_execution_yields_pools_daily') }}
WHERE fee_apr_30d IS NOT NULL
  AND token IS NOT NULL
  AND token != ''
  AND date < today()
ORDER BY date DESC, token, label

