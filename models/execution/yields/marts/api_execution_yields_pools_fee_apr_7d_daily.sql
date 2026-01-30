{{
    config(
        materialized='view',
        tags=['dev','execution','tier1','api:yields_pools', 'metric:fee_apr_7d', 'granularity:daily']
    )
}}

SELECT
    date,
    token,
    pool AS label,
    'Fee APR (7D trailing)' AS apy_type,
    fee_apr_7d AS value
FROM {{ ref('fct_execution_yields_pools_daily') }}
WHERE fee_apr_7d IS NOT NULL
  AND token IS NOT NULL
  AND token != ''
  AND date < today()
ORDER BY date DESC, token, label
