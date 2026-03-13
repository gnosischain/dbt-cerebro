{{
    config(
        materialized='view',
        tags=['dev','execution','tier1','api:yields_pools','metric:tvl_token','granularity:daily']
    )
}}

SELECT
    date,
    ref_token AS token,
    pool AS label,
    series,
    tvl_usd,
    tvl_in_token0,
    tvl_in_token1,
    token0_symbol,
    token1_symbol,
    token_amount
FROM {{ ref('fct_execution_yields_pools_tvl_token_daily') }}
WHERE date < today()
ORDER BY date DESC, token, label, series
