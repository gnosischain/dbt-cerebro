{{
  config(
    materialized='view',
    tags=['production','execution','circles_v2','api:circles_v2_pools_reserves_latest','granularity:latest']
  )
}}

-- Latest per-(pool, token) reserve and USD value for the main Circles DEX pools.
-- Two rows per pool (the two legs); backs the pool-reserves card.
SELECT
    pool,
    pool_address,
    token_address,
    token_symbol,
    argMax(reserve, date)   AS reserve,
    argMax(price_usd, date) AS price_usd,
    argMax(tvl_usd, date)   AS tvl_usd,
    max(date)               AS as_of
FROM {{ ref('api_execution_circles_v2_pools_reserves_token_daily') }}
GROUP BY pool, pool_address, token_address, token_symbol
