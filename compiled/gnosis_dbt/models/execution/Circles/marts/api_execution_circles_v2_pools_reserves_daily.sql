

-- Daily USD TVL per main Circles DEX pool = sum of both token legs' USD value.
-- Rolls up the per-token base (api_execution_circles_v2_pools_reserves_token_daily),
-- which covers all four pools (Uniswap V3 + the Balancer V3 pool). Backs the
-- Reserves-over-time chart and the latest-TVL used by the KPI / leaderboard.
SELECT
    date,
    pool,
    pool_address,
    round(sum(tvl_usd), 2) AS tvl_usd
FROM `dbt`.`api_execution_circles_v2_pools_reserves_token_daily`
GROUP BY date, pool, pool_address