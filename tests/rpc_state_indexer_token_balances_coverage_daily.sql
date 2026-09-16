{{ config(severity='error', tags=['production', 'data_quality', 'data_quality_daily', 'balances', 'rpc_state_indexer']) }}
-- Every whitelist token the census reads must carry holder balances on every day of the
-- lookback window, and those balances must reconcile to the token's own totalSupply.
--
-- The census publishes a curated token-day only when the observed holder balances sum to
-- totalSupply (integrity_mode full_supply), so a token-day that is present but fails the
-- reconciliation means the census changed shape underneath us, not that a token drifted.
-- A token with supply but no holders is a real failure; a token with neither is simply dormant
-- (bHIGH and bC3M sit at zero supply all through August 2026) and is not flagged.
-- A token-day that is missing entirely means the indexer stopped censusing that token —
-- check its daily_curated_balances selector entry before looking at dbt.
--
-- Returns offending token-days; passing = zero rows. Yesterday is excluded because the
-- census legitimately lags to D-1, and today is never built.
SELECT
    s.date AS d
    ,s.token_address AS token_address
    ,s.symbol AS symbol
    ,s.supply_total AS supply_total
    ,s.supply_holders AS supply_holders
    ,s.holders AS holders
FROM {{ ref('int_rpc_state_indexer_token_supply_daily') }} AS s
WHERE s.date >= today() - {{ var('test_lookback_days', 7) }}
  AND s.date < today() - 1
  AND s.supply_holders IS NOT NULL
  AND (
        (s.holders = 0 AND s.supply_total > 0)
     OR abs(s.supply_total - s.supply_holders) > 1e-9 * greatest(abs(s.supply_total), 1)
  )

UNION ALL

-- Density: every token censused in the window must have all 7 days of [today()-8, today()-2].
-- A missing day is an indexer outage for that token (no rows, no error), which the census
-- consumers would read as zero balance for the day until the census lands.
SELECT
    toDate(today() - 2) AS d
    ,g.token_address AS token_address
    ,g.symbol AS symbol
    ,toFloat64(g.days_present) AS supply_total
    ,toFloat64(7) AS supply_holders
    ,toUInt64(0) AS holders
FROM (
    SELECT token_address, any(symbol) AS symbol, uniqExact(date) AS days_present
    FROM {{ ref('int_rpc_state_indexer_token_supply_daily') }}
    WHERE date BETWEEN today() - 8 AND today() - 2
      AND supply_holders IS NOT NULL
    GROUP BY token_address
) AS g
WHERE g.days_present < 7
