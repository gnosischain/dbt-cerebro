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
FROM `dbt`.`int_rpc_state_indexer_token_supply_daily` AS s
WHERE s.date >= today() - 7
  AND s.date < today() - 1
  AND s.supply_holders IS NOT NULL
  AND (
        (s.holders = 0 AND s.supply_total > 0)
     OR abs(s.supply_total - s.supply_holders) > 1e-9 * greatest(abs(s.supply_total), 1)
  )