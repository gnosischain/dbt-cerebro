

-- Public DAU series: broad (`active_wallets`, any on-chain action by an app-engaged
-- wallet) + app-tagged (`active_wallets_app_tagged`, deliberate app-feature action,
-- comparable to the heuristic current-app DAU). WAU/MAU live in the underlying
-- fct_execution_gnosis_app_gt_active_wallets table (period_type week/month).
SELECT
    period_start    AS date,
    active_wallets,
    active_wallets_app_tagged,
    new_wallets,
    new_wallets_app_tagged
FROM `dbt`.`fct_execution_gnosis_app_gt_active_wallets`
WHERE period_type = 'day'
ORDER BY date DESC