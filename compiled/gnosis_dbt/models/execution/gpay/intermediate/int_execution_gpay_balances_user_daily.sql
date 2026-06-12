

-- Only refunded ("lost") pairs are cut over: their old-safe rows (all
-- tokens) count before first_refund_at and are recovery-entitled after.
-- Non-exploited pairs have no cutover - the old safe counts until the user
-- moves the funds. All user-holdings consumers (balance aggregates and
-- fct_execution_gpay_user_balances_latest) read this view; raw on-chain
-- per-Safe balances remain available in int_execution_gpay_balances_daily.

SELECT
    b.date        AS date,
    b.address     AS address,
    b.symbol      AS symbol,
    b.balance     AS balance,
    b.balance_usd AS balance_usd
FROM `dbt`.`int_execution_gpay_balances_daily` b
LEFT JOIN `dbt`.`int_execution_gpay_safe_switchover` s
    ON b.address = s.old_safe
WHERE s.old_safe = ''
   OR s.is_lost = 0
   OR b.date < s.first_refund_at