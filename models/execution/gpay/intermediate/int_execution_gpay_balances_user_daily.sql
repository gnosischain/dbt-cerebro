{{
  config(
    materialized='view',
    tags=['production','execution','gpay']
  )
}}

-- Old-safe rows (all tokens) count only before the pair's switch_at; new
-- safes always count. Per-Safe marts that must show raw on-chain balances
-- (fct_execution_gpay_user_balances_latest) keep reading
-- int_execution_gpay_balances_daily directly.

SELECT
    b.date        AS date,
    b.address     AS address,
    b.symbol      AS symbol,
    b.balance     AS balance,
    b.balance_usd AS balance_usd
FROM {{ ref('int_execution_gpay_balances_daily') }} b
LEFT JOIN {{ ref('int_execution_gpay_safe_switchover') }} s
    ON b.address = s.old_safe
WHERE s.old_safe = ''
   OR b.date < s.switch_at
