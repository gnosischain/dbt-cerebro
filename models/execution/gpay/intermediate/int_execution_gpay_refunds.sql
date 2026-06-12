{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='(new_safe, symbol)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gpay']
  )
}}

-- Exploit-recovery refunds credited to migrated NEW Safes. Refunds are
-- sent from the distributor wallets in the gpay_refund_distributors seed,
-- in per-user amounts, days after the migration completed - NOT at Safe
-- deployment time. refund_date is the cutover used by
-- int_execution_gpay_balances_user_daily: from that date the OLD Safe's
-- residual balance for the same token is recovery-entitled and must no
-- longer count as user holdings (it would double count the refund).

SELECT
    t."to"                          AS new_safe,
    t.symbol                        AS symbol,
    min(t.date)                     AS refund_date,
    sum(toFloat64(t.amount_raw))    AS refund_amount_raw
FROM {{ ref('int_execution_transfers_whitelisted_daily') }} t
WHERE t."to" IN (SELECT lower(new_safe_address) FROM {{ ref('gp_migrated_safes') }})
  AND t."from" IN (SELECT lower(address) FROM {{ ref('gpay_refund_distributors') }})
GROUP BY t."to", t.symbol
