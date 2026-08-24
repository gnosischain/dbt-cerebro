{{
  config(
    materialized='table',
    engine='ReplacingMergeTree()',
    order_by='account',
    unique_key='account',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gpay'],
    pre_hook=["SET join_use_nulls = 1"],
    post_hook=["SET join_use_nulls = 0"]
  )
}}

-- Bridge from Spender `Spend.account` addresses to card-Safe identities.
--
-- Spend.account is NOT the GP Safe itself: it is a per-card module ENABLED ON
-- the card Safe (the Safe emits ExecutionFromModuleSuccess(module = account)
-- in the same spend transaction, and the EURe Transfer to the settlement
-- collector originates from the Safe). The enabling Safe is recovered from
-- int_execution_safes_module_events; the mapping is 1:1 (no account has ever
-- been enabled on more than one Safe, verified over full history 2026-08-24).
-- ~99.5% of accounts map to a Safe already in int_execution_gpay_wallets; the
-- rest are decode-lag or non-card Safes, kept here with safe_address NULL /
-- is_gpay_wallet 0 so consumers choose their own gate.

WITH spender_accounts AS (
    SELECT DISTINCT lower(spend_account) AS account
    FROM {{ ref('int_execution_gpay_spender_events') }}
    WHERE event_name = 'Spend'
      AND spend_account IS NOT NULL
),

enable_events AS (
    SELECT
        lower(target_address)                             AS account,
        argMax(lower(safe_address), block_timestamp)      AS safe_address,
        min(block_timestamp)                              AS enabled_at
    FROM {{ ref('int_execution_safes_module_events') }}
    WHERE event_kind = 'enabled_module'
      AND lower(target_address) IN (SELECT account FROM spender_accounts)
    GROUP BY account
)

SELECT
    a.account                                                  AS account,
    e.safe_address                                             AS safe_address,
    coalesce(c.canonical_address, e.safe_address)              AS canonical_safe,
    e.enabled_at,
    toUInt8(isNotNull(w.address) OR isNotNull(wc.address))     AS is_gpay_wallet
FROM spender_accounts a
LEFT JOIN enable_events e
    ON e.account = a.account
LEFT JOIN {{ ref('int_execution_gpay_safe_canonical') }} c
    ON c.address = e.safe_address
LEFT JOIN {{ ref('int_execution_gpay_wallets') }} w
    ON w.address = e.safe_address
LEFT JOIN {{ ref('int_execution_gpay_wallets') }} wc
    ON wc.address = c.canonical_address
