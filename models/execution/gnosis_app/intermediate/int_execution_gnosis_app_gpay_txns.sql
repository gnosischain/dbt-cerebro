{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month',   none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='ReplacingMergeTree()',
    order_by='(block_timestamp, transaction_hash, log_index)',
    partition_by='toStartOfMonth(block_timestamp)',
    settings={'allow_nullable_key': 1},
    tags=['production','execution','gnosis_app','gpay','txns'],
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    post_hook=["SET join_algorithm = 'default'"]
  )
}}

-- Any USER-INITIATED Gnosis Pay card-wallet transaction (Payment = card spend, Crypto
-- Withdrawal, Fiat Off-ramp, Fiat Top Up) by a currently GA-owned wallet, from GA launch
-- (2025-11-12). Structural twin of int_execution_gnosis_app_gpay_topups.
--   * 'Crypto Deposit' is excluded — it is already the `topup` leg — so the two gPay legs
--     into the activity union stay disjoint (no double bucket).
--   * 'Reversal' (a refund) and 'Cashback' (a reward credit) are excluded — system-side,
--     not user activity.
-- Attribution mirrors topups: the safe's GA owner (first_ga_owner_address), gated on
-- current GA ownership via the persistent int_execution_gnosis_app_gpay_wallets bridge.

WITH ga_wallets AS (
    SELECT
        pay_wallet,
        first_ga_owner_address
    FROM {{ ref('int_execution_gnosis_app_gpay_wallets') }}
    WHERE is_currently_ga_owned
),

txns AS (
    SELECT
        a.block_timestamp        AS block_timestamp,
        a.transaction_hash       AS transaction_hash,
        a.wallet_address         AS gp_wallet,
        a.action                 AS action,
        a.token_address          AS token_address,
        a.symbol                 AS symbol,
        a.amount                 AS amount,
        a.amount_usd             AS amount_usd,
        a.counterparty           AS counterparty
    FROM {{ ref('int_execution_gpay_activity') }} a
    WHERE a.action NOT IN ('Crypto Deposit', 'Reversal', 'Cashback')
      AND a.block_timestamp >= toDateTime('2025-11-12')
      AND a.block_timestamp < today()
      {% if start_month and end_month %}
        AND toStartOfMonth(a.block_timestamp) >= toDate('{{ start_month }}')
        AND toStartOfMonth(a.block_timestamp) <= toDate('{{ end_month }}')
      {% else %}
        {{ apply_monthly_incremental_filter('a.block_timestamp', 'block_timestamp', add_and=True) }}
      {% endif %}
)

SELECT
    t.block_timestamp            AS block_timestamp,
    t.transaction_hash           AS transaction_hash,
    -- int_execution_gpay_activity has no log_index; synthesise a stable per-tx ordinal so any
    -- downstream cityHash64(tx, log_index) dedup key stays unique. A tx is atomic (single
    -- block/partition) so the window is complete within an insert_overwrite batch.
    (row_number() OVER (PARTITION BY t.transaction_hash
        ORDER BY t.gp_wallet, t.action, t.token_address, t.amount_usd, t.counterparty) - 1) AS log_index,
    w.first_ga_owner_address     AS ga_user,
    t.gp_wallet                  AS gp_wallet,
    t.action                     AS action,
    t.token_address              AS token_address,
    t.symbol                     AS symbol,
    t.amount                     AS amount,
    t.amount_usd                 AS amount_usd,
    t.counterparty               AS counterparty
FROM txns t
INNER JOIN ga_wallets w ON w.pay_wallet = t.gp_wallet
