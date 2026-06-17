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
    tags=['production','execution','gnosis_app','gpay','topups'],
    pre_hook=["SET join_algorithm = 'grace_hash'"],
    post_hook=["SET join_algorithm = 'default'"]
  )
}}

-- A top-up is a Gnosis Pay "Crypto Deposit" into a GP wallet that is currently
-- GA-owned (per the persistent int_execution_gnosis_app_gpay_wallets bridge),
-- from the GA launch (2025-11-12) on. This supersedes the old same-transaction
-- CoW-trade + deposit definition, which captured only ~1.4% of GA-owned funding
-- (the bulk is direct EURe transfers, not atomic swaps). See
-- docs/model_review/gpay_topup_capture_probe.md.
--
-- Scope choices (broadest, per product decision): counts ALL Crypto Deposits
-- into currently-GA-owned wallets. Two narrowing toggles are intentionally NOT
-- applied here and can be added as a one-line WHERE if wanted:
--   * self-funding: counterparty = the wallet's own GA owner address
--   * temporal:     block_timestamp < first_ga_owner_at (deposits predating
--                   GA ownership of an imported wallet)

WITH ga_wallets AS (
    SELECT
        pay_wallet,
        first_ga_owner_address
    FROM {{ ref('int_execution_gnosis_app_gpay_wallets') }}
    WHERE is_currently_ga_owned
),

deposits AS (
    SELECT
        a.block_timestamp        AS block_timestamp,
        a.transaction_hash       AS transaction_hash,
        a.wallet_address         AS gp_wallet,
        a.token_address          AS token_bought_address,
        a.symbol                 AS token_bought_symbol,
        a.amount                 AS amount_bought,
        a.amount_usd             AS amount_usd,
        a.counterparty           AS counterparty
    FROM {{ ref('int_execution_gpay_activity') }} a
    WHERE a.action = 'Crypto Deposit'
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
    d.block_timestamp            AS block_timestamp,
    d.transaction_hash           AS transaction_hash,
    -- int_execution_gpay_activity has no log_index; synthesise a stable per-tx
    -- ordinal so the downstream cityHash64(tx, log_index) dedup key in
    -- int_execution_gnosis_app_conversions / _events_chain_unified stays unique
    -- per deposit. A tx is atomic (single block/partition) so the window is
    -- complete within an insert_overwrite batch.
    (row_number() OVER (PARTITION BY d.transaction_hash
        ORDER BY d.gp_wallet, d.token_bought_address, d.amount_usd, d.counterparty) - 1) AS log_index,
    w.first_ga_owner_address     AS ga_user,
    d.gp_wallet                  AS gp_wallet,
    d.token_bought_address       AS token_bought_address,
    d.token_bought_symbol        AS token_bought_symbol,
    d.amount_bought              AS amount_bought,
    d.amount_usd                 AS amount_usd,
    d.counterparty               AS counterparty
FROM deposits d
INNER JOIN ga_wallets w ON w.pay_wallet = d.gp_wallet
