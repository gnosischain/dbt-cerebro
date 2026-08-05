{% set start_month = var('start_month', none) %}
{% set end_month   = var('end_month', none) %}

{{
  config(
    materialized='incremental',
    incremental_strategy=('append' if start_month else 'insert_overwrite'),
    engine='ReplacingMergeTree()',
    order_by='(safe_address, block_time, tx_hash, token_address, counterparty, action)',
    partition_by='toStartOfMonth(date)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','celo','gpay','activity']
  )
}}

{% set cashback_sources = [] %}  {# reward disburser(s); empty until identified — cashback not paid yet #}

-- Per-transfer classified Celo GP activity, off the single transfer base
-- (int_celo_gpay_safe_transfers_alltoken), restricted to whitelisted GP tokens
-- (token_symbol IS NOT NULL — excludes CELO gas dust and spoof tokens). The base
-- already resolves the card side (safe_address / direction / counterparty), so
-- classification reads those directly rather than re-deriving from sender/receiver.
--
-- SETTLEMENT IS A SET OF CONTRACTS, seeded in celo_gpay_settlement_contracts and
-- resolved in the `settlements` CTE below — never a hardcoded address. Two bridges
-- are live at once (settlement_legacy 0xc4df5cac… since 2026-03-31, scheduled to
-- migrate onto settlement_current 0xc07cd8c2… since 2026-05-28; GP confirmed both
-- are theirs on 2026-08-05). They are different contracts sharing no event
-- signatures, not two versions of one — so do not call them v1/v2.
--
-- THIS FILTER AND int_celo_gpay_safe_registry MUST WIDEN TOGETHER. The registry
-- decides which Safes exist; this CASE decides what their transfers mean. Widening
-- discovery alone would admit 235 cards whose 1,743 settlement transfers then fall
-- through to the `direction = 'out'` catch-all and book as WITHDRAWALS — inflating
-- withdrawals and still under-reporting payments, which is worse than omitting the
-- cards entirely. Never change one without the other.
--
-- settlement_address records WHICH bridge each settlement transfer used. It is a
-- per-TRANSFER fact, deliberately not a per-card generation column: when a card
-- migrates, its old payments stay on the legacy contract and its new ones land on
-- the current one, so any card-level attribute would go stale on migration day.
-- This column is also how migration progress becomes measurable.
--
-- Actions:
--   Payment    — card -> any settlement bridge in a STABLECOIN (the only real card
--                spend; token_class keeps a reward-token-to-bridge out of spend).
--   Other      — card -> bridge in a non-stablecoin whitelisted token (rare; kept
--                visible but excluded from every payment metric downstream).
--   Withdrawal — card -> anywhere else.
--   Reversal   — bridge -> card (processor refund of a failed/disputed charge).
--   Cashback   — a configured rewards disburser -> card in an RWA reward token.
--                COMPILED OUT until the cashback_sources list above is populated
--                (cashback not paid yet); until then reward inflows fall through
--                to Top-up — a safe no-op scaffold.
--   Top-up     — anything else -> card (e.g. a MiniPay funding wallet).
--
-- Safe-to-Safe transfers (both sides a card) would appear twice in the base (an
-- 'out' and an 'in' row). We collapse to the sender ('out') side so each transfer
-- is one row — a Withdrawal from the sender — matching the prior sender-priority
-- semantics. (In practice Celo GP has no card-to-card transfers.)
--
-- Incremental insert_overwrite recomputes the whole current calendar month every
-- run, so a card recognized slightly late is reclassified within the month.
--
-- The start_month/end_month branch is the staged-rebuild path driven by
-- scripts/full_refresh/refresh.py (meta.full_refresh), mirroring
-- int_execution_gpay_activity on Gnosis Chain. Scoped batches APPEND rather than
-- REPLACE (docs/lessons/staged-insert-overwrite-wipe.md): a 3-month batch against
-- monthly partitions happens to be partition-aligned, but append removes the
-- dependence on that alignment entirely, so no future change to batch_months or
-- the partition grain can turn a stage into a partition wipe. The cost is that a
-- scoped run must target EMPTY months — re-running one over a populated month
-- appends a second copy, and the marts read this table without FINAL.
-- Rebuild this after a staged rebuild of the transfers base, month for month.

WITH settlements AS (
    SELECT lower(address) AS address
    FROM {{ ref('celo_gpay_settlement_contracts') }}
    WHERE status IN ('active', 'migrating')
),

base AS (
    SELECT *
    FROM {{ ref('int_celo_gpay_safe_transfers_alltoken') }}
    WHERE token_symbol IS NOT NULL
    {% if start_month and end_month %}
      AND toStartOfMonth(block_date) >= toDate('{{ start_month }}')
      AND toStartOfMonth(block_date) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('block_date', 'date', true) }}
    {% endif %}
),

one_per_transfer AS (
    SELECT * FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY tx_hash, log_index
                ORDER BY direction DESC   -- 'out' before 'in': keep the sender side
            ) AS _rn
        FROM base
    )
    WHERE _rn = 1
),

flagged AS (
    -- Resolve settlement membership ONCE per row rather than re-running the IN
    -- subquery in every CASE branch.
    SELECT
        *,
        counterparty IN (SELECT address FROM settlements) AS is_settlement
    FROM one_per_transfer
)

SELECT
    tx_hash,
    block_time,
    block_date AS date,
    safe_address,
    CASE
        WHEN direction = 'out' AND is_settlement AND token_class = 'STABLECOIN' THEN 'Payment'
        WHEN direction = 'out' AND is_settlement THEN 'Other'
        WHEN direction = 'out' THEN 'Withdrawal'
        WHEN direction = 'in'  AND is_settlement THEN 'Reversal'
{%- if cashback_sources %}
        WHEN direction = 'in'  AND counterparty IN ({% for a in cashback_sources %}'{{ a }}'{% if not loop.last %}, {% endif %}{% endfor %}) AND token_class = 'RWA' THEN 'Cashback'
{%- endif %}
        WHEN direction = 'in'  THEN 'Top-up'
    END AS action,
    direction,
    token_symbol,
    token_address,
    counterparty,
    if(is_settlement, counterparty, CAST(NULL AS Nullable(String))) AS settlement_address,
    amount,
    amount_usd
FROM flagged
ORDER BY safe_address, block_time
