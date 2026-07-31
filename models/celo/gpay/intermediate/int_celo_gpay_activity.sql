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

{% set settlement = '0xc07cd8c24fb384d5e2b60a3ef39751f5d4cb69e1' %}  {# GP AggregateBridge (settlement sink) #}
{% set cashback_sources = [] %}  {# reward disburser(s); empty until identified — cashback not paid yet #}

-- Per-transfer classified Celo GP activity, off the single transfer base
-- (int_celo_gpay_safe_transfers_alltoken), restricted to whitelisted GP tokens
-- (token_symbol IS NOT NULL — excludes CELO gas dust and spoof tokens). The base
-- already resolves the card side (safe_address / direction / counterparty), so
-- classification reads those directly rather than re-deriving from sender/receiver.
--
-- Actions:
--   Payment    — card -> bridge in a STABLECOIN (the only real card spend; the
--                token_class gate keeps a reward-token-to-bridge out of spend).
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

WITH base AS (
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
)

SELECT
    tx_hash,
    block_time,
    block_date AS date,
    safe_address,
    CASE
        WHEN direction = 'out' AND counterparty = '{{ settlement }}' AND token_class = 'STABLECOIN' THEN 'Payment'
        WHEN direction = 'out' AND counterparty = '{{ settlement }}' THEN 'Other'
        WHEN direction = 'out' THEN 'Withdrawal'
        WHEN direction = 'in'  AND counterparty = '{{ settlement }}' THEN 'Reversal'
{%- if cashback_sources %}
        WHEN direction = 'in'  AND counterparty IN ({% for a in cashback_sources %}'{{ a }}'{% if not loop.last %}, {% endif %}{% endfor %}) AND token_class = 'RWA' THEN 'Cashback'
{%- endif %}
        WHEN direction = 'in'  THEN 'Top-up'
    END AS action,
    direction,
    token_symbol,
    token_address,
    counterparty,
    amount,
    amount_usd
FROM one_per_transfer
ORDER BY safe_address, block_time
