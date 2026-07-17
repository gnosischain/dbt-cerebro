{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='ReplacingMergeTree()',
    order_by='(safe_address, block_time, tx_hash, token_address, counterparty, action)',
    partition_by='toStartOfMonth(date)',
    settings={ 'allow_nullable_key': 1 },
    tags=['production','execution','celo','gpay','activity']
  )
}}

{% set settlement = var('celo_gp_settlement_address') %}
{% set cashback_sources = var('celo_gp_cashback_sources', []) %}

-- Per-transfer classification of Celo GP activity, NATIVE-only: reads
-- int_celo_gpay_transfers_native (whitelisted-token ERC-20 transfers touching a
-- GP card Safe, decoded straight from celo_execution.logs). Classification
-- lives HERE, not upstream (mirrors Gnosis Chain's int_execution_gpay_activity),
-- so fixes/additions are a dbt edit + re-run.
--
-- CASE ordering matters: sender-based branches (Payment/Withdrawal) are checked
-- before receiver-based branches (Reversal/Cashback/Top-up). A hypothetical
-- Safe-to-Safe transfer has sender AND receiver both in `wallets`; checking
-- receiver first would misclassify it as a Top-up to the receiving Safe instead
-- of a Withdrawal from the sending Safe (safe_address below always resolves to
-- sender when sender is a Safe, so action must agree with that).
--
-- Cashback: the MiniPay Card pays up to 5% cashback in XAUt0 (also USDT/USDC in
-- some markets) from a rewards disburser that is SEPARATE from the settlement
-- bridge (verified 2026-07: zero reward-token flow through the bridge). The
-- disburser address is not yet identified, so var celo_gp_cashback_sources is
-- empty and the Cashback branch below is COMPILED OUT — reward inflows currently
-- fall through to Top-up exactly as before (a safe no-op). Populate the var (with
-- 0x-prefixed lowercase addresses) to separate Cashback from user Top-ups.

WITH wallets AS (
    SELECT safe_address FROM {{ ref('int_celo_gpay_wallets') }}
),

classified AS (
    SELECT
        t.block_date,
        t.block_time,
        t.tx_hash,
        t.sender,
        t.receiver,
        t.token_symbol,
        t.token_address,
        t.amount,
        t.amount_usd,
        CASE
            WHEN t.sender IN (SELECT safe_address FROM wallets)
             AND t.receiver = '{{ settlement }}'
            THEN 'Payment'

            WHEN t.sender IN (SELECT safe_address FROM wallets)
            THEN 'Withdrawal'

            WHEN t.receiver IN (SELECT safe_address FROM wallets)
             AND t.sender = '{{ settlement }}'
            THEN 'Reversal'
{%- if cashback_sources %}

            WHEN t.receiver IN (SELECT safe_address FROM wallets)
             AND t.sender IN ({% for a in cashback_sources %}'{{ a }}'{% if not loop.last %}, {% endif %}{% endfor %})
            THEN 'Cashback'
{%- endif %}

            WHEN t.receiver IN (SELECT safe_address FROM wallets)
            THEN 'Top-up'
        END AS action,
        CASE
            WHEN t.sender IN (SELECT safe_address FROM wallets) THEN t.sender
            ELSE t.receiver
        END AS safe_address,
        CASE
            WHEN t.sender IN (SELECT safe_address FROM wallets) THEN 'out'
            ELSE 'in'
        END AS direction,
        CASE
            WHEN t.sender IN (SELECT safe_address FROM wallets) THEN t.receiver
            ELSE t.sender
        END AS counterparty
    -- FINAL: int_celo_gpay_transfers_native is a ReplacingMergeTree; read it
    -- collapsed so a row that landed in two overlapping incremental windows
    -- cannot double-count before a background merge collapses it.
    FROM {{ ref('int_celo_gpay_transfers_native') }} t
    FINAL
    -- insert_overwrite + this macro recomputes the WHOLE current month's
    -- partition every run (not just literally-new rows) — so a Safe recognized
    -- slightly late (registry lag) is reclassified correctly on the next run
    -- within the same month. int_celo_gpay_wallets stays a full rebuild
    -- regardless (bounded by card count, not transaction volume).
    {{ apply_monthly_incremental_filter('t.block_date', 'date', false) }}
)

-- action IS NULL means neither sender nor receiver matched a wallet at
-- classification time. transfers_native and int_celo_gpay_wallets now both
-- derive from the SAME native registry within a single dbt build, so the
-- cross-source race that motivated this drop on the old Dune pipeline is
-- largely gone; the filter is kept as a cheap safety net and still self-heals
-- within the current month's reprocessing window under insert_overwrite.
SELECT
    tx_hash,
    block_time,
    block_date AS date,
    safe_address,
    action,
    direction,
    token_symbol,
    token_address,
    counterparty,
    amount,
    amount_usd
FROM classified
WHERE action IS NOT NULL
ORDER BY safe_address, block_time
