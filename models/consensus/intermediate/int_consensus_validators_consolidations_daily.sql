{% set start_month = var('start_month', none) %}
{% set end_month = var('end_month', none) %}

{{
    config(
        materialized='incremental',
        incremental_strategy=('append' if start_month else 'delete+insert'),
        engine='ReplacingMergeTree()',
        order_by='(date, validator_index, role)',
        unique_key='(date, validator_index, role)',
        partition_by='toStartOfMonth(date)',
        pre_hook=["SET join_algorithm = 'grace_hash'"],
        post_hook=["SET join_algorithm = 'default'"],
        tags=["production", "consensus", "validators_consolidations"]
    )
}}

-- Consolidations (EIP-7251 / MaxEB). See https://notes.ethereum.org/@fradamt/maxeb-consolidation
-- Self-consolidation (source_pubkey == target_pubkey): credential switch 0x01 -> 0x02, no balance transfer.
-- Cross-consolidation (source != target): processed at source's withdrawable epoch; source's full effective
-- balance transfers to target and source exits. We emit the row on the APPLICATION day, inferred as the
-- first day after the request where the source validator's effective balance reaches 0. If application has
-- not yet occurred, no row is emitted (a later run will pick it up).
-- Incremental monthly by request_date so the source_snapshot join stays within the cluster's
-- 10.8 GiB memory cap. Request dedup happens upstream in
-- int_consensus_validators_consolidation_requests (which is a small, full-table-rebuild model
-- — dedup there, snapshot join here).

WITH

-- Deduped requests: see int_consensus_validators_consolidation_requests for the rationale
-- (beacon-chain FIFO processing + operator resubmissions). Applying the monthly window here
-- — the upstream model holds the full-history dedup state.
requests AS (
    SELECT request_slot, request_date, source_pubkey, target_pubkey
    FROM {{ ref('int_consensus_validators_consolidation_requests') }}
    WHERE request_date < today()
    {% if start_month and end_month %}
      AND toStartOfMonth(request_date) >= toDate('{{ start_month }}')
      AND toStartOfMonth(request_date) <= toDate('{{ end_month }}')
    {% else %}
      {{ apply_monthly_incremental_filter('request_date', 'date', 'true') }}
    {% endif %}
),

-- Pubkey→validator_index resolution. Uses fct_consensus_validators_status_latest
-- (which reads from stg_consensus__validators_all) instead of int_consensus_validators_labels
-- because labels filters on balance > 0 and therefore excludes exited validators —
-- and the whole point of tracking consolidation SOURCES is that they exit after
-- their balance transfers to the target. Filtering out exited sources dropped every
-- cross-consolidation request on busy application days (observed: 301/302 sources
-- missing on 2025-11-18, producing -533k GNO of spurious "income" loss in aggregate
-- across ~300 orphaned balance-drops).
--
-- Restrict to ONLY the pubkeys that actually appear in this batch's requests. Without
-- this filter the LEFT JOINs below build hash tables of every validator on the network
-- (hundreds of thousands of pubkey strings) — twice — which OOM'd on high-volume
-- batches (e.g. Aug 2025, a peak 0x00→0x02 migration month).
labels AS (
    SELECT validator_index, lower(pubkey) AS pubkey
    FROM {{ ref('fct_consensus_validators_status_latest') }}
    WHERE lower(pubkey) IN (
        SELECT source_pubkey FROM requests
        UNION DISTINCT
        SELECT target_pubkey FROM requests
    )
),

resolved AS (
    SELECT
        req.request_slot
        ,req.request_date
        ,req.source_pubkey
        ,req.target_pubkey
        ,ls.validator_index AS source_validator_index
        ,lt.validator_index AS target_validator_index
    FROM requests req
    LEFT JOIN labels ls ON ls.pubkey = req.source_pubkey
    LEFT JOIN labels lt ON lt.pubkey = req.target_pubkey
),

-- Self-consolidations: credentials-only, emit on request day, no amount.
self_rows AS (
    SELECT
        request_date AS date
        ,source_validator_index AS validator_index
        ,'self' AS role
        ,CAST(NULL AS Nullable(UInt64)) AS counterparty_validator_index
        ,CAST(0 AS Float64) AS transferred_amount_gno
        ,1 AS cnt
    FROM resolved
    WHERE source_pubkey = target_pubkey
      AND source_validator_index IS NOT NULL
),

-- Cross-consolidations: find application day per source validator = first day at/after request where
-- the source's effective_balance_gwei first becomes 0. Transferred amount = the last non-zero effective
-- balance observed strictly before that day (captured via argMax over date when balance>0).
cross_requests AS (
    SELECT *
    FROM resolved
    WHERE source_pubkey != target_pubkey
      AND source_validator_index IS NOT NULL
      AND target_validator_index IS NOT NULL
),

-- Pre-filter snapshots to only the source validators that appear in cross_requests and
-- a TIGHT window around EACH source's request. Using global min/max over cross_requests
-- produces a year-wide window that OOMs the cluster when the model is built as a full
-- table (the 10.8 GiB memory cap chokes on ~12 months of snapshot data × ~10k sources).
-- Instead, derive (validator_index, request_date) per source, then filter the snapshot
-- table with an IN-tuple so each source only pulls ±37 days of its own history.
-- Gnosis's churn limit applies cross-consolidations within hours to a few days of the
-- request; a 30-day upper bound is generous and the 7-day pre-request lookback keeps the
-- "last non-zero effective balance" lookup correct for same-day applications.
source_snapshots AS (
    SELECT validator_index, date, effective_balance_gwei, balance_gwei
    FROM {{ ref('int_consensus_validators_snapshots_daily') }}
    WHERE validator_index IN (SELECT source_validator_index FROM cross_requests)
      AND date >= (SELECT min(request_date) FROM cross_requests) - INTERVAL 7 DAY
      AND date <= (SELECT max(request_date) FROM cross_requests) + INTERVAL 30 DAY
),

applications AS (
    SELECT
        cr.request_slot
        ,cr.source_validator_index
        ,cr.target_validator_index
        ,minIf(s.date, s.effective_balance_gwei = 0 AND s.date >= cr.request_date) AS application_date
        -- v5 (2026-04): transferred amount = source validator's last-observed REAL balance
        -- (balance_gwei), not effective balance. Effective balance is a step function (32-GNO
        -- multiples pre-Pectra) that over-reports the real mass moved on consolidation when
        -- the source carried inactivity penalties. Using balance_gwei makes the target's
        -- consolidation_inflow match the target's actual balance delta, eliminating the
        -- residual -10 to -120 GNO per-target "loss" days seen in v4. Gate on eb>0 rather
        -- than balance>0 because balance can be microscopically non-zero due to pending
        -- penalties while effective balance is already 0 — the last row with eb>0 is the
        -- last row where the validator was still "active" from the spec's perspective, and
        -- its real balance on that row is what transfers.
        ,argMaxIf(s.balance_gwei, s.date, s.effective_balance_gwei > 0) AS transferred_amount_gwei
    FROM cross_requests cr
    INNER JOIN source_snapshots s
        ON s.validator_index = cr.source_validator_index
    GROUP BY 1, 2, 3
),

cross_rows AS (
    SELECT
        application_date AS date
        ,source_validator_index AS validator_index
        ,'source' AS role
        ,toNullable(target_validator_index) AS counterparty_validator_index
        ,transferred_amount_gwei / POWER(10, 9) AS transferred_amount_gno
        ,1 AS cnt
    FROM applications
    WHERE application_date IS NOT NULL

    UNION ALL

    SELECT
        application_date AS date
        ,target_validator_index AS validator_index
        ,'target' AS role
        ,toNullable(source_validator_index) AS counterparty_validator_index
        ,transferred_amount_gwei / POWER(10, 9) AS transferred_amount_gno
        ,1 AS cnt
    FROM applications
    WHERE application_date IS NOT NULL
)

-- v6 (2026-04): dedupe requests per source_pubkey in the `requests` CTE above. Before v6,
-- operator resubmissions (duplicate consolidation-request events for the same source) were
-- all treated as real consolidations. Scale of the bug: on 2025-10-06 target 548367 had
-- 449 raw requests but only 109 unique source pubkeys — inflating target inflow from the
-- true ~1,792 GNO to a model value of 14,342 GNO, leaving 12,550 GNO of "phantom" inflow
-- that had to be attributed to negative income by the ledger identity. This cascaded into
-- network-wide -36,000 GNO income days. Per EIP-7251 the beacon chain processes the FIRST
-- valid request per source and rejects resubmissions; v6 models that behaviour.
-- Also switched to a plain table materialisation since the dedup needs global
-- ROW_NUMBER() visibility that monthly-incremental partitioning can't provide.
--
-- v5 (2026-04): use source's real balance (balance_gwei), not effective_balance_gwei, for
-- transferred_amount. This models what the source actually held when consolidating, rather
-- than the spec's 32-GNO-multiple step-function view.
--
-- KNOWN SECONDARY LIMITATION: v5 trades one rounding direction for another — it does NOT
-- fully eliminate the per-target residual on heavy-consolidation days:
--   * Under v4 (effective_balance): dormant sources carrying inactivity penalties had
--     effective_balance > real_balance, so the target received LESS than recorded —
--     target's income showed a small NEGATIVE residual.
--   * Under v5 (real balance): active sources earning rewards between their last end-of-day
--     snapshot and the application slot have real_balance_at_snapshot < real_balance_at_slot,
--     so the target receives MORE than recorded — but we also miss the sweep that happens
--     between snapshot and application which RETURNS excess-over-effective to the source's
--     withdrawal address rather than the target. Observed example: validator 35407 on
--     2026-04-14, 47 sources consolidating, transfer recorded at 1563 GNO, actual target
--     balance delta 1407 GNO, residual -156 GNO attributed to income.
--
-- Fully closing this residual requires measuring transfer amount from the TARGET's actual
-- balance delta on the application day rather than from the source's observed balance —
-- a larger redesign of this model. For now:
--   * Mass-balance invariant still holds (source_out = target_in per date) — guaranteed
--     by construction since both sides read from the same applications CTE.
--   * Network-level income_gno remains stable and correct (validated via
--     fct_consensus_validators_income_total_daily).
--   * tests/consensus_income_within_spec_cap.sql retains the consolidation-inflow
--     carve-out until the target-side measurement redesign lands.
--
--
-- v4 (2026-04): BUG FIX — aggregate at the ReplacingMergeTree unique-key grain.
--
-- The table's unique_key=(date, validator_index, role). Previously the final SELECT
-- grouped by (date, validator_index, role, counterparty_validator_index), emitting one
-- row per distinct counterparty. When N sources consolidated into the same target on
-- the same application day (common: 55 legacy 0x01 validators → one 0x02 target), the
-- model emitted N target rows sharing the unique key; ReplacingMergeTree then kept
-- only one of them, silently dropping (N-1) × 32 GNO of target inflow.
--
-- Observed impact: 2025-11-17 had 16,729 source rows (535,328 GNO outflow) but only
-- 305 target rows (9,760 GNO inflow) — 98% of target credit missing. Downstream,
-- int_consensus_validators_income_daily's spec-cap formula absorbed the missing credit
-- into effective_deposits_credited_gno, corrupting per-validator income attribution.
--
-- Fix: group strictly at the unique-key grain. transferred_amount_gno becomes the SUM
-- across all counterparties; cnt becomes the count of counterparties;
-- counterparty_validator_index becomes a representative example (any()) — fidelity is
-- lost but the column was informational only (no downstream consumer relies on it).
-- Mass-balance invariant restored:
--   SUMIf(transferred_amount_gno, role='source') = SUMIf(transferred_amount_gno, role='target')
-- per date, verified by tests/consensus/test_consolidations_mass_balance.sql.
SELECT
    date
    ,validator_index
    ,role
    ,any(counterparty_validator_index) AS counterparty_validator_index
    ,SUM(transferred_amount_gno) AS transferred_amount_gno
    ,SUM(cnt) AS cnt
FROM (
    SELECT * FROM self_rows
    UNION ALL
    SELECT * FROM cross_rows
)
GROUP BY 1, 2, 3
