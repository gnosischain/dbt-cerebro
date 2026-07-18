---
id: refill-append-aggregator-inflation
title: An aggregator run in the same dbt invocation as its source's append reads 2x rows
status: remediated
scope: >-
  any append-rewrite refill over ReplacingMergeTree models where one selected
  model aggregates another selected model (canonical: the tag:refill_append
  cohort — int_execution_tokens_supply_holders_daily and 6 other aggregators
  over int_execution_tokens_balances_daily); scripts/maintenance/refill_after_price_gap.sh
symptom: >-
  aggregate metrics (supply, cohort counts, sector totals, fee sums) exactly
  2x for the refilled window across every entity; row-level dup checks pass
  (dup_excess = 0) because the VALUES are doubled, not the rows
last_verified: 2026-07-17
evidence:
  - '2026-07-17 incident: after refill_after_price_gap.sh --from-date 2026-07-13, every whitelisted token''s July supply read exactly 2x (ratio ~2.0 at the 06-30->07-01 boundary, ~26 tokens); int-layer dup_excess was 0 everywhere'
  - 'mechanism: within one dbt invocation, DAG order runs the source append (int_execution_tokens_balances_daily gains a second live July copy) BEFORE the aggregators compute — OPTIMIZE only runs after the invocation, so aggregators bake sum(2x rows); their doubled row then survives every merge as the latest version per key. The script''s old pass-B re-appended sources again, recreating the same 2x state it was designed to clear'
  - 'canary blind spot: Phase 1.5 scanned GNO supply +/-7d around --from-date (2026-07-13); the doubling sat at the month boundary and was uniform inside July -> no intra-window jump, canary passed'
  - 'sibling instance: the 2026-07-02 OC-sDAI backfill left Apr/May/Jun partitions of int_execution_tokens_balances_daily with every OC-sDAI row exactly twice (352/5,287/16,026 dup rows == 100% of those partitions'' dup_excess) and no OPTIMIZE — OC-sDAI supply read 2x native for its entire history'
  - 'fix: pass C/D re-ran the 7 aggregators alone against merged sources + OPTIMIZE; script rewritten to per-month "sources-then-OPTIMIZE-then-aggregators-then-OPTIMIZE" with a split-membership guard (scripts/maintenance/refill_after_price_gap.sh)'
---

## Symptom
After an append-mode refill, every aggregate over the refilled window is exactly 2x —
uniformly across entities — while row-level duplicate checks on the aggregate tables
pass. Charts show a clean step at the refill window's boundary (often a month edge).

## Root cause
ReplacingMergeTree merges are lazy. A scoped append writes a second full copy of the
window; until OPTIMIZE (or a background merge) collapses it, both copies are live to
any reader without FINAL. dbt's DAG ordering guarantees the aggregator runs AFTER its
source's append within the same invocation — i.e. at the worst possible moment. The
aggregator's doubled output row is the newest version per key, so later merges keep it.

## Forbidden action
Never select a source model and an aggregator that reads it in the same append-mode
dbt invocation. Never trust `count() - uniqExact(key)` alone to clear an aggregate
table — it catches row duplication, not value inflation baked at compute time.

## Detection
Value-level ratio scan: adjacent-day ratio per entity across the refill window's
boundaries (a uniform 2x shows only at the edges). Cross-layer check: aggregate vs
recompute from its source (e.g. supply vs sum of balances). tests/data_quality/
dq_weekly_sparse_series_density-style checks don't catch this — add a boundary-ratio
canary spanning every affected month boundary, not a fixed window around one date.

## Safe remediation
Re-run ONLY the aggregators for the affected months (their sources must already be
merged/clean), then OPTIMIZE each touched partition. Physical row duplicates in a
source partition (append without OPTIMIZE, the OC-sDAI case) need
`OPTIMIZE TABLE ... PARTITION ... FINAL DEDUPLICATE` first. Then rebuild downstream
fct_*/api_* (they baked the inflated values as tables).

## Ground truth
Recompute the aggregate directly from its source table for a sample of
(entity, day) and compare; for supply, the native chain total is the anchor.

## Enforcement
refill_after_price_gap.sh rewritten (sources-then-aggregators per month, OPTIMIZE
between, split-membership guard, boundary-spanning canary). Not yet a CI/test gate —
a supply-vs-balances reconciliation test would move this to enforced.
