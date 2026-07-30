---
id: frontier-day-incomplete-inputs
title: A cumulative chain that builds the frontier day before its inputs settle freezes the hole
status: remediated
scope: >-
  cumulative carry-forward models whose daily slice is built from same-morning
  upstream data (canonical: int_execution_tokens_balances_native_daily reading
  int_execution_tokens_address_diffs_daily); any layer joining a
  possibly-late-settling input at the frontier (prices -> the USD balances layer)
symptom: >-
  a burst of negative real-holder balances (or 100% NULL joined values) all
  dated to ONE recent day; upstream models look complete when you inspect them
  later, because they self-healed and the cumulative layer did not
last_verified: 2026-07-17
evidence:
  - '2026-07-17 incident: dq_daily_negative_real_holder_balances found 201 negatives, ALL dated 2026-07-16, across 11 tokens'
  - '2026-07-15 was a near-dead day in the balances chain: EURe had 3,330 addresses with transfers but only 151 balance changes; GBPe 79 vs 0; WxDAI 1,301 vs 53'
  - 'mechanism proof: for sampled addresses model_0715 == model_0714 EXACTLY (zero net integrated), while int_execution_tokens_address_diffs_daily held the correct 07-15 delta (+2,559.09 EURe for 0x2cbe..., matching raw execution.logs Int256 reconstruction)'
  - 'transfers/diffs self-healed because they recompute the latest month (insert_overwrite); the balances incremental branch only generates dates AFTER max(date), so the frozen day is never revisited'
  - 'repair: model-documented reprocess_overwrite window (start_month=end_month=2026-07-01), 471s run, July dup_excess stayed 0, negatives 201 -> 11 (the 11 are pre-July deficits, older class)'
  - 'sibling instance same week (the USD layer): int_execution_tokens_balances_daily 2026-07-13/14 built before those days'' prices landed -> 100% NULL balance_usd both days; repaired via scripts/maintenance/refill_after_price_gap.sh --from-date 2026-07-13'
---

## Symptom
A one-day burst of impossible values at (or near) the frontier: negative real-holder
balances, or a day where a joined attribute (USD price) is NULL/default for ~100% of
rows. Days on either side look normal. By the time you investigate, the upstream
models look complete — they healed on a later run.

## Root cause
The cron builds the cumulative model's day-(D) slice on the morning of D+1. If the
inputs for D (decoded transfers/diffs, or the price feed) had not fully landed at
build time — raw ingestion lag, an upstream batch failing in a *separate* dbt
invocation (batch isolation breaks DAG failure propagation), or a source that
publishes late — the slice integrates a partial (or empty) day.

The asymmetry is the trap: upstreams that recompute their latest month
(insert_overwrite) self-heal on the next run, but a cumulative carry-forward only
generates dates after its own `max(date)` — the frozen day is never revisited, and
every subsequent day inherits the deficit.

## Forbidden action
Don't "fix" the negative rows in place, and don't assume the upstream was at fault —
by inspection time the upstream is usually correct; the cumulative layer holds the
stale integration. Don't re-run the daily runner harder (it only appends forward).

## Detection
dq_daily_negative_real_holder_balances (tests/data_quality/) catches the cumulative
case; a per-day activity comparison localizes the frozen day: addresses with
transfers vs addresses whose balance changed, per (symbol, date) — the frozen day
shows a collapse in the ratio. For join-layers: % NULL of the joined column per day.

## Safe remediation
- Native cumulative chain: the model's own documented reprocess window —
  `dbt run -s int_execution_tokens_balances_native_daily --vars '{start_month: <month>,
  end_month: <month>, reprocess_overwrite: true}'` (delete+insert atomically replaces
  the month partitions; the seed reads the last good day BEFORE the window).
  Verify `dup_excess = 0` on the partition before and after.
- Price-join layer: `scripts/maintenance/refill_after_price_gap.sh --from-date <gap
  start>` (two-pass append + OPTIMIZE DEDUPLICATE per month, then rebuilds the
  downstream fct_*/api_* — do NOT use bare start_month vars: append without OPTIMIZE
  duplicates).
- Always fix the native layer BEFORE re-pricing the USD layer, or the refill bakes
  wrong native balances into USD.

## Ground truth
Raw `execution.logs` Int256 per-address reconstruction for the frozen day; on-chain
balanceOf at the day's last block.

## Enforcement
Detection is wired (daily data-quality tests). Recompute remains manual. Durable-fix
candidates (proposed): make the balances incremental branch re-integrate a trailing
N-day window each run (needs dup-safety analysis under delete+insert), or gate the
cron's balances batch on an input-completeness check (diffs row count for D within
tolerance of the trailing average).
