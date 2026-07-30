---
id: microbatch-state-skips-data-holes
title: >-
  Microbatch stages that "complete" without landing rows are skipped forever by
  the state watermark, freezing partial-band coverage that biases every
  downstream aggregate
status: observed
scope: >-
  meta.full_refresh staged microbatch models run by
  scripts/refresh/dbt_incremental_runner.py — especially multi-band models
  (validator_index ranges) whose consumers aggregate whatever rows exist
  (int_consensus_validators_income_daily -> per_index_apy -> dists_daily ->
  fct/api_consensus_info marts)
symptom: >-
  A latest-day mart serves a wildly wrong aggregate (negative network APY,
  spec-cap-pinned APY ~35.7) while all value-level tests pass; per-day row
  counts show one 100k validator band instead of ~558k rows
last_verified: "2026-07-26"
evidence:
  - "dbt.int_consensus_validators_income_daily coverage by day: full 558k rows through 2026-07-07; band 0-100k only 2026-07-08..19; full 07-20..22; single (different) band each on 07-23/24/25 (4,118-row day drove avg_apy to -0.91 in int_consensus_validators_dists_daily on 2026-07-25)"
  - "Reproduction 2026-07-26/27: recovery run of 150 slices reported all-success but landed band-0 rows only; identical band-1 slice (incremental_end_date 2026-07-01, validator_index 100000-200000) inserted 0 rows before and 100,000 rows after adding filters_sql=range_sql to the network_state CTE (models/consensus/intermediate/int_consensus_validators_income_daily.sql)"
  - "2026-07-20 pinned at spec cap: 56,340 of 56,564 validators (index >= 100000, balance > 0) at apy ~ 35.7 after balance_prev fell back to 0 across the hole; validator 150000 cumulative_withdrawals_gno reset 0.247 -> 0 the same day"
  - "scripts/refresh/dbt_incremental_runner.py plan_for_model: floor = max(data_watermark+1, state.last_completed_end_date+1) — state can only advance the floor, so a slice that succeeded without landing rows is never re-planned (fix in tree: heal_lookback_days, pending deploy)"
  - "tests/consensus_income_daily_coverage.sql failed with 4 offending days against the pre-fix state on 2026-07-26 (new test, pending deploy)"
  - "docs/model_review/ + memory: consensus.validators crawler halted 2026-06-07 before; ingestion-time races leave a nightly slice reading a partially-crawled day"
---

## Symptom

A consensus mart that aggregates per-validator rows goes insane while every
value-level test stays green: network avg APY turns negative (−0.91 on
2026-07-25), or a whole day pins at the spec-cap APY (~35.7 on 2026-07-20).
Per-day `count()` on `int_consensus_validators_income_daily` shows ~100k rows
(one validator band) instead of ~558k, or a different single band on each
recent day.

## Root cause

Two stacked failures (both CONFIRMED during the 2026-07-26 recovery, which
reproduced the exact production symptom: 150/150 slices "successful", band 0
rows only):

1. **An unscoped watermark CTE turns later-band slices into silent no-ops.**
   The income model's `network_state` CTE called
   `apply_monthly_incremental_filter` WITHOUT `filters_sql=range_sql`. The
   macro's watermark subquery runs against `{{ this }}`, so it read the
   GLOBAL `max(date)` of the income table. On the microbatch path the first
   band stage to insert day D advances that global max to D; every later
   band's slice then compiles `network_state` to `date > D AND date <= D` →
   empty → the INNER JOIN wipes the whole insert → 0 rows, dbt reports
   success. Reproduced: a band-1 slice inserted 0 rows pre-fix and 100,000
   rows post-fix with the identical command. The bug was latent since the
   microbatch tag landed (2026-04-28, cb23208c) but masked while the cron
   also ran a plain delete+insert pass (its 3-day lookback refilled all
   bands nightly); it went live when the cron moved to runner-only slicing
   around 2026-07-08.
2. **The runner's state watermark makes the hole permanent**: `plan_for_model`
   takes `floor = max(data_watermark + 1, last_completed_end_date + 1)`. State
   can only move the floor FORWARD, so once a slice is marked completed the
   runner never looks at that day again, even when the target table
   demonstrably has no rows for it. Coverage froze at 1-of-6 bands for 12
   consecutive days.

The corruption then compounds: a later catch-up rebuild of 07-20..22 found no
07-19 rows for bands ≥100k, so `balance_prev` fell back to 0, the
effective-credit formula absorbed each validator's entire balance as a phantom
deposit, income pinned at the spec cap for 56k validators, and every
`cumulative_*` column restarted from zero (prev_state JOIN missed).

Downstream amplification: `int_consensus_validators_per_index_apy_daily` is an
INNER JOIN of snapshots × income, and `int_consensus_validators_dists_daily`
averages whatever rows exist with only an upper outlier cut (`apy < 200`). A
surviving band that happens to be a mostly-exited cohort (its stragglers bleed
annualized inactivity penalties, APY down to −69) drags the day's mean
negative. `fct_consensus_info_latest` then serves that day as "the" APY.

## Forbidden action

- Do not treat a green nightly runner log as proof a microbatch day is
  complete — completion state and landed rows are different facts.
- In a range-sliced microbatch model, never call
  `apply_monthly_incremental_filter` without `filters_sql=range_sql` — even in
  a CTE whose SOURCE has no range column. The range scope applies to the
  `{{ this }}` watermark subquery; without it the CTE watermarks off the
  global frontier and later-band slices go empty.
- Do not "fix" a partial day by re-running only the newest slices; the
  income model is cumulative (`{{ this }}` prev_state), so holes must be
  rebuilt chronologically per band from the last clean partition boundary.
- Do not diagnose the negative/pinned APY as a formula bug — the spec-cap and
  ledger-identity tests pass on corrupted days; check population coverage
  first.

## Detection

- `tests/consensus_income_daily_coverage.sql` — per-day
  `uniqExact(validator_index)` parity between income and snapshots (fails on
  any day below 99% coverage in the test lookback window).
- Ad hoc: `SELECT date, count(), min(validator_index), max(validator_index)
  FROM dbt.int_consensus_validators_income_daily GROUP BY date` — a
  min/max span of exactly one band is the signature.
- The runner (fixed version) prints a `[warn] … state watermark … ahead of
  the data watermark` line when state claims days the target does not hold.

## Safe remediation

Damage is contained in whole monthly partitions (partition_by =
`toStartOfMonth(date)`), and the income model's prev_state chains from the
prior partition boundary, so:

1. `dbt run-operation drop_partition` for the affected month on
   `int_consensus_validators_income_daily` AND
   `int_consensus_validators_per_index_apy_daily` (verify the partition value
   with `list_partitions` first).
2. Rebuild chronologically per band with the microbatch runner (income first,
   then per_index — per_index INNER JOINs income, so a per_index slice run
   before its income slice lands nothing):
   `python scripts/refresh/dbt_incremental_runner.py --select
   int_consensus_validators_income_daily …` capped with `--max-end-date` at
   yesterday so an empty today-slice never poisons the state watermark.
3. Rebuild downstream: monthly `insert_overwrite` models
   (`int_consensus_validators_dists_daily`,
   `int_consensus_validators_apy_dist_income_daily`,
   `fct_consensus_validators_explorer_daily`) self-heal the whole month on a
   plain run; `int_consensus_validators_explorer_apy_dist_daily` needs its
   month partition dropped + a `start_month`/`end_month` run; tables/views
   (`fct_consensus_validators_apy_mean_daily`, `fct_consensus_info_latest`,
   api marts) rebuild plain.

## Ground truth

- Income grain is one row per (date, validator_index) for EVERY validator in
  the day's snapshot (including exited/zero-balance): daily row count must
  match `int_consensus_validators_snapshots_daily` near-exactly (~558k).
- The negative-APY tail on complete days (q05 ≈ −6) is REAL (offline
  validators, annualized penalties); with full population the daily mean sits
  at ~+8. A negative MEAN is always a population artifact.

## Enforcement

- Model fix (pending deploy): `filters_sql=range_sql` on the income model's
  `network_state` CTE — all other range-sliced consensus models were audited
  and already scope every filter call.
- `tests/consensus_income_daily_coverage.sql` (pending deploy) catches any
  partial day within the test lookback window.
- Runner `--heal-lookback-days` (default 3, pending deploy): when state is
  ahead of the data watermark, the trailing N state-claimed days are
  re-planned so a silently-empty slice self-heals within N days; older holes
  warn and need a manual backfill. Unit tests:
  `tests/test_dbt_incremental_runner.py::test_plan_for_model_state_ahead_of_data_heals_within_lookback`
  and siblings.
