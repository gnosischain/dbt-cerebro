---
id: append-over-populated-duplicates
title: Scoped append over already-populated months exactly doubles the data
status: remediated
scope: >-
  windowed-incremental models whose strategy takes the append path when
  start_month is set (canonical: int_revenue_fees_weekly_per_user and the per-stream
  revenue daily models)
symptom: >-
  a metric doubles in one step exactly at the reprocess start_month (rows, sums,
  and non-distinct counts all x2)
last_verified: 2026-07-17
evidence:
  - models/revenue/intermediate/int_revenue_fees_weekly_per_user.sql (strategy expression; fixed in commit c7685d14 "fix model strategy", 2026-07-16)
  - docs/data-quality-learnings-and-remediation.md (L7 — api_revenue_gpay_eure_cohorts_weekly jumped 197,720→397,306 fees / 6,214→12,492 users at 2026-03-02; restored and verified dup_excess = 0)
---

## Symptom
Every cohort/aggregate downstream of the model doubles at one date — the `start_month`
of a scoped re-run.

## Root cause
The `start_month → append` path is only correct into **empty** months (the staged
orchestrator guarantees non-overlap). Re-running scoped `--vars {start_month,end_month}`
over months that already hold rows appends a second full copy. The table is
`ReplacingMergeTree`, but marts read it **without `FINAL`** and aggregate with
`sum()`/`countIf()` — both copies count.

## Forbidden action
Never use the scoped-append path as a "reprocess" — it is a backfill-into-empty tool.

## Detection
Duplicate-excess by month: `count() - uniqExact(<unique_key>)` grouped by month and
stream; any nonzero excess = a doubled window. Downstream tell-tale: a single-step 2×
in a weekly series.

## Safe remediation
Reprocess with `reprocess_overwrite=true` **one `slice` at a time** (see
wide-delete-insert-wipe for why never whole-window). Verify `dup_excess = 0` after.

## Ground truth
Recompute one stream's month directly from its upstream daily model and compare totals.

## Enforcement
Duplicate-key data-quality test (tests/data_quality/); the strategy expression fix in
c7685d14 hardens the model itself.
