---
id: never-seeded-incremental
title: An incremental model created before its input existed stays empty forever
status: remediated
scope: >-
  incremental models whose INNER JOIN input came online after the model's table
  was first created (canonical: int_revenue_ocsdai_user_balances_daily)
symptom: model at 0 rows while its SELECT returns tens of thousands; daily runs "succeed"
last_verified: 2026-07-17
evidence:
  - docs/data-quality-learnings-and-remediation.md (OC-2 — int_revenue_ocsdai_user_balances_daily at 0 rows while its SELECT yielded 32,943 rows / ~$16M; input int_yields_ocsdai_share_price_daily came online after first create)
  - feat/ocsdai program (merge commit 2c4a87fb)
---

## Symptom
An incremental model sits permanently at 0 rows. Every scheduled run passes.

## Root cause
The table was created empty (its join input didn't exist yet). Once empty, the daily
microbatch runner keys off `max(date) FROM this` (epoch 1970) and bootstraps only a few
days back — it can't sanely seed history, and a forward incremental never reaches back.
Anything downstream silently undercounts.

## Forbidden action
Don't trust "the run is green" as evidence a model has data; don't try to fix an empty
incremental by re-running the daily runner harder.

## Detection
Zero-row (or implausibly-low-row) check on incremental models vs their compiled SELECT;
freshness alone won't catch it (there's nothing to be stale).

## Safe remediation
One-time `dbt run --full-refresh -s <model>`, then rebuild its direct downstream chain
(`dbt run -s <model>+`) so the undercount clears.

## Ground truth
Run the model's compiled SELECT ad hoc and compare row counts to the table.

## Enforcement
None automatic yet — creation-order is procedural: when adding a model, build its
inputs first, then seed it with --full-refresh (see /new-model checklist).
