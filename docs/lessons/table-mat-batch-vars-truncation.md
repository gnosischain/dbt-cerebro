---
id: table-mat-batch-vars-truncation
title: Table-materialized models with month-var branches get truncated to the last batch
status: observed
scope: >-
  materialized='table' models whose SQL branches on start_month/end_month vars,
  when run via batched refresh.py (live instances exist — find them via manifest:
  table materialization + start_month in raw SQL)
symptom: after a batched refresh, the table holds only the final batch's window
last_verified: 2026-07-17
evidence:
  - scripts/full_refresh/refresh.py run_model_batched (~408-520) — every batch runs dbt run --vars {start_month,end_month}; for a table materialization each batch is a full CREATE/REPLACE, so only the last window survives
  - models/execution/Circles/intermediate/int_execution_circles_v2_mint_activity_daily.sql:25-27 — in-code statement of this exact failure (that model dropped its month-var branch for this reason)
  - 'live instances still carrying the risk: models/execution/gpay/intermediate/int_execution_gpay_wallets.sql (:3 table, :29-31 branch), models/execution/gnosis_app/intermediate/int_execution_gnosis_app_user_activity_daily.sql (:3 table, :29-31,:47-49,:64 branches)'
---

## Symptom
A `table` model shows only recent months after a "full-history" batched refresh.

## Root cause
`materialized='table'` rebuilds the whole table every run. The orchestrator's batching
(`--vars {start_month,end_month}` per batch) assumes incremental append semantics; on a
table model each batch replaces the previous batches entirely.

## Forbidden action
Don't run a table-materialized model through the batched/staged refresh path; don't
add `meta.full_refresh` staging to a table model without converting it to incremental
(or removing the month-var branch, as circles_v2_mint_activity_daily did).

## Detection
`min(date)` of the table vs its configured history start after any batched run.

## Safe remediation
A plain `dbt run -s <model>` self-heals (the no-vars branch builds full history).

## Ground truth
The model's own no-vars SELECT row span.

## Enforcement
None automatic — flagged in the change packet for table models with month-var
branches; candidates should either drop the branch or convert to incremental.
