---
id: wide-delete-insert-wipe
title: Wide delete+insert OOMs, then the background delete silently wipes the window
status: enforced
scope: any delete+insert reprocess whose delete-set spans many slices/streams; policy
  applies repo-wide
symptom: dbt reports failure (OOM), but minutes later the target window is empty —
  months of data gone with no error attributing the wipe
last_verified: 2026-07-17
evidence:
  - docs/data-quality-learnings-and-remediation.md (L7 — int_revenue_fees_weekly_per_user whole-window reprocess: Code 341 in CreatingSetsTransform, then system.mutations showed the lightweight delete (_row_exists = 0) completed AFTER dbt failed; 2026-03→07 dropped to single-digit live rows)
  - '2026-07-17 live save: int_execution_tokens_balances_native_daily July reprocess OOMed (Code 341, overcommit victim); system.mutations showed the delete mutation is_done=0, failed=1, NOT killed — i.e. still eligible to retry and wipe. KILL MUTATION cleared it before any rows were masked (July verified intact, 6,269,117 rows). A per-symbol batched re-run replaced the whole-window delete-set'
  - scripts/checks/no_delete_insert.py (repo-wide static gate + allowlist)
  - dbt_project.yml (+incremental_strategy: insert_overwrite global default)
---

## Symptom
A reprocess fails with a memory error — and afterwards the window it targeted is
*empty*, not unchanged. Partitions stay physically bloated (masked rows).

## Root cause
ClickHouse `delete+insert` issues a lightweight delete (`UPDATE _row_exists = 0`) that
keeps executing in the background after the client (dbt) has already reported failure.
The DELETE finishes; the INSERT never ran. On a model that unions many slices, one
whole-window delete-set (`WHERE (key) IN (SELECT … every slice …)`) also OOMs the set
build (Code 341).

## Forbidden action
Never run a wide `delete+insert` — not as a model strategy (CI gate blocks it), not as
a manual repair. Do not assume a failed dbt run left the table unchanged: check
`system.mutations` (`is_done`) after any delete+insert failure.

## Detection
After any failed reprocess: row counts per month vs a pre-run baseline;
`system.mutations` for the failure window. A mutation showing `is_done=0` with a
fail reason and `is_killed=0` is NOT dead — it can retry in the background and
complete the wipe later. `KILL MUTATION` it explicitly before re-running anything
(the runners' pre-flight `kill_failed_mutations` macro does this; plain `dbt run`
does not).

## Safe remediation
Reprocess per `slice` (small delete-sets, e.g. `stream:SYMBOL`) — a 9-slice loop
restored and de-duplicated the incident window with every slice PASS. For partitioned
models prefer `insert_overwrite` (atomic REPLACE PARTITION) where its constraints fit
(see staged-insert-overwrite-wipe for when they don't).

## Ground truth
Pre-incident row counts (query log / baseline) per month.

## Enforcement
`scripts/checks/no_delete_insert.py` in CI blocks the strategy repo-wide (allowlist for
grandfathered cases); per-slice reprocess documented in models/revenue/AGENTS.md.
