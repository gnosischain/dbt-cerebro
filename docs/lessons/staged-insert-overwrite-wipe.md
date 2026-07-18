---
id: staged-insert-overwrite-wipe
title: Staged/batched refreshes against insert_overwrite models wipe months via REPLACE PARTITION
status: enforced
scope: models with meta.full_refresh stages or start_month/incremental_end_date var
  branches whose strategy could resolve to insert_overwrite; macros/db/get_incremental_filter.sql
symptom: after a staged refresh, a month partition holds only the last stage/slice
  (e.g. a whole month reduced to one day)
last_verified: 2026-07-17
evidence:
  - commit 267e04bf (2026-06-19, "fix: insert_overwrite") — get_incremental_filter.sql reordered so the insert_overwrite branch wins over the microbatch branch even when incremental_end_date is set; diff comment "Letting the microbatch branch below win here is what wiped a whole month down to one day under REPLACE PARTITION"
  - commit 641b5aae (2026-06-26, "fix insert overwrite incrementals") — follow-up
  - commit 4ae47913 (2026-07-09) — consensus withdrawals/proposer flipped to ('append' if start_month else 'insert_overwrite') after a staged refresh.py run wiped them to last-stage-only
  - macros/db/get_incremental_filter.sql:1-24 (header documents all three strategy branches and the wipe mechanism)
  - models/consensus/intermediate/int_consensus_validators_withdrawals_daily.sql:9 and int_consensus_validators_proposer_rewards_daily.sql:9 (strategy expressions)
---

## Symptom
A staged or windowed refresh "succeeds", and afterwards the target holds only the last
stage's window — earlier months/days in the same partition are gone.

## Root cause
`insert_overwrite` REPLACEs whole partitions. Any run that writes a *narrower* window
than the partition grain — a microbatch slice, a `start_month` stage, an
`incremental_end_date`-bounded run — replaces the full partition with just that window.
Three instances (June–July 2026): the microbatch branch winning over insert_overwrite in
the shared filter macro, and staged `refresh.py` batches against insert_overwrite
consensus models.

## Forbidden action
Never combine `insert_overwrite` with staged batches, `slice` vars, or any run window
narrower than the partition. Never partition wider (e.g. by year) on an
insert_overwrite model to dodge partition-count limits — that widens the blast radius
(see ch-partition-cap).

## Detection
Row counts per partition before/after any staged run; a partition whose distinct
date-span shrank is a wipe.

## Safe remediation
Models needing staged backfills use the strategy expression
`('append' if (start_month or incr_end) else 'insert_overwrite')` (narrower
`('append' if start_month ...)` variant on the consensus pair) so scoped windows
append and only full runs REPLACE. Recover wiped windows with the affected-only
runner path (drop partition + re-run scoped) — not a whole-month plain run (OOMs).

## Ground truth
Partition-level row counts vs the upstream source for the affected window.

## Enforcement
Macro branch-order fix (267e04bf, 641b5aae) + append-if-start_month strategy
expressions across staged models (~16 models use the pattern). The wipe mechanism is
documented in the macro header itself. STATIC GATE (2026-07):
`scripts/checks/no_delete_insert.py` rules `staged_literal_overwrite` /
`staged_scoped_branch` fail any meta.full_refresh model whose RAW strategy is a
literal/inherited insert_overwrite or whose scoped branch is not append (raw code,
not resolved config — the manifest collapses the expression to its default branch).
Pre-existing violators are grandfathered shrink-only in no_delete_insert.allow.
