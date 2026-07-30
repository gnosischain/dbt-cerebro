---
id: ch-merge-semantics-primer
title: Why ClickHouse has many engines and why duplicates persist — merge-semantics primer
status: primer
scope: >-
  every *MergeTree table in this repo; background for the duplication lessons
  (append-over-populated-duplicates, refill-append-aggregator-inflation,
  wide-delete-insert-wipe) and the insert_overwrite conventions
symptom: n/a (conceptual background — read when duplicates or engine choice confuse you)
last_verified: 2026-07-17
evidence:
  - 'worked examples: the 2026-07 incidents recorded in append-over-populated-duplicates (2x cohorts), refill-append-aggregator-inflation (2x supply, baked), duplicate-seed-drift'
  - models/execution/tokens/intermediate/int_execution_tokens_balances_native_daily.sql:94-99 (why GROUP BY any() instead of FINAL on a 395M-row table)
  - dbt_project.yml (+incremental_strategy: insert_overwrite default; contracts/ append override)
---

## The bargain: speed comes from never checking

An OLTP database enforces UNIQUE by doing an index lookup on every insert — which is
why it cannot ingest millions of rows/second. ClickHouse refuses that check: an INSERT
blindly appends a new immutable sorted file (a "part") and returns. Nothing looked at
existing data, so nothing can enforce uniqueness at insert time. Background **merges**
continually fold parts together — and a merge is the ONLY moment ClickHouse ever holds
two same-key rows side by side, so it is the only place a collision policy can run
cheaply.

## Engines are merge-time collision policies

All *MergeTree engines share identical storage mechanics; they differ only in what a
merge does with two rows having the same sort key:

- `MergeTree` — keep both (duplicates are your problem)
- `ReplacingMergeTree` — keep the latest version ("eventual upsert")
- `SummingMergeTree` — sum the numeric columns
- `AggregatingMergeTree` — combine partial aggregate states
- `CollapsingMergeTree` — cancel +1/−1 sign pairs

There is no universally right collision policy — it is application semantics — so
ClickHouse pushes the choice into the table definition.

## The three traps (each has bitten this repo)

1. **Merges are lazy.** Between insert and merge, BOTH copies are live.
   ReplacingMergeTree is a promise duplicates die *someday*, not a unique constraint.
   Force resolution with `FINAL` (merge-at-read — ruinously slow on big tables; see
   the native balances model's seed comment) or `OPTIMIZE ... PARTITION ... FINAL
   DEDUPLICATE` (whole-partition rewrite — treat as an explicit barrier step).
2. **FINAL-less readers count both copies.** `sum()`/`countIf()` over an unmerged RMT
   window double-counts — the append-over-populated-duplicates incident (weekly
   revenue cohorts exactly 2x).
3. **Computed values freeze the duplication.** An aggregator that reads its source
   while both copies are live writes `sum(2x)` into its own row; the source later
   merges clean, but the aggregator's doubled row IS the latest version per key and
   every future merge faithfully preserves it. Duplication laundered into one
   clean-looking wrong row — invisible to `dup_excess` checks, caught only by
   value-level reconciliation (refill-append-aggregator-inflation incident: every
   token's July supply exactly 2x with zero duplicate rows).

## Why the repo conventions look the way they do

All the house rules are the same move — make writes IDEMPOTENT instead of trusting
eventual dedup:

- `insert_overwrite` default: REPLACE PARTITION is an atomic swap, rerun-safe —
  sidesteps merge semantics entirely. Price: partition grain must equal the write
  grain (staged-insert-overwrite-wipe).
- `append` only into provably empty windows (the staged orchestrator's guarantee).
- `delete+insert` only with small delete-sets; mutations are background and
  non-transactional (wide-delete-insert-wipe).
- OPTIMIZE is part of the write protocol on append refills, and an aggregator never
  runs in the same dbt invocation as its source's append.
- Uniqueness is a TEST (`count() - uniqExact(key)`), not a constraint — and pair it
  with value-level boundary-ratio checks, because dup_excess = 0 can coexist with
  everything being exactly 2x.

Nuance: ClickHouse does dedup byte-identical insert *blocks* for a while (replay
protection for crashed loaders). That is the only insert-time dedup that exists — it
does not make inserts idempotent in general.
