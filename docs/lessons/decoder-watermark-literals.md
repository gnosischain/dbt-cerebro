---
id: decoder-watermark-literals
title: Decode watermarks must be embedded literals — scalar subqueries can't prune partitions
status: enforced
scope: macros/decoding/decode_logs.sql, macros/decoding/decode_calls.sql, and any
  model hand-rolling a watermark WHERE clause
symptom: every decode run reads the full raw-table span (measured 112s/run vs 9s with
  literal bounds); an empty target full-scans on every run
last_verified: 2026-07-17
evidence:
  - macros/decoding/decode_logs.sql — run_query("SELECT max(block_number), toString(max(...)) FROM " ~ this) at render time, embedded as literals; in-macro comment documents the 112s→9s measurement and "Empty/missing target => no watermark"
  - macros/decoding/decode_calls.sql:302-317 — same pattern for traces
  - commit c97369e0 (2026-06-12, "update decoing macros and some models incremental") introduced the literal watermark in both macros
---

## Symptom
Incremental decode runs take minutes when they should take seconds; query log shows
full-span reads of `execution.logs`/traces despite a watermark predicate.

## Root cause
ClickHouse cannot prune partitions from a **scalar-subquery** bound
(`WHERE block_number > (SELECT max(...) FROM this)`); the predicate is evaluated too
late for partition pruning. Embedding the values as literals at render time (via
`run_query` during compilation) restores pruning.

## Forbidden action
Don't "simplify" the macros back to a scalar subquery; don't hand-roll incremental
predicates with subquery bounds on partitioned sources.

## Detection
Decode-model run durations in the cron logs; `system.query_log` read_rows vs the
expected incremental slice.

## Special case: empty target
An empty/missing target yields no watermark, so the run reads the full raw span —
correct for the first seed, pathological if the model stays empty (see
never-seeded-incremental). Seed new decode models once with `--full-refresh`.

## Ground truth
`system.query_log` `read_rows`/`read_bytes` for the decode INSERT.

## Enforcement
The pattern is the macro default since c97369e0 — all decode models inherit it.
