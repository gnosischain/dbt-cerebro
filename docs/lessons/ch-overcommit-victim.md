---
id: ch-overcommit-victim
title: Code 241 with a "(total)" server-wide limit means your query is the victim, not the culprit
status: remediated
scope: any model failing with MEMORY_LIMIT_EXCEEDED / OvercommitTracker errors on
  ClickHouse Cloud during busy cron windows
symptom: a routinely-green model suddenly fails Code 241; the error cites the server
  total limit, not a per-query limit
last_verified: 2026-07-17
evidence:
  - README.md:575,720 — orchestrator treats "Code 241 ... OvercommitTracker decision (cluster contention)" as transient and retries with exponential backoff
  - ~26 models carry pre_hook SET max_memory_usage = N with post_hook reset = 0 (e.g. models/execution/tokens/intermediate/int_execution_tokens_balances_daily.sql:23,29); commit 1e34b402 (2026-07-13, "OOM fixes and API metadata")
  - note: the exact fingerprint '(total) ... maximum X GiB' + 'allocate chunk 0.00 B' is experiential (not recorded in a repo artifact)
---

## Symptom
A model that normally builds in seconds–minutes fails with Code 241 during a busy
window. The message references the server-wide (`total`) cap and often a tiny/zero
allocation size.

## Root cause
ClickHouse Cloud's OvercommitTracker kills a query when the **server** is saturated —
the killed query can be small. It's a victim of concurrent load (cron batches), not a
fat query.

## Forbidden action
Don't rewrite/split a model on the basis of a single 241-during-cron failure; don't
treat per-model `max_memory_usage` as a cure (it bounds your query, it cannot free the
server — it's hygiene that makes you a smaller victim/culprit).

## Detection
The `(total)` wording in the error; the same model building green when the cron is
idle; skipped downstreams self-healing next cron.

## Safe remediation
Retry when the cluster is idle. If a model genuinely is fat, bound it via
`query_settings=` (memory cap, external group-by/sort spill) — never `settings=`
(storage-only).

## Ground truth
`system.query_log` memory usage for the run vs the server cap; whether concurrent cron
queries were running.

## Enforcement
The orchestrator auto-classifies Code 241/OvercommitTracker as transient and retries
with backoff (README "Running Models" / observability sections); per-model memory
hygiene landed in 1e34b402.
