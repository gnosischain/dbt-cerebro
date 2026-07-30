---
id: global-frontier-carry-forward
title: Global-frontier carry-forward silently drops thin series
status: enforced
scope: >-
  event-driven daily models that forward-fill state off a shared max(date) frontier
  (canonical: int_execution_pools_balancer_v3_daily; any calendar-spine + carry-forward model)
symptom: >-
  sparse per-entity date coverage that tracks the entity's activity frequency;
  gaps surface raw in downstream marts that don't re-densify
last_verified: 2026-07-17
evidence:
  - models/execution/pools/intermediate/int_execution_pools_balancer_v3_daily.sql (per-(pool,token) frontier anchor; fixed in commit 4cc8c608 "fix balancer v3 trades", 2026-07-16)
  - docs/data-quality-learnings-and-remediation.md (L8 — Circles s-gCRC/sDAI pool 0x155c…f1a1 at 5/48 days; density tracked trade frequency, 0.93 for active pools vs 0.10–0.55 for thin ones; restored to 48/48, density 1.0)
---

## Symptom
A daily state series (reserves, balances) has holes for exactly the entities that trade
or emit events infrequently. Density (`distinct_dates / span`) correlates with activity.

## Root cause
The incremental branch keyed its window (`current_partition`, `prev_balances`,
`calendar`) off a single **global** `max(date)`. The design assumes every entity emits
a row every day: an entity that skips a day falls off the global frontier, drops out of
the carry-forward, and the calendar never generates its missing dates — permanent gaps.

## Forbidden action
Never anchor carry-forward on a shared frontier date; never let a consuming mart read a
carry-forward model raw without its own spine (the UV3 branch builds its own daily
spine — the Balancer branch that didn't is how gaps reached charts).

## Detection
Per-entity density: `uniqExact(date) / dateDiff('day', min(date), max(date))` per
entity; investigate anything materially below 1.0 (see the sparse-series data-quality
test).

## Safe remediation
`dbt run --full-refresh -s <model>` (the non-incremental branch builds a per-entity
dense calendar), then rebuild direct downstream tables; views auto-reflect.

## Ground truth
The entity's own event dates: a reserve/balance row must exist for every calendar day
from the entity's first event, regardless of activity.

## Enforcement
Fixed in the model: the window anchors at the earliest per-(pool, token) frontier
(`min(max(date)) GROUP BY pool_address, token_address`), safe under insert_overwrite
because every touched partition is rebuilt with all pools present (commit 4cc8c608).
Sparse-series density test in tests/data_quality/.
