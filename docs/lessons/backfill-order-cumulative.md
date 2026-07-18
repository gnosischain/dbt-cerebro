---
id: backfill-order-cumulative
title: Backfill order matters — cumulative downstreams need history first
status: observed
scope: incremental models that read {{ this }} (self-referencing carry-forward) —
  ~34 models per grep, incl. the balances chains
symptom: a backfilled token/dimension gets permanently wrong values in cumulative
  downstreams that had already advanced past the backfilled window
last_verified: 2026-07-17
evidence:
  - grep -rl '{{ this }}' models/ — representative cumulative models: models/execution/tokens/intermediate/int_execution_tokens_balances_native_daily.sql, models/execution/Circles/intermediate/int_execution_circles_v2_balances_daily.sql, int_execution_circles_v1_balances_daily.sql, models/execution/lending/intermediate/int_execution_lending_aave_user_balances_daily.sql, models/consensus/intermediate/int_consensus_validators_income_daily.sql
  - note: the failure scenario itself is experiential (no repo incident doc) — hence status observed
---

## Symptom
After backfilling a new token/entity into an upstream, a cumulative downstream carries
values seeded from the wrong (empty) history — and forward-only runs never repair them.

## Root cause
A model reading `{{ this }}` seeds each day from its own prior day. If its watermark has
already advanced past the backfilled window, the new entity's history never enters the
carry-forward; the model integrates from a wrong initial condition forever.

## Forbidden action
Don't run a generic forward refresh over a mixed downstream set after a backfill
without classifying the models first.

## Detection / classification
Before any backfill lands: `grep -l '{{ this }}'` over the downstream closure.
- **Cumulative** (reads `{{ this }}`): backfill history FIRST, chronologically, before
  the model advances again.
- **Stateless incremental**: backfill after; only the backfilled window is incomplete
  until then.
- **table/view/latest**: self-heal on next build.

## Safe remediation
For an already-poisoned cumulative model: staged rebuild from the entity's true start
(see scripts/full_refresh/AGENTS.md); forward runs cannot fix it.

## Ground truth
On-chain state for the entity at a date inside the backfilled window.

## Enforcement
None automatic — classification step is part of the required workflow (AGENTS.md) and
the `context --task backfill` change packet.
