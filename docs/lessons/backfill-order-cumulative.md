---
id: backfill-order-cumulative
title: Backfill order matters — cumulative downstreams need history first
status: observed
scope: incremental models that read {{ this }} (self-referencing carry-forward) —
  ~34 models per grep, incl. the balances chains
symptom: a backfilled token/dimension gets permanently wrong values in cumulative
  downstreams that had already advanced past the backfilled window
last_verified: 2026-07-18
evidence:
  - grep -rl '{{ this }}' models/ — representative cumulative models: models/execution/tokens/intermediate/int_execution_tokens_balances_native_daily.sql, models/execution/Circles/intermediate/int_execution_circles_v2_balances_daily.sql, int_execution_circles_v1_balances_daily.sql, models/execution/lending/intermediate/int_execution_lending_aave_user_balances_daily.sql, models/consensus/intermediate/int_consensus_validators_income_daily.sql
  - note: the failure scenario itself is experiential (no repo incident doc) — hence status observed
  - '2026-07-18 gap-recovery corollary quantified: the two decode families hit by the 2026-07-08 raw-logs hole have 662 transitive downstream models incl. 18 cumulative (Circles v2 balances/cohorts, all four pools dailies, Aave user balances, revenue fees weekly per user) — see docs/incidents/logs_ingestion_gap_2026.md addendum'
  - '2026-07-18 verification technique that WORKS for cumulative correctness: pick entities whose day has only additive events (e.g. Balancer V2 pools with Swap-only Vault events), then assert model day-over-day delta == decoded full-day net EXACTLY in raw units. Applied to all three gap days: 7/7 pairs (May-30), top pool (Jun-14), 9/9 pairs (Jul-08) — all match, proving carry-forward was rebuilt. An on-chain balanceOf spot-check is unnecessary when this identity holds'
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

## Gap-recovery corollary (past-window backfills)
When recovering a PAST gap (e.g. a raw-logs hole backfilled weeks later), a cumulative
downstream is wrong from the gap DAY forward — every frozen day after it integrated
the wrong prior state (compose with frontier-day-incomplete-inputs: the daily branch
never revisits built days). Therefore the cumulative rebuild window is
**gap month through CURRENT month, chronologically** — never just the gap month.
"Gap month only" is sufficient only while the gap month IS the current month; each
month boundary crossed since the gap adds a month to the window. Stateless
downstreams still need only the gap window. `gap_window_refresh.py --months` must
list the full span for the cumulative subset.

## Ground truth
On-chain state for the entity at a date inside the backfilled window.

## Enforcement
None automatic — classification step is part of the required workflow (AGENTS.md) and
the `context --task backfill` change packet.
