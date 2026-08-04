---
id: circular-completeness-proof
title: A completeness proof anchored on the identifier the model already knows cannot detect a second one
status: observed
scope: entity-discovery models that define a population from one contract fingerprint
  (int_celo_gpay_safe_registry, int_celo_gpay_roles_modules); any "every X is covered"
  verification that derives X from the model's own anchor
symptom: a coverage check passes at 100% while an entire parallel cohort of the same
  entity is missing from the model; counts look internally consistent and are wrong
last_verified: 2026-08-04
evidence:
  - models/celo/gpay/intermediate/int_celo_gpay_roles_modules.sql (keys the whole Celo GP card universe on the single AggregateBridge 0xc07cd8c24fb384d5e2b60a3ef39751f5d4cb69e1)
  - "on-chain 2026-08-04: a second AggregateBridge 0xc4df5cac03f05603eb6c33cf3f68a5366e6e0a8d settled 1,725 transfers from 154 distinct card Safes; 235 cards provisioned on it 2026-03-31..2026-06-11; ZERO overlap with int_celo_gpay_safe_registry; both bridges settled within 4 seconds of each other that morning"
  - "the earlier verification that 'all 483 spenders are in the registry' passed because it defined the spender set as senders-to-0xc07cd8c2 — the second bridge was outside the question being asked"
---

## Symptom
A discovery model claims a population ("GP card Safes on Celo") and a verification
confirms it is complete. Both are true and both are useless: a second cohort of the
same entity, of comparable size, is absent. Downstream counts are self-consistent, so
nothing looks broken — the growth curve is simply the wrong shape.

## Root cause
The model identifies entities by a fingerprint anchored on ONE address (here: a Safe
is a GP card iff its Roles module wired in AggregateBridge `0xc07cd8c2…`). The
completeness check then derived its own universe from that same address — "list
everyone who sent to `0xc07cd8c2…`, confirm they are all in the registry." That is
circular. It can only ever return 100%, because the population and the model share an
anchor. A second settlement contract is not a row that fails the check; it is outside
the set the check enumerates.

## Forbidden action
Never verify the completeness of an anchored population using the anchor. Any proof
of the form "everyone who interacts with X is in the model" is vacuous when the model
is defined as "everyone who interacts with X".

## Detection
Start one level up, from the *class* of anchor rather than an instance:

```sql
-- Which contracts receive settlement transfers from ANY known card Safe?
-- Any receiver here that is not a known bridge is a new settlement generation.
SELECT concat('0x', substring(replaceAll(topic2, '0x', ''), 25, 40)) AS receiver, count()
FROM celo_execution.logs
WHERE replaceAll(topic0, '0x', '') = 'ddf252ad...b3ef'  -- Transfer
  AND concat('0x', substring(replaceAll(topic1, '0x', ''), 25, 40))
      IN (SELECT lower(address) FROM int_celo_gpay_safe_registry)
GROUP BY receiver ORDER BY count() DESC
```

The cheap proxy signal is mastercopy drift: a new card generation has now twice
coincided with a new mastercopy, so `assert_celo_gpay_roles_mastercopy_known` fires
first and costs 0.2s. It is a leading indicator, NOT a substitute — both known Celo
generations used allowlisted mastercopies, so that test would not have caught this
instance on its own.

## Safe remediation
Do NOT simply widen the bridge filter to `IN (bridge1, bridge2)`. The two Celo
generations straddle the June 2026 post-exploit module rebuild and behave differently
(one is a closed, flat cohort; the other is the growing live program). A blind union
silently blends them and makes every cohort/retention/growth figure a mixture. Add a
generation/bridge dimension and carry it through the marts so a consumer can ask for
one, the other, or both deliberately.

## Ground truth
The chain, enumerated from the entity side: for every Safe already known to be a card,
where does its money actually settle? Plus the operator's own list of settlement
addresses — ask, do not infer, and treat the answer as the seed rather than a
confirmation of what you already modelled.

## Enforcement
None yet. `assert_celo_gpay_roles_mastercopy_known` is the partial leading indicator
described above. `models/celo/AGENTS.md` carries the rule that any Celo completeness
check must start from the set of settlement contracts, never a single address.

## OPEN DECISION (as of 2026-08-04)
`int_celo_gpay_safe_registry` is knowingly incomplete and every Celo GP figure is
new-generation-only. Quantified impact: ~14% of cards and ~35% of settlement transfers
missing overall, but the gap is front-loaded — in the week of 2026-06-08 the unmodelled
bridge was 79% of all settlement transfers, by the week of 2026-07-27 it was 12.5%. So
the distortion is not a flat undercount, it makes the June launch look like a ramp from
near-zero when a steady ~30-active-cards/week cohort already existed. June/July
month-over-month growth, cohort and churn numbers are materially wrong.

Blocked on Gnosis Pay (asked 2026-08-04, awaiting reply): whether `0xc4df5cac…` is
theirs and still in service, whether the migration they mentioned on 2026-07-01 is
still planned, whether the two cohorts should be reported as one program or separately,
whether `0xd11e35ca1594651f172748428ffc4b6c63c3cca3` (deployed, never used) is a
planned successor, and the complete list of settlement addresses past and present.

Resume here when they answer: their answer to the reporting question determines whether
this becomes a generation dimension across the marts or a separate legacy series. Until
then, label every Celo GP dashboard figure as covering the current generation only.
