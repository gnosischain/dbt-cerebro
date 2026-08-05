---
id: circular-completeness-proof
title: A completeness proof anchored on the identifier the model already knows cannot detect a second one
status: resolved
scope: entity-discovery models that define a population from one contract fingerprint
  (int_celo_gpay_safe_registry, int_celo_gpay_roles_modules); any "every X is covered"
  verification that derives X from the model's own anchor
symptom: a coverage check passes at 100% while an entire parallel cohort of the same
  entity is missing from the model; counts look internally consistent and are wrong
last_verified: 2026-08-05
evidence:
  - seeds/celo_gpay_settlement_contracts.csv (the fix — the anchor set is now seeded, and consumed by int_celo_gpay_roles_modules, int_celo_gpay_safe_registry and int_celo_gpay_activity)
  - "on-chain 2026-08-04: a second AggregateBridge 0xc4df5cac03f05603eb6c33cf3f68a5366e6e0a8d settled 1,743 transfers from 155 distinct card Safes; 235 cards provisioned on it 2026-03-31..2026-06-11; ZERO overlap with the then-registry; both bridges settled within 4 seconds of each other that morning"
  - "the earlier verification that 'all 483 spenders are in the registry' passed because it defined the spender set as senders-to-0xc07cd8c2 — the second bridge was outside the question being asked"
  - "Gnosis Pay confirmed 2026-08-05 that both bridges are theirs and that the legacy one will be migrated onto the current one, which is why the bridge is modelled per-transfer and not as a per-card generation"
  - "the two bridges share ZERO event signatures (5 vs 7, no overlap), so they are different contracts rather than two versions of one — do not label anchors v1/v2 on assumption"
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
coincided with a new mastercopy, so the fingerprint ⊆ mastercopy probe in
`models/celo/AGENTS.md` is a useful first look. It is a leading indicator, NOT a
substitute — both known Celo generations used allowlisted mastercopies, so that probe
would not have caught this instance on its own. The settlement-receiver query above is
the only detection that actually works.

## Safe remediation
Replace the anchor with a **seeded set**, and put the anchor identity on the *event*,
not on the entity. Three parts, and they are not separable:

1. **Seed the set** (`celo_gpay_settlement_contracts`), with a `status` so a candidate
   contract can sit inert until confirmed. The point is that the next discovery costs
   one seed row instead of an edit to every model that hardcoded the address.
2. **Widen discovery and classification in the same run.** Discovery decides which
   entities exist; classification decides what their events mean. Widening only the
   first admits entities whose events then fall through to a catch-all branch — on Celo
   that would have booked 1,743 payments as *withdrawals*, which is worse than omitting
   the cards.
3. **Attach the anchor to the transfer, never to the entity.** GP is migrating the
   legacy cards onto the current contract, so a card's bridge changes over its life.
   A per-card "generation" column is
   correct only until the first migration, then silently wrong; a per-transfer
   `settlement_address` stays correct and makes migration progress measurable.

An earlier revision of this file said "do NOT simply widen the filter — the two
generations behave differently, blending them corrupts cohort figures." That was
half-right and is superseded. The caution about blending was sound, but the operator
confirmed both bridges are one program, so the union is the *correct* population and
excluding a cohort was the larger error. Keep the dimension, drop the exclusion.

## Ground truth
The chain, enumerated from the entity side: for every Safe already known to be a card,
where does its money actually settle? Plus the operator's own list of settlement
addresses — ask, do not infer, and treat the answer as the seed rather than a
confirmation of what you already modelled.

## Enforcement
None, by decision. There is no automated gate for this class on Celo: a new settlement
contract or mastercopy is legitimate operator activity, not a data defect, so it does
not belong in a build-breaking test — and a dbt test cannot see the thing that matters
here anyway (a bridge the model has never heard of). A predecessor test asserting the
inverted direction was removed for exactly this reason.

Enforcement is therefore documentary and must stay that way to be useful:
`models/celo/AGENTS.md` carries the rule that any Celo completeness check starts from
the set of settlement contracts, never a single address, plus the manual mastercopy
drift probe. This hazard is registered on `int_celo_gpay_safe_registry`, so it surfaces
via `get_dbt_change_context` to anyone editing the card universe. Re-run the detection
query above by hand before trusting any Celo GP growth, cohort, or churn figure.

## RESOLVED 2026-08-05
Gnosis Pay confirmed `0xc4df5cac…` is theirs, still in service, and scheduled to
migrate onto `0xc07cd8c2…` at an unannounced date — one program, two settlement
endpoints. The registry now unions both from the seed, and
`int_celo_gpay_activity.settlement_address` carries the bridge per transfer.

**What the omission actually cost, measured on 2026-08-05.** 235 cards (13%), 1,743
settlement transfers (34%), and ~$75.4k of payment volume (57,698 USDT + 17,710 USDC).
But the level understates the damage; the *shape* was wrong. Monthly payments, modelled
vs. real:

| Month | modelled | actual | missing |
|---|---|---|---|
| 2026-03 | 0 | 4 | 100% |
| 2026-04 | 0 | 225 | 100% |
| 2026-05 | 4 | 472 | 99% |
| 2026-06 | 266 | 759 | 65% |
| 2026-07 | 2,533 | 3,008 | 16% |

March through May were essentially absent, so the tree told a zero-to-one story
starting in June when the program had been running steadily since March and the legacy
cohort was flat at ~470–490 payments/month while the current one grew. Every pre-2026-08-05 Celo GP
growth, cohort and churn figure is both understated and reshaped — restate, do not
splice.

**Post-fix state.** 1,815 cards from 2026-03-31; completeness re-proven from the seed
rather than an anchor (507 current + 155 legacy spenders, zero missing); zero bridges
enrolled as cards; and the mastercopy cross-check gap closed to zero, which
independently confirms the legacy cohort and the `roles_pilot` mastercopy are the same
population.

**Do not name anchors on assumption.** The first version of this fix labelled the two
contracts `v1`/`v2`, which invented a lineage nobody had evidenced. They in fact share
no event signatures at all, so they are separate contracts that both happen to settle
cards — the labels are now `settlement_legacy` / `settlement_current`, ours and
descriptive. This is not cosmetic: it means decoding settlement events requires two
distinct ABIs, which a v1/v2 reading would have obscured.

**Still watch.** `0xd11e35ca…` is deployed, unused, and unconfirmed — it sits in the
seed as `status='planned'`, excluded from every filter, so activating it is a one-word
edit. No Roles module wires both bridges yet, so migration has not begun; when it does,
`int_celo_gpay_roles_modules.wired_settlements` will show two-element arrays and no
model change should be needed. Re-run the detection query above before trusting any new
Celo GP growth figure — the seed is only as complete as the last conversation with the
operator.
