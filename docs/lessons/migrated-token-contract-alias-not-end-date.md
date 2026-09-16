---
id: migrated-token-contract-alias-not-end-date
title: A retired token contract that fronts the live ledger must be aliased in the census, not end-dated
status: remediated
scope: >-
  any token that migrated contracts and whose old contract still answers balanceOf and
  totalSupply from the new ledger (canonical: Monerium EURe and GBPe, both migrated
  2024-08-25; retired GBPe 0x5cb9073902f2035222b9749f8fb0c9bfe5527108)
symptom: >-
  the retired contract's daily census is PublicationBlocked with
  holder_sum_equals_total_supply (6-7 failed attempts a day) as soon as it enters the
  curated set, while the seed treats it as a normal windowed token
last_verified: 2026-09-16
evidence:
  - 'census_attempts 2026-09-14/15 — retired GBPe: 13 attempts, all `publication blocked: holder_sum_equals_total_supply`; its holder universe (own Transfer scan) missed holders that only ever transacted on the new contract'
  - 'on-chain 2026-09-16 (eth_call from the GKE daemon pod) — retired and live GBPe report identical totalSupply (362,375.12) and identical balanceOf for the top holder (85,520.00); same for EURe (18,711,320.63): both retired contracts are facades over the new ledger'
  - 'rpc-state-indexer config/gnosis/tokens.yaml — EURe v1 already carried universe_aliases → 0x420ca0…; the fix gave GBPe v1 universe_aliases → 0x8e34bfec… (commit ece72df); a two-day re-census then verified and published (universe ~2,693)'
  - 'a date_end was staged first and rejected: dbt only reads the retired contract through 2024-08-25 (seed date_end), but the census must keep reconciling or the daemon fails every day'
---

## Symptom
The retired GBPe contract was added to the curated set to recover 289 missing days. Its
history backfilled, then every daily census failed the supply-equality check while the
retired EURe contract, added the same way months earlier, kept verifying.

## Root cause
Both retired contracts still front the live ledger. The census discovers holders from the
contract's own Transfer events, so holders that only ever moved the new contract are
invisible to the old one; the holder sum falls short of `totalSupply` and the integrity
mode refuses to publish. EURe's entry already aliased the new contract's universe; GBPe's
did not.

## Forbidden action
Do not end-date a retired contract in the indexer to silence the failure, and do not compare
coverage per symbol: migrated symbols carry two addresses with contiguous seed windows
(`date_end` exclusive). Alias the retired contract's universe to the live one, exactly as
EURe v1 is configured, and verify the facade on chain first (identical `totalSupply` and a
matching `balanceOf` for a large holder).

## Detection
`census_attempts` with `error_message = 'publication blocked: holder_sum_equals_total_supply'`
concentrated on one target across consecutive days; and in dbt a per-address coverage
check against the seed windows.

## Remediated
GBPe v1 aliased (ece72df, pinned on both GKE stacks 2026-09-16). The deployment-block class
that hid the same contract's history is `indexer-deployment-block-truncates-history`.
