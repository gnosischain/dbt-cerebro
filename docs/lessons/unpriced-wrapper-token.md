---
id: unpriced-wrapper-token
title: Every wrapper/vault token needs a price path or it reads $0 everywhere
status: remediated
scope: tokens_whitelist additions that are wrappers/vault shares (aTokens, ERC-4626
  vaults, RWA wrappers); int_execution_token_prices_daily
symptom: real supply/balances but $0 USD on every USD-valued surface
last_verified: 2026-07-17
evidence:
  - models/execution/prices (int_execution_token_prices_daily ocsdai_price CTE; commit bfa0baad "fix: oc-sdai price")
  - docs/data-quality-learnings-and-remediation.md (OC-1 — OC-sDAI 265,446 shares rendered $0; fixed to ~$1.244/share via share_price x native sDAI price)
---

## Symptom
A token shows correct native supply/balances but `$0` USD everywhere — supply plots,
portfolio values, revenue look-throughs.

## Root cause
`int_execution_token_prices_daily` derives wrapper prices only for the branches it
knows (Aave/Spark aTokens via `lending_market_mapping`, RWA via `backedfi`, ERC-4626
via share-price CTEs). A new wrapper class gets no price row, and downstream computes
`supply_usd = supply * coalesce(price, 0)`.

## Forbidden action
Don't whitelist a wrapper/vault token without also wiring its price derivation — the
`$0` is silent (no error, no null).

## Detection
Unpriced-token guard: any whitelisted token with nonzero native supply but
absent/zero USD price for a recent date (see tests/data_quality/).

## Safe remediation
Add a derived-price branch mirroring the existing ones (share price × underlying
native price, correct priority), then rebuild the price model and USD downstreams for
the affected window. Check the token's whole downstream chain also has history
(never-seeded-incremental is the sibling failure).

## Ground truth
The vault's `convertToAssets`/share-price on-chain × the underlying's market price.

## Enforcement
Unpriced-token data-quality test in tests/data_quality/.
