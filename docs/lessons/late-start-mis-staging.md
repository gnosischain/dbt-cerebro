---
id: late-start-mis-staging
title: Stage start_date later than real first activity silently truncates history
status: remediated
scope: any token/entity whose meta.full_refresh stage start_date (or tokens_whitelist
  date_start) post-dates its real first on-chain activity
symptom: negative or short balances for holders whose inflows predate the configured start
last_verified: 2026-07-17
evidence:
  - models/execution/tokens/intermediate/schema.yml (int_execution_tokens_balances_native_daily staged start_dates; wstETH given its own 2022-06 stage)
  - docs/data-quality-learnings-and-remediation.md (L2 — wstETH staged 2025-01 but live since 2023-02; Balancer V2 Vault + Spark were the missing holders)
---

## Symptom
A token's balances go negative (or history looks short) even though the decode layer is
complete. Backfilling with the configured window makes it *worse* — more outflows become
visible while the older inflows stay missing.

## Root cause
The orchestrator only builds from the stage `start_date`. If that date post-dates the
token's real first activity, all earlier inflows are permanently absent from the staged
rebuild.

## Forbidden action
Don't assume a whitelist/stage start date is correct because it's written down — it
encodes someone's belief, not chain reality.

## Detection
Compare each whitelisted token's `min(date)` in the transfers model against
`tokens_whitelist.date_start`; flag when the model's first-seen is materially later
(the token never got its early history), and independently verify a token's true first
activity on-chain before setting any stage date.

## Safe remediation
Give the token its own stage with a verified `start_date`, then a staged rebuild of the
affected window (history first — the balances chain is cumulative).

## Ground truth
First `Transfer` log for the token contract on-chain (eth_getLogs from genesis /
deployment block), not any model.

## Enforcement
Data-quality test comparing whitelist start vs model first-seen (see tests/data_quality/).
