---
id: indexer-deployment-block-truncates-history
title: A token's configured deployment_block later than its real deployment silently truncates its census history
status: remediated
scope: >-
  any rpc-state-indexer token entry (config/<chain>/tokens.yaml) whose deployment_block
  was estimated rather than verified; surfaces in dbt as a census-derived model whose
  per-token history starts later than the tokens_whitelist date_start
symptom: >-
  a curated-balances backfill over a token's full window publishes nothing (or starts
  weeks/months late) with no error, while execution.logs shows the contract active
  from a much earlier block
last_verified: 2026-09-15
evidence:
  - rpc-state-indexer config/gnosis/tokens.yaml — retired GBPe 0x5cb9073902f2035222b9749f8fb0c9bfe5527108 carried deployment_block 31866249 (2024-01-09); execution.logs min(block_number) for that address is 25223106 (2022-11-30)
  - archive RPC eth_getCode 2026-09-15 via the GKE backfill pod — no code at 25223105, code at 25223106 (the same no-code/code boundary the GNO entry documents)
  - backfill Job rpc-state-indexer-backfill-20260915-164135 (2023-11-10..2024-08-25) — 0 GBPe publications while other targets published, because every anchor before block 31866249 predates the configured deployment
  - svZCHF 0x6165946250dd04740ab1409217e95a4f38374fe9 — configured 42168425, verified boundary 42067508/42067509 (2025-09-11); census history began 2025-09-17 instead of 2025-09-11
  - audit: values() of the curated selector joined to execution.logs min(block_number) per address, filtered configured > first_log — exactly these two of 66 curated tokens
  - fix: both deployment_block values corrected with archive-verified comments (indexer commit after 48cc0f2, 2026-09-15); the truncated windows re-backfilled
---

## Symptom
The retired GBPe contract was added to `daily_curated_balances` and a backfill over
2023-11-10..2024-08-25 ran to completion. It published other historical gaps it found on
the way and never mentioned GBPe. `execution.logs` shows the contract active from
2022-11-30. In dbt the census-derived balances model had the token's successor contract
only, so a per-symbol comparison looked fine while a per-address one showed 289 missing days.

## Root cause
The indexer treats `deployment_block` as the floor below which a token cannot have state:
no day whose anchor block precedes it is censused. The value for this token had been
entered as 31866249 (2024-01-09), fourteen months after the real deployment. Nothing
validates the field against the chain, and a late value produces no error — only absence.

## Forbidden action
Never enter or copy a `deployment_block` without the archive `eth_getCode` boundary check
(no code at N-1, code at N), and record the check in a comment next to the value as the GNO
entry does. A first-log block from `execution.logs` is an acceptable floor only when the
boundary check is impossible; it can still be later than the true deployment.

## Detection
For a chain's curated selector, join each configured `deployment_block` to
`min(block_number)` from `execution.logs` for the same address and list `configured >
first_log`. Any hit is a truncation. In dbt, compare per-token `min(date)` in the
census-derived model to the whitelist `date_start`, joining on the contract **address**
rather than the symbol, because migrated symbols (EURe, GBPe on 2024-08-25) carry two
addresses and a symbol-level comparison hides the retired one.

## Enforced / remediated
Remediated: both values corrected and the windows re-backfilled. Not enforced: the indexer
has no config-time check against the chain. A cheap gate is a startup or validate-config
step that reads `eth_getCode` at `deployment_block - 1` and refuses a non-empty result.
