---
id: census-scalars-cross-job-scope
title: Reading census scalars across jobs admits tokens that have no holder census
status: remediated
scope: >-
  any dbt model reading rpc_state_indexer token_scalars (totalSupply and friends) without
  pinning job_name to the job whose token set it means to serve (canonical:
  int_rpc_state_indexer_token_supply_daily before 2026-09-16)
symptom: >-
  supply-only rows with holders NULL for tokens the consumers never served — here the 14
  aGno*/sp* wrapper tokens the old chain excluded via symbol_exclude — which would enter
  class overviews and double-count the underlying EURe/WxDAI
last_verified: 2026-09-16
evidence:
  - 'warehouse 2026-09-16 12:20 UTC — int_rpc_state_indexer_token_supply_daily held 15 September rows each for aGnoEURe, aGnoGNO, aGnoUSDC, aGnoUSDCe, aGnoWXDAI, aGnosDAI, spEURe, spGNO, spUSDC, spUSDC.e, spUSDT, spWETH, spWXDAI, spwstETH, all with supply_holders NULL (scalars from daily_token_supply / daily_atokens_full / daily_treasury)'
  - 'rpc-state-indexer config/gnosis/jobs.yaml — the daily_curated_balances selector omits every aGno*/sp* whitelist address; dbt_project.yml symbol_exclude lists the same 15 wrappers for the transfer chain'
  - 'losslessness of the fix — LEFT ANTI JOIN of curated publications to curated-job totalSupply scalars: 0 curated token-days without their own scalar, last 7 days and full history'
  - 'fix: `AND s.job_name = ''daily_curated_balances''` in the supply model''s scalars CTE (2026-09-16); models/rpc_state_indexer/AGENTS.md invariant 1'
---

## Symptom
The census supply model published supply for 50 tokens where the old chain served 36. The
extra 14 were wrapper tokens with a `totalSupply` scalar from another census job and no
holder census, so `supply_holders` and `holders` were NULL.

## Root cause
The scalars CTE deduplicated `totalSupply` across every job with `max()`, on the reasoning
that all jobs read the same anchor block. That is true, but the job also defines the token
set: `daily_token_supply` covers ~3,400 tokens and the aToken/treasury jobs cover wrappers,
while only `daily_curated_balances` covers the tokens dbt serves.

## Forbidden action
Never read `token_scalars` or `token_balances` from the staging views without pinning
`chain_id` and the `job_name` whose token set you mean to serve. `max()` across jobs is a
dedup guard, not a scope.

## Detection
`SELECT symbol, count() FROM <supply model> WHERE supply_holders IS NULL GROUP BY symbol`
must return no rows; the coverage test's density check runs per curated token.

## Remediated
Job filter in place; the model's `meta.agent` invariants state it. Not enforced by a gate.
