---
id: event-struct-array-decode-unreliable
title: A decoded event struct-array (tuple[]) is unreliable — derive positional token maps from an independent source
status: observed
scope: models that read a Solidity struct-array (`tuple[]`) event param and use its
  positional index to attribute per-token amounts — Balancer V3 PoolRegistered.tokenConfig
  in int_execution_pools_balancer_v3_daily; any decode chain over ABI tuple[] params
symptom: per-token reserves/balances misattributed across a pool's own tokens (one leg
  negative, another inflated) even though pool-total and swap-side numbers look fine
last_verified: 2026-07-24
evidence:
  - models/execution/pools/intermediate/int_execution_pools_balancer_v3_daily.sql (pool_tokens CTE; tokenconfig_raw parses decoded_params['tokenConfig'] tuple[])
  - "on-chain Vault 0xba1333333333a1ba1108e8412f11850a5c319ba9 getPoolTokenCountAndIndexOfToken: pool 0x5a15b1e1 -> wstETH=0, 0x7ef5=1, WBTC=2, GNO=3, sDAI=4 (address-sorted); matched arraySort(swap tokens), NOT the tokenConfig decode"
  - "7 disagreement (pool,index) cases: mapped_addr (swap-sorted) sat at the disputed index on-chain in all 7; reliable_addr (tokenConfig) either reverted (0x09f9611f/0x89c80a45 not registered) or was misplaced (wstETH decoded at idx4, actually idx1)"
  - "census: Balancer V3 pool-token pairs ever negative 142 -> 0 after the swap-sorted rebuild; combined labeled pools negatives ~163 -> 1 (a -$0.05 BalV2 dust, 2026-05-17)"
---

## Symptom
A pool's per-token reserves are misattributed **across its own tokens**: one leg goes
negative while another is inflated by the missing amount, even though the pool-total
reserve and the swap-side (token-address-keyed) numbers look right. On Balancer V3 the
sDAI leg of pool `0x5a15b1e1` read −540 while an unlabeled leg read +5,292; the true
split was sDAI +37.75 and that leg +30.67.

## Root cause
Liquidity events (`LiquidityAdded`/`LiquidityRemoved`) carry only a **positional
`token_index`** into the Vault's address-sorted token order; they do not carry the token
address. The model resolved that index → address from the **decoded `PoolRegistered.tokenConfig`
struct-array** (`tuple[]`). The log decoder does not decode nested `tuple[]` params
faithfully: inner tokens decode to `0x0`, some entries are **bogus addresses that were
never registered** (verified: `getPoolTokenCountAndIndexOfToken` reverts for them), and
**real tokens are placed at the wrong index** (wstETH decoded at index 4 but is actually
index 1). So `amountsAddedRaw[i]` was attributed to the wrong token; the LP-add of one
token landed on another leg, driving the true leg negative.

The swap side was always correct because Swap events carry the token **address** directly,
so only the index-keyed liquidity deltas were mis-mapped.

## Forbidden action
Do not trust a decoded `tuple[]`/struct-array param positionally. Do not use its element
addresses, its element order, or infer "sentinels" from it, when an independent
authoritative ordering exists.

## Detection
`tests/data_quality/dq_daily_pools_reserve_physical_balance.sql` (severity=warn) — a
physical reserve must be `>= 0` and equal `token_amount`; a negative leg is the signature.
Cross-check offline: for each pool where `length(arraySort(swap tokens)) == tokenConfig
length`, assert the swap-sorted address at each reliably-decoded (non-sentinel) index
equals the tokenConfig address — disagreements are decode failures, not fix regressions.

## Safe remediation
Derive the `token_index → address` map from an **independent authoritative source**. For
Balancer V3 the Vault registers tokens **address-sorted**, and that order indexes the
positional `amountsAddedRaw`/`amountsRemovedRaw` arrays — so `arraySort(groupUniqArray(Swap
token_address))` reproduces it exactly (confirmed on-chain via
`getPoolTokenCountAndIndexOfToken`). Use the swap-sorted list whenever it fully covers the
pool's registered token count (`n_swap == token_cnt`); keep the old tokenConfig path only
as a fallback for pools whose tokens are not all traded. Then **full-refresh** the model
(cumulative — the non-incremental branch rebuilds the whole per-pool calendar with no
`{{ this }}` seed) and rebuild the downstream balances/TVL closure.

## Ground truth
Balancer V3 Vault (core, not the delegated extension) `getPoolTokenCountAndIndexOfToken(pool,
token)` returns `(count, index)` and reverts (`0xddef98d7`) for an unregistered token — the
authoritative index map, one RPC call per (pool, token).

## Enforcement
Detection is the reserve physical-balance DQ test above. NOTE: the code fix (the
swap-sorted `pool_tokens` mapping) is in the working tree only; production runs the merged
image, so the nightly incremental re-arms the mis-mapping for **new** days until merged —
the warehouse history is corrected, the code is not yet deployed.
