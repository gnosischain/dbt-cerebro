---
id: old-token-burned-supply-overstatement
title: >-
  A raw totalSupply() read overstates effective supply for pre-2018-style tokens —
  transfers to 0x0 accumulate at the zero address without decrementing totalSupply.
status: observed
scope: >-
  Any model deriving a token supply from a contract totalSupply()/scalar read, or
  summing holder balances without excluding 0x0/0xdEaD. Bit WL-036 (GNO supply splice);
  fix authored + validated in the working tree, pending merge.
symptom: >-
  A supply series reconstructed from totalSupply() disagrees with a transfer-delta
  series (e.g. Dune's) by a large constant that appears after historical burn events;
  for mainnet GNO the gap is ~3,147,806 GNO (~31% of the 10M totalSupply).
last_verified: 2026-08-25
evidence:
  - "eth_call 2026-08-24: GNO 0x6810e776… balanceOf(0x0) = 3,147,806.3457465111 while totalSupply() = 10,000,000 constant"
  - "models/crawlers_data/staging/stg_rpc_state_indexer__gno_supply_daily.sql (burned term; EthCirc = minted - burned - vesting - bridge)"
  - "Full-overlap reconciliation vs Dune 2026-08-24: Non-Circ 2,120/2,120 days exact; EthCirc 2,118/2,120 exact (2 one-day anchor-vs-midnight artifacts, self-correcting)"
  - "Dune parent query 4615226 (captured in session): supply = cumulative transfer deltas with from/to 0x0 as mint/burn"
  - "work-log handover WL-036 (2026-08-24) recorded the wrong formula: 'ETH GNO totalSupply is a CONSTANT 10,000,000' without the burned term"
---

## Symptom

An "effective/circulating supply" metric built from a contract `totalSupply()` read (or
an indexer scalar of it) overstates supply, diverging from transfer-delta sources by a
constant that jumps at burn-event dates. WL-036: the reconstructed
`Ethereum Circ. Supply` read 4,709,740 vs Dune's 1,561,933.74 — a 3,147,806.3458 gap.

## Root cause

Pre-2018-style ERC20s (mainnet GNO, 2017) accept plain `transfer(0x0, x)` and never
decrement the supply variable: burned tokens accumulate as `balanceOf(0x0)` (sometimes
`0xdEaD`). Modern OZ-style tokens revert transfers to 0x0 and decrement `totalSupply()`
in `_burn`, so the two conventions disagree exactly by the burned balance. Transfer-delta
derivations (Dune's `tokens_*.transfers` with from/to 0x0 as mint/burn) net burns out
implicitly, so they measure effective supply; `totalSupply()` measures minted supply.

## Forbidden action

Quoting `totalSupply()` (or a scalar snapshot of it) as circulating/effective supply for
a token without first checking `balanceOf(0x0)` and `balanceOf(0xdEaD)`. Also: summing
holder balances INCLUDING the zero/dead addresses while simultaneously subtracting
nothing — that double-counts burned tokens as held.

## Detection

- One-off: `eth_call balanceOf(0x0…0)` and `balanceOf(0x…dEaD)` on the token; nonzero on
  either means raw `totalSupply()` is not effective supply.
- Cross-check any supply series against an independent transfer-delta reconstruction;
  a large flat offset appearing at discrete dates is the signature.
- Chain-100 GNO needs no adjustment — proven: its bridged-token mint/burn moves
  `totalSupply()`, and the scalar matched Dune's transfer-delta series on all 1,613
  published days.

## Safe remediation

effective_supply = totalSupply() − balanceOf(0x0) − balanceOf(0xdEaD) — harmlessly
zero-subtracting for well-behaved tokens. In the WL-036 design the zero address is
tracked as a census holder (rpc-state-indexer `gno_supply_wallets` universe) so the
burned term is a first-class daily balance, exact at every historical anchor.

## Ground truth

On-chain state: `balanceOf` at the canonical day-anchor block. For validation, the Dune
transfer-delta series (frozen `crawlers_data.dune_gno_supply` history). Note the two
conventions legitimately differ intraday: state-at-anchor vs end-of-day transfer sum can
disagree for one day when a tracked balance moves after the anchor; it self-corrects the
next day.
