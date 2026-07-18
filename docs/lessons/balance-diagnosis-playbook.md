---
id: balance-diagnosis-playbook
title: Diagnosing balance/decode discrepancies — the traps that cost hours
status: remediated
scope: any investigation reconciling decoded/cumulative data against the chain
symptom: n/a (this is a playbook lesson — the traps below each produced wrong conclusions)
last_verified: 2026-07-17
evidence:
  - docs/data-quality-learnings-and-remediation.md (L5 + appendix, 2026-07 investigation)
---

## The traps

1. **Float vs Int256.** Summing ~1e20-wei values in `Float64` fabricated a fake
   "+0.64 ≈ balanced" for an address that was short a whole inflow. Reconcile in exact
   `Int256`: `reinterpretAsInt256(reverse(unhex(substring(data,1,64))))`.
2. **Bare hex.** `execution.logs` topics/addresses have no `0x` prefix; pad addresses
   to 32 bytes when matching topics.
3. **Block↔date is non-linear.** Estimating a date from a block delta sent one scope
   three years off. Always read `block_timestamp` / join `execution.blocks`.
4. **Verify against the chain, not the model.** On-chain `balanceOf` = 0 while the
   model said −54,875 is what proved a dropped inflow. The model can't be its own
   ground truth.
5. **Query-surface limits.** Correlated subqueries are rejected; `SYSTEM`/DDL blocked.
6. **Never a wide `delete+insert` during repair** — see wide-delete-insert-wipe.

## Queries that worked

- Exact per-address net from raw logs:
  `sumIf(reinterpretAsInt256(...), topic2 = <padded>) − sumIf(..., topic1 = <padded>)`
  filtered to the token contract, bounded by `block_number` for speed.
- Decode gap by month: `execution.logs` count vs decode-model count grouped by
  `toStartOfMonth(block_timestamp)`; nonzero deficit = dropped logs.

## Enforcement
Playbook only — encoded here and in models/execution/AGENTS.md.
