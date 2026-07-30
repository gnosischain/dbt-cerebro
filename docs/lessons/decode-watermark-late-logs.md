---
id: decode-watermark-late-logs
title: Append decode watermark drops late-arriving logs forever
status: remediated
scope: all append decode models under models/contracts (see manifest), plus the inline
  decode in int_execution_transfers_whitelisted_daily
symptom: negative balances for real holders (canary); silent undercount in every other
  decoded metric (mints, trust, swaps, trades, lending events)
last_verified: 2026-07-17
evidence:
  - macros/decoding/decode_logs.sql (watermark rendered as embedded literals, no lookback)
  - docs/data-quality-learnings-and-remediation.md (L1, 2026-07 investigation; WxDAI 461->0 negatives after recovery)
  - docs/incidents/logs_ingestion_gap_2026.md (related raw-layer instance)
  - scripts/refresh/gap_window_refresh.py (recovery lever)
---

## Symptom
A metric built on decoded events silently undercounts, or cumulative balances go
negative for real holders. Balances are the canary because a dropped *inflow* flips a
cumulative sum negative; counts and aggregates just drift low.

## Root cause
Decode models are `incremental_strategy='append'` wrappers over `decode_logs()`. At
render time the macro embeds literal `AND block_number > <max>` (+ timestamp bound)
from the target table. There is **no lookback**: a log backfilled into
`execution.logs` for an already-passed range sits below the watermark and is excluded
by every subsequent run, forever.

## Forbidden action
Do not add a daily lookback to the decode layer. On `append` models a lookback
duplicates rows until a background merge, and downstreams read without `FINAL` →
double-count. (A lookback is dup-safe only on `insert_overwrite` models.)

## Detection
Raw-vs-decoded parity per contract per month: count in `execution.logs` filtered to the
contract vs count in the decode model, grouped by `toStartOfMonth(block_timestamp)`;
nonzero deficit = dropped logs. Cheapest, earliest signal — fires before balances do.

## Safe remediation
`scripts/refresh/gap_window_refresh.py --months <gap months> --select <decode>+` —
drops the gap-month partition (lowering the watermark) and re-runs scoped. Never the
daily runner (only advances the watermark); a full-history rebuild is hours slower for
the same result.

## Ground truth
On-chain `eth_getLogs` over the affected range: if the chain has the logs and
`execution.logs` does too, this lesson applies; if `execution.logs` is missing them,
see raw-logs-ingestion-holes (layer below dbt).

## Enforcement
None automatic yet — parity/continuity checks exist as data-quality tests (see
tests/data_quality/) and run on the observability schedule; a weekly sweep +
gap-window recovery is the operating posture. Long-term option (not built): convert
decode models to month-partitioned insert_overwrite, making a lookback dup-safe.
