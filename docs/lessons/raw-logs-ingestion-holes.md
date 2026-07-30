---
id: raw-logs-ingestion-holes
title: Raw execution.logs can have block holes — a layer below dbt
status: observed
scope: source execution.logs (external node crawler, declared in
  models/execution/execution_sources.yml); everything decoded from it
symptom: residual data gaps that survive every dbt-level re-decode
last_verified: 2026-07-18
evidence:
  - 'docs/incidents/logs_ingestion_gap_2026.md — instances 1+2 (2026-05-30 blocks 46434334-46434399, 2026-06-14 blocks 46689500-46689599; 165 blocks, 11,550 logs): RESOLVED end-to-end, warehouse-verified 2026-07-18 (raw counts match chain, decodes reprocessed, the surfaced Trust tx present in int_execution_circles_v2_trust_updates)'
  - 'instance 3 (2026-07-08 blocks 47,089,900-47,089,999, zero logs for all contracts, dropped 48 WxDAI inflows; docs/data-quality-learnings-and-remediation.md L3): RESOLVED 2026-07-18 — raw backfill (16,895 logs/100 blocks) had landed below the append watermarks (15 decode families at 0 decoded); recovered with gap_window_refresh.py --months 2026-07-01 over 96 models, 0 failures, ~23 min. Verified: 16/16 families decoded==raw exactly, 0 duplicate sort-key groups in touched July partitions, smooth 07-06..07-10 day series, 9/9 cumulative pool/token deltas match decoded full-day nets'
  - docs/incidents/logs_ingestion_gap_CONFIRMED.csv
  - 'detection automated: tests/data_quality/dq_daily_raw_logs_block_continuity.sql'
  - 'recovery recipe proven twice: enumerate affected families from the WINDOW RAW ADDRESSES (21 families for the 8-minute July hole, not the 1-2 a symptom report names), exclude *_live rolling variants, skip chains already reprocessed for the month, then gap_window_refresh.py. Verify with decoded==raw EQUALITY per family (catches missing AND doubled) plus a sort-key dup scan'
---

## Symptom
After a full decode-layer recovery, some rows are still missing. No dbt lever fixes it.

## Root cause
`execution.logs` is produced by an external node crawler outside this repo. An ingestion
skip leaves a run of consecutive blocks with zero logs across **all** contracts. The
logs were never in the source, so no re-decode can recover them.

## Forbidden action
Don't keep re-running dbt refresh levers against a raw hole — if the raw source lacks
the rows, every downstream tool will faithfully reproduce the gap.

## Detection
Block-continuity scan: a >5-block span with zero logs between two present blocks on a
live chain is not "quiet blocks". (See the data-quality test in tests/data_quality/.)

## Safe remediation
Confirm the flagged range on-chain (`eth_getLogs` — chain has logs, `execution.logs`
doesn't), then request a raw re-index of the block range from the ingestion side. After
the raw backfill lands, recover the decode layer with `gap_window_refresh.py` (see
decode-watermark-late-logs — the backfilled rows are below the watermark).
CUMULATIVE downstreams of the affected decodes are wrong from the gap DAY forward,
so their rebuild window is gap month through CURRENT month, chronologically — never
just the gap month (see backfill-order-cumulative, gap-recovery corollary).
Classify first: grep -rl '{{ this }}' models/ ∩ dbt ls -s <decode>+.

## Ground truth
On-chain `eth_getLogs` for the block range.

## Enforcement
Block-continuity data-quality test on the observability schedule; remediation remains
manual (external system).
