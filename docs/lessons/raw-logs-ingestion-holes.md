---
id: raw-logs-ingestion-holes
title: Raw execution.logs can have block holes — a layer below dbt
status: observed
scope: source execution.logs (external node crawler, declared in
  models/execution/execution_sources.yml); everything decoded from it
symptom: residual data gaps that survive every dbt-level re-decode
last_verified: 2026-07-17
evidence:
  - docs/incidents/logs_ingestion_gap_2026.md (confirmed 100-block hole, blocks 47,089,900-47,089,999 on 2026-07-08, zero logs for all contracts, dropped 48 WxDAI inflows)
  - docs/incidents/logs_ingestion_gap_CONFIRMED.csv
  - docs/data-quality-learnings-and-remediation.md (L3)
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

## Ground truth
On-chain `eth_getLogs` for the block range.

## Enforcement
Block-continuity data-quality test on the observability schedule; remediation remains
manual (external system).
