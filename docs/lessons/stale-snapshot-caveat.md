---
id: stale-snapshot-caveat
title: argMax "latest/quarter-end" marts silently serve the last ingested day when a source halts
status: observed
scope: >-
  snapshot-style marts using argMax(value, date) over sources that can silently
  halt (canonical: consensus.validators quarterly marts); any "latest" surface
symptom: a flat/plausible "current" value that is actually days or weeks old
last_verified: 2026-07-17
evidence:
  - docs/model_review/consensus.md:185,219-220,229 — consensus tables frozen at max_date 2026-06-07 (partial snapshot, 58,313 of ~558k rows); elementary freshness_anomalies are severity warn, so a multi-day silent outage does not fail the build
  - models/quarterly_data/gnosis_chain/marts/api_quarterly_data_validators_active.sql:36-37 and api_quarterly_data_staked_gno.sql:21-22 — argMax(value, date) labeled "quarter-end"
  - models/consensus/consensus_sources.yml:9-12 — source freshness warn 26h / error 48h exists, but the scheduled test path only warns
---

## Symptom
A "quarter-end" or "latest" metric looks fine but is frozen — the underlying source
stopped ingesting and `argMax(value, date)` keeps promoting the last observed day.
The last day can also be *partial*, making the value wrong as well as stale.

## Root cause
argMax-style snapshot marts have no freshness contract of their own; the scheduled
Elementary freshness tests are `severity: warn`, so a silent source halt doesn't fail
anything. (dbt-native `source freshness` would error after 48h — but only if that
surface is run and acted on.)

## Forbidden action
Never quote a snapshot/"latest"/"quarter-end" number without checking `max(date)` of
the underlying table against today — and whether the last day's row count is in line
with previous days (partial-day trap).

## Detection
`SELECT max(date), count() FROM <table> WHERE date = (SELECT max(date) FROM <table>)`
vs a typical day's count.

## Safe remediation
Escalate the ingestion halt (external system); until fixed, source true current values
from an alternative (the consensus incident used dora) and label the mart's staleness.

## Enforcement
None yet — freshness alerts remain warn-severity; the caveat is encoded here and in
AGENTS.md verification caveats.
