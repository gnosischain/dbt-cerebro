---
id: frozen-upstream-behind-fresh-ingest
title: A full-reload source froze upstream while every load stayed green; load-time freshness cannot see it
status: observed
scope: >-
  sources reloaded in full from a vendor file (crawlers_data.ember_*, any click-runner
  csv-mode table) and every model gated on their latest period (the ESG chain)
symptom: >-
  downstream marts stop at a fixed date months ago while their other inputs are
  current; the ingest job is green and the freshness alert on the raw table is quiet
last_verified: 2026-11-10
evidence:
  - 'Ember long-format file (crawlers_data.ember_electricity_data) froze at data to 2026-06-01: the 2026-10-21 load reinserted 364,334 rows and added no key (378,066 rows FINAL before and after). Every api_esg_* mart stopped at 2026-06-30 while the node models were current to 2026-11-08 (morning check 2026-11-09)'
  - 'nothing fired for 4+ months: the dbt source had freshness: null (models/probelab/probelab_sources.yml), and the infra rule crawlers-data-stale measured max(version), the load time, which advanced on every load (alerting stack alerts/click-runner.yaml, 480 h)'
  - 'it surfaced only because Ember retired the URL on 2026-11-02 14:10 UTC (it now serves a two-column notice CSV) and the 2026-11-08 load failed with Code 47 on Area'
  - 'fix in tree, pending deploy: source crawlers_data.ember_generation_monthly gets freshness on the DATA date of the World series (loaded_at_field toDateTime(Date), filter Area = World, warn 120 d / error 150 d); a data-date row for the infra alert is proposed alongside the click-runner change'
---

## Symptom
A family of marts ends on the same date weeks or months ago, while the other inputs of
the same models are current. The ingest job is green, and the freshness alert on the
raw table is quiet.

## Root cause
A full-reload ingestor re-inserts the vendor's whole file on every run. Its load
timestamp (`version`, `ingested_at`) therefore advances on every run, even when the
file's content has stopped changing. Freshness measured on the load time proves the
job ran, not that new data arrived. With `freshness: null` on the dbt source, nothing
looked at the data date at all.

The ESG chain made it worse: it emits a month only when Ember's intensity for it
exists, so a frozen file silently caps every ESG mart instead of failing anything.

## Forbidden action
Don't measure the freshness of a full-reload source on its load time alone, and don't
set `freshness: null` on a source whose latest period gates a mart. Don't read a green
ingest job as fresh data.

## Detection
`max(<data date>)` of the series that gates the marts, against the vendor's normal
publication lag. For Ember: `max(Date)` where `Area = 'World'` and
`Electricity source = 'Total generation'`; healthy is 65-110 days old (World is
published about two months after month end, and we load on the 8th and 21st; a late release can add ~10 days).

## Safe remediation
Point the dbt source freshness at the data date, filtered to the series that gates
the marts, with thresholds just above the vendor's normal lag. If the vendor moved or
froze the file, switch the ingestor to the live file and let the full-rebuild ESG
models pick up every published month on the next run; no backfill is needed.

## Ground truth
The vendor's own file: for Ember, the newest `Date` of the World row in
`release_generation_monthly_global.csv`, against Ember's release notes.

## Enforcement
Pending deploy: dbt source freshness on the data date of the World series
(`ember_generation_monthly`, warn 120 d / error 150 d), and a data-date row for the
infra `crawlers-data-stale` rule (2880 h). Neither has fired in production yet.
