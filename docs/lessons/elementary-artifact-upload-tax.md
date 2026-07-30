---
id: elementary-artifact-upload-tax
title: Elementary re-uploaded the full artifact catalog on every dbt invocation
status: enforced
scope: Elementary OSS on-run-end hooks; any orchestration that shells out many dbt
  invocations per run (the microbatch runner does)
symptom: every dbt invocation pays a 35-68s flat tax; slice-heavy cron runs dominated
  by artifact uploads
last_verified: 2026-07-17
evidence:
  - 'dbt_project.yml:11-19 — disable_dbt_artifacts_autoupload: true with full rationale comment (measured 35-68s per catalog upload on ClickHouse, no temp-table support)'
  - scripts/run_dbt_observability.sh:364-385 — step "elementary-artifacts-refresh" re-enables the upload exactly once per run via --vars, with an exactly-once guard
  - 'commit 52cfd9d6 (2026-07-04) landed the flag (note: unrelated-sounding subject "updated: gnosis_ga_gt models and gnosis pay wallets")'
---

## Symptom
Cron/preview wall time dominated by "Uploading dbt artifacts" — a fixed cost repeated
per dbt invocation, brutal for microbatch runs that invoke dbt per slice.

## Root cause
Elementary's on-run-end hook uploads the full artifact catalog every invocation;
ClickHouse has no temp-table support so each upload is slow.

## Forbidden action
Don't re-enable per-invocation autoupload globally; don't remove the once-per-run
refresh step (run/test/freshness results still upload normally — only the artifact
catalog is deferred).

## Detection
Grep run logs for "Uploading dbt artifacts" occurrences vs dbt invocation count — it
should appear exactly once per orchestrated run.

## Enforcement
`disable_dbt_artifacts_autoupload: true` in dbt_project.yml + the exactly-once refresh
step (with guard) in run_dbt_observability.sh.
