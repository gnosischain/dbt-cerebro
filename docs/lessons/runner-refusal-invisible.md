---
id: runner-refusal-invisible
title: >-
  The microbatch runner refuses a stage whose gap exceeds the slice cap with a
  stderr line and exit 0, so a model that is not being advanced looks green everywhere
status: observed
scope: >-
  every microbatch model run by scripts/refresh/dbt_incremental_runner.py; bites
  sparse decode models (contracts_*) whose last event is older than
  --max-slices-per-stage days, and any model whose meta date_column cannot be read
symptom: >-
  a production model has not been executed by the cron for weeks while Elementary
  shows its last run as success and dbt_model_status reports it pending or success;
  the only trace is a "[error] ... exceeds --max-slices-per-stage" line in the pod's
  stderr for that day
last_verified: 2026-09-07
evidence:
  - 'scripts/refresh/dbt_incremental_runner.py plan_for_model (pre-fix): on a refusal it printed to stderr, appended (stage, []) and continued; the refusal was not in `failures`, not stashed, not in the exit code, not in Elementary, not in the Prometheus status'
  - '2026-09-07 sweep of 113 microbatch models: 39 had a data watermark more than 30 slices old; Elementary last execution for them: contracts_AgentResultMapping_calls 2026-05-12, contracts_CowProtocol_GPv2AllowListAuthentication_events 2026-07-15, contracts_circles_v2_StandardTreasury_events 2026-08-24, contracts_circles_v2_CMGroupDeployer_events 2026-08-26 (0 runs in the last 7 days for ~20 of them)'
  - 'spot check that this was an observability gap rather than a data gap: execution.logs has 0 rows for the StandardTreasury contract 0x08f90ab73a515308f03a718257ff9887ed330c6e since 2026-07-01, so nothing was missed there — but a dormant contract that wakes up is never decoded until someone runs it by hand'
  - 'same day, the sibling silent path: int_celo_gpay_funder_wallet_transfers / _monthly_presence declared date_column block_timestamp which their tables do not have; get_max_date failed (rc=1) and the runner bootstrapped 7 slices every day with only a [warn] line (fixed in models/celo/gpay/intermediate/schema.yml)'
  - 'fix in tree, pending deploy: record_runner_event() in the runner writes target/failed_batches/runner-refused-*.json and runner-watermark-fallback-*.json; emit_model_status_metrics.py reports status="refused", dbt_runner_refused_gap_days and dbt_runner_watermark_fallback; --fail-on-refusal exit 3; tests in tests/test_dbt_incremental_runner.py and tests/test_emit_model_status_metrics.py'
---

## Symptom
A production model quietly stops being executed. Elementary's last row for it is a
success from weeks ago, `dbt_model_status` shows `pending` (or `success` if another
stage ran), and nothing alerts. Only the cron pod's stderr for that day carries
`[error] <model> stage=<s>: gap is N day(s) ... exceeds --max-slices-per-stage=30`.

## Root cause
`plan_for_model` treats a refusal as "nothing to do": it prints, appends an empty slice
list and returns; the runner's exit code, failure stash, Elementary upload and the
Prometheus emitter all key off dbt invocations, and a refused stage never produces one.
Sparse decode models make this chronic: their watermark is the timestamp of the last
decoded event, so a contract with no events for 31+ days is refused every day from then
on, whether or not it has since emitted anything. The bootstrap fallback for an
unreadable watermark has the same shape (a `[warn]` line, then 7 slices).

## Forbidden action
Do not read a `pending`/`success` status or an Elementary `success` row as "this model
is current" for a microbatch model; check its data watermark against its source.
Do not raise `--max-slices-per-stage` on the cron path to make the refusals go away —
the cap exists so a multi-month gap is backfilled by `scripts/full_refresh/refresh.py`,
not chewed through by the daily runner.

## Detection
`dbt_model_status{status="refused"}` and `dbt_runner_refused_gap_days` after this
fix; before it, `target/failed_batches/runner-refused-*.json` in the pod, or the stderr
grep above. Independently: for every microbatch model, `max(date_column)` older than
`--max-slices-per-stage` days means the runner is refusing it.

## Safe remediation
For a genuinely stale model: `scripts/full_refresh/refresh.py --select <model>` for
the gap, then the daily runner resumes. For a dormant sparse decode: first check the
raw source (`execution.logs` for the contract since the watermark); if it is empty
the refusal is harmless today, but the model still needs a policy — either an
explicit "scanned-through" watermark that advances on empty days, or running the
refused decode with a bounded `incremental_end_date` so its block watermark can
move. Neither exists yet; the cap stays and the refusal is now visible.

## Ground truth
The model's `max(date_column)` versus the raw source's latest row for the same key
(contract address, validator band, ...).

## Enforcement
None yet: refusals are visible (status, gauge, stash) but still exit 0 by default.
Candidate: run the cron with `--fail-on-refusal` once the known sparse decodes are
either given a scanned-through watermark or moved off the production tag.
