---
id: refresh-state-collision
title: Refresh runners used a single global state file — a new run clobbered a pending resume
status: enforced
scope: scripts/full_refresh/refresh.py, scripts/refresh/dbt_incremental_runner.py
symptom: a pending --resume becomes unresumable (or resumes the wrong plan) after a
  different --select run was started in between
last_verified: 2026-07-17
evidence:
  - historical bug locus — refresh.py used a single shared scripts/full_refresh/.refresh_state.json; the runner defaulted to a single target/incremental_microbatch_state.json; neither keyed state by selection
  - fix — scripts/refresh/run_state.py (shared identity module), refresh.py and dbt_incremental_runner.py wired to target/refresh_state/<tool>_<id>.json; tests/test_run_state.py
---

## Symptom
You interrupt a long staged refresh intending to `--resume` later; meanwhile another
refresh with a different `--select` runs. The original state is overwritten — the
resume either fails or silently resumes the wrong selection's plan.

## Root cause
Both runners persist run state at a fixed path shared by every invocation. State
identity is "whoever wrote last", not "which run is this".

## Forbidden action
Never start a new runner invocation with a different selection while a `--resume` is
pending on the same state path. If an upstream is missing mid-resume, build it with a
plain `dbt run -s <model>` — not a second runner invocation.

## Detection
Before starting: check for pending state (`scripts/full_refresh/.refresh_state.json`,
`target/incremental_microbatch_state.json` or the runner's `target/refresh_state/`
directory) and compare its recorded selection against yours.

## Safe remediation
Finish or explicitly clear the pending run first. For the microbatch runner,
`--state-file` can isolate a run manually.

## Ground truth
The state file's recorded selection/plan vs the run you're about to start.

## Enforcement
Run state is keyed by identity — a hash of the run-defining arguments — under
`target/refresh_state/<tool>_<id>.json` (`scripts/refresh/run_state.py`):

- **refresh.py (staged rebuilds — hard guard):** starting a run whose models overlap
  any pending run is *refused*; a pending same-identity run must be `--resume`d or
  `--clear-state`d; `--resume <id>/latest` is verified against the current selection;
  a legacy `.refresh_state.json` blocks with instructions instead of being guessed at.
- **dbt_incremental_runner.py (daily hot path — soft guard):** identity-keyed state,
  loud overlap *warning* only — a hard block would let a stray failed manual run halt
  the nightly cron, and the runner's planning is watermark-driven (self-healing), so a
  foreign pending run can't corrupt data through it. State clears on clean completion.

Tests: `tests/test_run_state.py`.
