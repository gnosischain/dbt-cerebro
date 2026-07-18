# scripts/refresh — daily runner and gap recovery

## `dbt_incremental_runner.py` (microbatch runner)

- **Daily forward catch-up only.** It advances watermarks in per-day slices. It refuses
  stages whose gap exceeds `--max-slices-per-stage` (pointing you at the full-refresh
  orchestrator) — do not disable the cap to force a backfill through it.
- It **cannot** seed an empty table (bootstraps only `--bootstrap-lookback-days` back),
  and it **never recovers backfilled history** — anything below a model's watermark is
  invisible to it.
- Resume state: see the runner `--help` for the state-file flags. Never point two
  concurrent runs at the same state file; never start a different selection expecting a
  pending `--resume` to survive.

## `gap_window_refresh.py`

- The correct lever when raw data was **backfilled into an already-passed month** of a
  decode chain (`docs/lessons/decode-watermark-late-logs.md`): drops the gap-month
  partition (lowering the decode watermark) and re-runs scoped with
  `--vars {start_month,end_month}`. Targeted and resumable — reach for this before the
  full-history orchestrator.

## Choosing a lever

The decision table lives in the root [AGENTS.md](../../AGENTS.md#refresh-levers--which-tool-when)
— read it before running anything that writes to the warehouse.
