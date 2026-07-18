# scripts/full_refresh — staged full-history rebuilds

## `refresh.py` (orchestrator)

- Rebuilds a model's full history via its `meta.full_refresh` stages: **batch 1 runs
  `--full-refresh`** (recreates the table — this is what clears duplicate-seed drift),
  later batches append non-overlapping windows. OOM-safe by construction.
- **Run state:** check for pending state before starting a new run, and finish or
  explicitly abandon a pending `--resume` before selecting something that overlaps it.
  The runner refuses overlapping selections against pending state; if you hit that
  refusal, resume (or clear) the pending run — do not delete state files to force through.
  See `docs/lessons/refresh-state-collision.md`.

## Model-shape footguns

- **Staged models must use append-if-`start_month`.** A staged/batched refresh against
  an `insert_overwrite` model REPLACEs the month partition on every stage — the table
  ends up holding only the last stage. Three live incidents.
  See `docs/lessons/staged-insert-overwrite-wipe.md`.
- **Table-materialized models that branch on `start_month`/`end_month`** are rebuilt
  whole per batch — a batched refresh truncates them to the last batch. A plain
  `dbt run -s <model>` self-heals. See `docs/lessons/table-mat-batch-vars-truncation.md`.
- **Backfill ordering:** before backfilling a new token/dimension, classify downstreams:
  models reading `{{ this }}` are cumulative and need history backfilled first,
  chronologically. See `docs/lessons/backfill-order-cumulative.md`.

## Choosing a lever

Decision table: root [AGENTS.md](../../AGENTS.md#refresh-levers--which-tool-when).
For a known backfilled month in a decode chain use
`scripts/refresh/gap_window_refresh.py`, not a full-history rebuild.
