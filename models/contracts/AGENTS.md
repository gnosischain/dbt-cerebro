# models/contracts — decode-layer rules

Every model here is a thin wrapper over `decode_logs()` / `decode_calls()`
(`macros/decoding/`). The directory-level config (`dbt_project.yml`) overrides the
global strategy to `append` on `ReplacingMergeTree`. Membership and counts: derive from
the manifest (`models/contracts` path), never from prose.

## The watermark is the whole game

- At render time the decode macros embed **literal** `max(block_number)` /
  `max(block_timestamp)` values from the target into the WHERE clause (they run
  `run_query` during compilation). This is deliberate: ClickHouse cannot prune
  partitions from scalar-subquery watermarks. **Do not** "simplify" back to a scalar
  subquery — see `docs/lessons/decoder-watermark-literals.md`.
- There is **no lookback**. A log backfilled into `execution.logs` for an
  already-passed range sits below the watermark and is dropped **forever** by normal
  runs. Balances scream (negatives); counts/aggregates silently undercount. See
  `docs/lessons/decode-watermark-late-logs.md`.
  - Recovery lever: `scripts/refresh/gap_window_refresh.py --months <gap months>
    --select <decode>+` — never the daily runner, which only advances the watermark.
- An **empty** decode target has watermark 0 and full-scans the raw source every run.
  Seed a new decode model once with `dbt run --full-refresh -s <model>`.

## Adding a decode model

- New decode models crash `dbt docs generate` at compile time until first built —
  build before generating docs.
- A lookback would NOT be dup-safe here: `append` + downstream reads without `FINAL`
  double-count until a background merge. The posture is detect-then-reprocess
  (raw-vs-decoded parity checks), not daily lookback.
- ABIs: `macros/decoding/fetch_abi_from_blockscout.sql` / `fetch_and_insert_abi.sql`.
