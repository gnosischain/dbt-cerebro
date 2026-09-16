---
id: microbatch-append-window-must-be-strategy-aware
title: A hand-rolled whole-month incremental window re-appends the month on every microbatch slice
status: remediated
scope: >-
  any incremental model whose strategy is the expression ('append' if (start_month or
  incremental_end_date) else 'insert_overwrite') and that computes its own incremental
  window instead of calling apply_monthly_incremental_filter (canonical: the three
  int_rpc_state_indexer_token_* models on their first cron, 2026-09-16)
symptom: >-
  after one cron a month partition holds several times its distinct keys
  (count() > uniqExact(grain)); consumers reading without FINAL over-count until the
  ReplacingMergeTree merges, and the duplicates come back every day
last_verified: 2026-09-16
evidence:
  - 'cron dbt-cerebro-cron-29825640 (2026-09-16 06:00 UTC, image 7d3ac76) ran each census model twice (two slices, ~20 s apart); September in int_rpc_state_indexer_token_balances_daily held 17,481,116 rows for 5,959,834 keys, the priced model 1,056,905 rows for 398,386 keys on 2026-09-15 alone'
  - 'macros/db/get_incremental_filter.sql apply_monthly_incremental_filter — strategy-aware; the insert_overwrite branch returns whole months (capped by incremental_end_date), the incremental_end_date branch returns strictly > max(date) AND <= end'
  - 'fix: the three models call apply_monthly_incremental_filter(<source date>, ''date'', ''true'') instead of the hand-rolled elif is_incremental() month branch; proven 2026-09-16 10:32 UTC with scripts/refresh/dbt_incremental_runner.py in the docker dbt container — one append each, rows == keys for 2026-09-15 and all of September'
  - 'September deduplicated with OPTIMIZE TABLE … PARTITION ''2026-09-01'' FINAL (alter_sync=2), 10:08 UTC'
---

## Symptom
The first cron on the census models left the September partition of the two balance
models about three times its size in rows, with the distinct-key count unchanged. Every
daily run would have added another copy.

## Root cause
Under the microbatch runner the strategy expression resolves to `append` (the runner passes
`incremental_end_date`). The models' incremental branch selected the whole current month —
the right window for `insert_overwrite`, which REPLACEs a partition, and the wrong one for
`append`, which adds rows. Each slice and the plain flush appended the month again.

## Forbidden action
Never hand-roll the incremental window on a model with the strategy expression. Call
`apply_monthly_incremental_filter`, which knows the resolved strategy: whole months under
`insert_overwrite`, strictly after `max(date)` under the runner's append path, the lookback
path otherwise.

## Detection
Per month, `count() - uniqExact(<grain>)` must be 0 on every ReplacingMergeTree model built
by the runner. Verify a new model on the production path before pinning it: run the runner
in the docker container against the same warehouse and re-check.

## Remediated
The three census models use the macro (commit after 98c8538). Not enforced: no gate compares
a model's window logic to its strategy; the per-month duplicate check is manual.
