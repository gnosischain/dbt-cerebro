# models/revenue — reprocess and backfill rules

Several models here union many per-stream slices into one windowed-incremental table
(canonical case: `int_revenue_fees_weekly_per_user`, strategy expression
`'delete+insert' if reprocess_overwrite else ('append' if start_month else 'delete+insert')`).
Two traps, both have caused live incidents:

## 1. Scoped append only into EMPTY months

The `start_month → append` path is correct **only** when the target months hold no rows
(the staged full-refresh orchestrator guarantees non-overlap). Re-running a scoped
`--vars {start_month,end_month}` over months that already hold data appends a second
full copy — exact 2× rows. The tables are `ReplacingMergeTree` but the marts read them
**without `FINAL`** and aggregate with `sum()`/`countIf()`, so both copies count.
See `docs/lessons/append-over-populated-duplicates.md`.

## 2. Reprocess per `slice`, never whole-window

The repair path (`reprocess_overwrite=true` → `delete+insert`) must run **one `slice`
at a time** (the model's `slice` var, format `stream:SYMBOL`). A whole-window run builds
a delete-set over every slice → OOM — and worse, the lightweight delete keeps running in
the background **after dbt has reported failure**, so the window gets fully deleted while
the insert never runs: a silent wipe. See `docs/lessons/wide-delete-insert-wipe.md`.

Do not "fix" this by switching the reprocess strategy to `insert_overwrite`: it rejects
`unique_key` and REPLACEs whole partitions, so a single-slice run would wipe the
co-slices sharing that month partition.

## Config hygiene

- Query knobs (`max_threads`, `max_bytes_before_external_group_by/sort`,
  `max_memory_usage`) go in `query_settings=`; `settings=` takes storage settings only.
- Contract-address leakage: revenue "active user" streams anti-join the contract
  denylist (`int_execution_accounts_label_contracts`) — keep new streams consistent, and
  never full-rebuild the big denylist joins in one shot (they OOM; batch them).
