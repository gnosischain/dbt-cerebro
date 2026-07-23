# Lessons index

Mistake classes this repo has already paid for. Each record carries a status —
`observed` (seen, no safeguard) → `remediated` (instance fixed, recurrence possible) →
`enforced` (a gate/test/code fix prevents recurrence) — plus evidence refs and the safe
remediation lever. **Status describes the DEPLOYED state**: production runs the
CI-built image from merged main, so a fix that only exists in the working tree is at
most `observed` with a "fix pending deploy" note — it re-arms in production nightly
until merged (learned the hard way; see sparse-zero-row-stale-survival's evidence).
**Check here before diagnosing a data-quality symptom or running any
refresh/backfill.** New lesson? Use the `/incident` command (evidence required).

## Wipes and data loss

- [staged-insert-overwrite-wipe](staged-insert-overwrite-wipe.md) `enforced` — any run
  window narrower than the partition on an insert_overwrite model REPLACEs the whole
  partition; staged models must use append-if-start_month.
- [wide-delete-insert-wipe](wide-delete-insert-wipe.md) `enforced` — a failed
  delete+insert keeps deleting in the background after dbt errors; reprocess per slice.
- [table-mat-batch-vars-truncation](table-mat-batch-vars-truncation.md) `observed` —
  batched refreshes truncate table-materialized month-var models to the last batch.

## Incremental / backfill correctness

- [decode-watermark-late-logs](decode-watermark-late-logs.md) `remediated` — append
  decode watermarks drop backfilled logs forever; recover with gap_window_refresh.py.
- [backfill-order-cumulative](backfill-order-cumulative.md) `observed` — downstreams
  reading `{{ this }}` need history backfilled first, chronologically.
- [late-start-mis-staging](late-start-mis-staging.md) `remediated` — a stage start_date
  later than real first activity silently truncates history.
- [duplicate-seed-drift](duplicate-seed-drift.md) `remediated` — a duplicate seed row in
  an append chain carries a constant error forward every day.
- [append-over-populated-duplicates](append-over-populated-duplicates.md) `remediated` —
  scoped append over populated months exactly doubles the data.
- [never-seeded-incremental](never-seeded-incremental.md) `remediated` — an incremental
  created before its input existed stays at 0 rows forever; seed with --full-refresh.
- [global-frontier-carry-forward](global-frontier-carry-forward.md) `enforced` — shared
  max(date) frontiers drop thin series; anchor carry-forward per entity.
- [refresh-state-collision](refresh-state-collision.md) `enforced` — run state is
  identity-keyed under target/refresh_state/; refresh.py refuses overlapping starts.
- [frontier-day-incomplete-inputs](frontier-day-incomplete-inputs.md) `remediated` — a
  cumulative chain that builds the frontier day before its inputs settle freezes the
  hole; upstreams self-heal, the cumulative layer never revisits the day.
- [refill-append-aggregator-inflation](refill-append-aggregator-inflation.md)
  `remediated` — an aggregator run in the same dbt invocation as its source's append
  reads 2x live RMT rows and bakes doubled values that row-level dup checks miss.
- [sparse-zero-row-stale-survival](sparse-zero-row-stale-survival.md) `enforced`
  — a sparse table that drops zero rows can never overwrite a stale key with "zero";
  spend-to-zero keys survive every reprocess and inflate apparent supply. Tombstone
  fix + dq_daily_balance_conservation DEPLOYED 2026-07-18 (image b930150); 19-token
  pre-deploy backlog re-cleaned 2026-07-19 (conservation 0 all July days, dq suite
  8 PASS/0 WARN). Detection: dq_daily_balance_conservation.

## ClickHouse platform

- [ch-overcommit-victim](ch-overcommit-victim.md) `remediated` — Code 241 citing the
  server "(total)" cap = you're the victim of saturation, not a fat query.
- [ch-partition-cap](ch-partition-cap.md) `remediated` — >100 partitions/INSERT fails
  (252), Cloud blocks raising it (452); never year-partition an insert_overwrite model.
- [ch-alias-shadows-where](ch-alias-shadows-where.md) `remediated` — output aliases
  shadow source columns in same-level WHERE; isolate/relabel in another scope.
- [ch-left-join-nulls](ch-left-join-nulls.md) `remediated` — LEFT JOIN misses return
  defaults not NULLs; pre_hook SET join_use_nulls = 1 (convention; intent isn't
  statically gateable).
- [decoder-watermark-literals](decoder-watermark-literals.md) `enforced` — scalar
  subquery watermarks can't prune partitions; the decode macros embed literals.

## Pipeline / sources

- [raw-logs-ingestion-holes](raw-logs-ingestion-holes.md) `observed` — execution.logs
  can have block holes below dbt; no dbt lever fixes them. May/June 2026 instances
  recovered end-to-end; the 2026-07-08 instance is raw-backfilled but decode
  recovery is pending (below-watermark — needs gap_window_refresh.py).
- [event-struct-array-decode-unreliable](event-struct-array-decode-unreliable.md)
  `observed` — a decoded event `tuple[]` (Balancer V3 tokenConfig) mis-decodes inner
  tokens to 0x0, emits bogus addresses, and misplaces real ones; derive positional
  token maps from an independent source (address-sorted swap tokens == Vault order).
- [unpriced-wrapper-token](unpriced-wrapper-token.md) `remediated` — every new
  wrapper/vault token needs a price path or it reads $0 everywhere.
- [stale-snapshot-caveat](stale-snapshot-caveat.md) `observed` — argMax "latest" marts
  silently serve the last ingested (possibly partial) day; check max(date) first.
- [elementary-artifact-upload-tax](elementary-artifact-upload-tax.md) `enforced` —
  Elementary artifact autoupload is disabled globally, refreshed once per run.

## Process / registry

- [semantic-retirement-gate](semantic-retirement-gate.md) `enforced` — retiring/renaming
  a model with metrics on it breaks the strict CI registry gate; grep semantic/ first.
- [docs-catalog-zero-nodes](docs-catalog-zero-nodes.md) `observed` — catalog.json has 0
  model nodes on dbt-clickhouse; expected, don't chase.

## Playbooks & primers

- [balance-diagnosis-playbook](balance-diagnosis-playbook.md) — Int256 not Float64,
  bare-hex topics, block↔date via execution.blocks, verify against the chain.
- [ch-merge-semantics-primer](ch-merge-semantics-primer.md) `primer` — why the engine
  zoo exists (merge-time collision policies), the three duplication traps, and why the
  repo's write conventions are all idempotency moves. Read when RMT duplicates confuse.
