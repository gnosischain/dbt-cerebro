# models/quarterly_data/ — scoped guide

Quarter-grain snapshot marts used for quarterly reporting. Read with root
AGENTS.md.

## Invariants

- **"Quarter-end" is only as fresh as the source.** These marts take
  argMax-style latest values per quarter; when an upstream source halts, the
  last ingested day silently becomes "quarter end" and the number is wrong
  while looking authoritative. Before quoting ANY value: check the upstream's
  `max(date)` against the true period end (stale-snapshot-caveat). Consensus-
  derived snapshots (staked, validator counts) have been bitten by exactly
  this.
- **Exclude (or clearly label) the open period.** Aggregating a quarter that
  has not closed understates it; a quarterly report was restated for this.
  Comparisons must be closed-quarter vs closed-quarter.
- **Table-materialized models that branch on month vars must never go through
  a batched refresh** — each batch rebuilds the whole table and only the last
  batch survives; a plain `dbt run` self-heals
  (table-mat-batch-vars-truncation).

## Validation

- `python scripts/checks/run_all.py`; for report-facing numbers, cross-check
  one value per source family against the live upstream mart before signing
  off. Point-in-time endpoints need an as_of/date column (api-tags gate).
