---
id: ch-partition-cap
title: ">100 partitions per INSERT fails (252) and Cloud blocks raising the cap (452)"
status: remediated
scope: full rebuilds of wide-history month-partitioned tables; any single INSERT
  spanning many partitions
symptom: full-refresh fails Code 252 (max_partitions_per_insert_block); attempting to
  raise the setting fails Code 452
last_verified: 2026-07-17
evidence:
  - docs/model_review/execution-state.md:56 and docs/model_review/crawlers_data.md:52,199 (both document the 252/452 pair and the toStartOfYear convention)
  - toStartOfYear in use: models/execution/prices/intermediate/int_execution_token_prices_daily.sql, models/revenue/intermediate/int_revenue_fees_monthly_per_user.sql, models/execution/lending/marts/fct_execution_lending_weekly.sql, models/execution/transactions/marts/fct_execution_transactions_by_sector_weekly.sql, fct_execution_transactions_by_project_monthly_top5.sql, models/ESG/intermediate/int_esg_carbon_intensity_ensemble.sql
  - still on monthly partitions despite wide history (flagged, pending): int_execution_state_size_full_diff_daily, int_crawlers_data_labels
  - '2026-11-09 instance of the year-partition trap: int_esg_carbon_intensity_ensemble (insert_overwrite, toStartOfYear partitions, apply_monthly_incremental_filter) held only Dec 2024, Dec 2025 and Jun 2026 for 2024-2026, because each run REPLACEd the year partition with "latest month onwards". The daily ESG marts were unharmed only because they had already read those months; a downstream full refresh would have lost Jan-May 2026. Fix in tree, pending deploy: the model becomes a full-rebuild table (a few thousand rows), which also drops it from the toStartOfYear list above'
---

## Symptom
A full rebuild of a table with many months of history fails with Code 252; setting
`max_partitions_per_insert_block` higher fails with Code 452 (ClickHouse Cloud blocks
changing it).

## Root cause
One INSERT (a `--full-refresh` writes one) may touch at most 100 partitions. A
month-partitioned table with >100 months of history can never full-rebuild in one shot.

## Forbidden action
**Never** widen partitions to year on an `insert_overwrite` model — partition grain
must equal the overwrite grain or scoped runs wipe co-resident months
(staged-insert-overwrite-wipe). Year-partitioning is only safe on append/table models
or where every write rebuilds whole years.

## Safe remediation
Either partition wide-history tables by `toStartOfYear` (the project convention where
strategy-compatible), or rebuild in staged batches so no single INSERT spans >100
partitions (the full-refresh orchestrator's stages do this naturally).

## Ground truth
`system.parts` partition count for the table vs the insert window.

## Enforcement
Convention documented and applied to the models listed above; two flagged models remain
monthly (pending). No static gate.
