# Model review: execution/transactions

**Convergence:** converged in 1 round — inspector and context reports were mutually consistent on all material findings; the arbiter directly verified the three load-bearing items and closed the one open question (gas-share denominator) as a non-defect.

---

## Scope and inventory

The unit covers every EVM transaction on Gnosis Chain from genesis (October 2018) onward. It is organized in three layers:

| Layer | Count | Description |
|---|---|---|
| Intermediate | 7 | Core computation: daily info, by-project daily/alltime/hourly-recent, unique addresses, cumulative daily, daily active addresses |
| Fact/mart | 6 | fct_ tables: active accounts daily, snapshots, by-project snapshots, by-project monthly top-5, by-project alltime, by-sector weekly |
| API views | 31+ | api_ prefix wrappers: counts, fees, gas, gas share, active accounts — at hourly, daily, weekly, monthly, all-time granularities |

Paths: `models/execution/transactions/intermediate/` (7 SQL files) and `models/execution/transactions/marts/` (38 SQL files, 2 schema.yml files). All files were read in full by the inspector.

---

## Business context

**What it measures.** Transaction counts, native xDAI gas fees, and unique initiator accounts on Gnosis Chain, sliced by transaction type, labelled project, and business sector. Time granularities: hourly (48-hour rolling window), daily, weekly, monthly, all-time cumulative. Feeds the "OnChain Activity" dashboard sector, the cerebro-api REST API, and the cerebro-dev MCP semantic layer.

**Canonical definitions.**

- **transaction_count** — count of on-chain EVM transactions where `success=1`, deduplicated on `(block_number, transaction_index)`. Reverted transactions are excluded from project/sector counts but retained in `int_execution_transactions_info_daily` under `success=0` for monitoring.
- **fee_native_sum** — `SUM(gas_used * gas_price / 1e18)` in xDAI. "Native" = xDAI throughout this unit.
- **fee_usd_sum** — `fee_native_sum * xDAI_price` from `int_execution_token_prices_daily`, with `coalesce(price, 1.0)` fallback when price data is absent.
- **active_accounts / initiators** — count of distinct `from_address` senders. Daily/all-time exact deduplication uses `groupBitmapState/groupBitmapMerge` over `cityHash64(lower(from_address))`. Windowed (1D/7D/30D/90D) deduplication uses `countDistinct` over `int_execution_transactions_daily_active_addresses` (181-day horizon). All-time cumulative reads `int_execution_transactions_cumulative_daily`.
- **cumulative_accounts** — stock metric; read only the latest-date row; do not sum across periods.
- **change_pct** — `(current_window / prior_window - 1) * 100`, rounded to 1 decimal; NULL for the 'All' window.
- **project** — human-readable label for `to_address` from `int_crawlers_data_labels`; 'Unknown' when unlabeled or NULL (contract creation). Multi-address projects collapse to one row per day.
- **sector** — business sector from the same label registry; defaults to 'Unknown'.
- **gas_share** — per-project `gas_used_sum / day_total_gas_used * 100`, rounded to 2 decimal places; denominator includes all rows (`WHERE date < today()` consistently applied to both numerator and denominator — verified not a defect).

**Semantic coverage.** 33 semantic model entries, 70+ candidate metrics. Only 4 nodes carry `quality_tier:approved`: `transaction_count` and `transaction_fees_native` metrics plus their two source models (`execution_transactions_by_sector_daily`, `execution_transactions_fees_native_by_sector_daily`). All remaining metrics are `quality_tier:candidate` and carry an explicit "review before relying" note. Conservative scoping; appropriate.

**Contract context.** No hardcoded contract addresses. Project/sector attribution is cleanly delegated to `int_crawlers_data_labels`.

---

## Implementation assessment

### Critical

**gas_price_avg and gas_price_median truncated to 0 via CAST to Int32; schema.yml declares Float64.**
`int_execution_transactions_info_daily` computes `CAST(avg_wei / 1e9 AS Int32)` and `CAST(median_wei / 1e9 AS Int32)` for Gwei conversion. EIP-7702 type-4 transactions have sub-1-Gwei effective prices (fee > 0, derived Gwei < 1), so the cast truncates to 0. Verified in production: 1,477 rows have `gas_price_avg=0` and 2,722 rows have `gas_price_median=0` while `fee_native_sum > 0` — mathematically incoherent. These zeroes are served directly to the tier-1 endpoint `api_execution_transactions_gas_used_daily`. Compounding the bug, `schema.yml` declares both columns as Float64, masking the integer truncation from consumers and schema tests.
Affected: `models/execution/transactions/intermediate/int_execution_transactions_info_daily.sql`, `models/execution/transactions/intermediate/schema.yml`, `models/execution/transactions/marts/api_execution_transactions_gas_used_daily.sql`

### High

**`int_execution_transactions_unique_addresses` carries ~209k unmerged duplicate rows (append + no OPTIMIZE FINAL).**
The table uses an append incremental strategy on a ReplacingMergeTree ordered by `address_hash` alone. `count()=4,506,746` vs `uniqExact(address_hash)=4,297,638` — 209,108 extra rows. Of 143,484 duplicated address hashes, 130,568 (91%) carry conflicting `first_seen_date` values across copies. The cumulative_daily consumer correctly guards with `min(first_seen_date) GROUP BY address_hash`, so the all-time headline count (4,297,638) is accurate today. However, any future raw reader that does not replicate this deduplication pattern will over-count, and the append model accumulates further duplicates with each incremental run. No OPTIMIZE FINAL is scheduled.
Affected: `models/execution/transactions/intermediate/int_execution_transactions_unique_addresses.sql`, `models/execution/transactions/intermediate/int_execution_transactions_cumulative_daily.sql`, `models/execution/transactions/intermediate/int_execution_transactions_daily_active_addresses.sql`

**`int_execution_transactions_by_project_hourly_recent` full-table rebuild can silently drop newest hours across UTC midnight.**
The model is `materialized='table'` with watermark `toStartOfDay(max(block_timestamp))` and filter `[subtractDays(max_day, 2), max_day)`. A build that crosses UTC midnight advances `max_day`; the new day's hours fall outside the window and are excluded until source data is sufficient to shift `max_day` again, leaving a visible gap in hourly dashboard charts. Verified current data: `min_hour=2026-06-10`, `max_hour=2026-06-11`, 1,539 rows, 67 projects — currently healthy, but the boundary risk is structural.
Affected: `models/execution/transactions/intermediate/int_execution_transactions_by_project_hourly_recent.sql`

### Medium

**`by_project_daily` is 4 days stale (max_date = 2026-06-07 vs today 2026-06-11); `info_daily` is 2 days stale.**
The 4-day lag cascades to all project-, sector-, and snapshot-level marts. Elementary freshness tests exist but apparently did not alert. Confirm whether this is expected cadence and whether freshness thresholds need tightening.
Affected: `models/execution/transactions/intermediate/int_execution_transactions_by_project_daily.sql`

**`by_project_daily` (delete+insert) vs `alltime_state` (insert_overwrite) can desync on late-arriving corrections.**
`by_project_daily` uses delete+insert with a 1-month lookback; `alltime_state` uses insert_overwrite over monthly partitions driven by `start_month`/`end_month` variables. A historical correction in `by_project_daily` requires a manually matched re-run of `alltime_state`. No CI guard, lineage test, or documented remediation procedure covers this.
Affected: `models/execution/transactions/intermediate/int_execution_transactions_by_project_daily.sql`, `models/execution/transactions/intermediate/int_execution_transactions_by_project_alltime_state.sql`

**`daily_active_addresses` 181-day hard-coded horizon used for 90D window with no boundary test.**
`int_execution_transactions_daily_active_addresses` hard-codes `WHERE date > subtractDays(today(), 181)`. The 90D window in `fct_execution_transactions_snapshots` is within this horizon under normal conditions. During a partial rebuild or data gap, the effective boundary could clip the 90D window and silently under-count active accounts. No test guards the margin.
Affected: `models/execution/transactions/intermediate/int_execution_transactions_daily_active_addresses.sql`, `models/execution/transactions/marts/fct_execution_transactions_snapshots.sql`

**`int_execution_transactions_by_project_hourly_recent` issues 12 cluster-wide `SYSTEM DROP CACHE` pre-hooks.**
All four cache types (mark, uncompressed, compiled-expression, query) are dropped globally before every full table rebuild on the shared ClickHouse Cloud cluster. The memory-limiting `query_settings` are justified by the wide label join, but cache drops are cluster-wide and degrade concurrent query performance for unrelated workloads.
Affected: `models/execution/transactions/intermediate/int_execution_transactions_by_project_hourly_recent.sql`

### Low

**Schema.yml documents phantom columns that are not projected.**
`by_project_daily` and `hourly_recent` both list an `address` column in `intermediate/schema.yml`; neither model outputs that column (actual output: project, sector, date/hour, tx_count, bitmap state, gas_used_sum, fee_native_sum). `fct_execution_transactions_snapshots` and `by_project_snapshots` document a `max_date` column in `marts/schema.yml`; the final SELECT outputs only `label, window, value, change_pct`. This schema drift misleads MCP consumers and will keep Elementary `schema_changes` tests warning indefinitely.
Affected: `models/execution/transactions/intermediate/schema.yml`, `models/execution/transactions/marts/schema.yml`

**Gas-share partial-day denominator: verified NOT a defect.**
Context raised this as an open question. The `tot` CTE in `api_execution_transactions_gas_share_by_project_daily` does apply `WHERE date < today()` consistently with the outer numerator join. Recorded to close the open question; no action required.
Affected: `models/execution/transactions/marts/api_execution_transactions_gas_share_by_project_daily.sql`

---

## Business-logic assessment

### High

**`fee_usd_sum` silently equals native amount when xDAI price is missing.**
`coalesce(px.price, 1.0)` assigns `fee_usd_sum = fee_native_sum * 1.0` when `int_execution_token_prices_daily` has no xDAI row for the date. Verified: 533 rows up to the latest populated day (2026-06-09) have `fee_usd_sum == fee_native_sum > 0`, spanning 2021-03-16 through today. The fallback is documented in a SQL comment (tied to the Chainlink native-price-feed migration in `docs/native_token_prices_build_plan.md`) but is invisible in served data. At a 1.1 USD/xDAI price, this produces roughly a 9% understatement of USD fees on affected dates with no flag or NULL to alert consumers.
Affected: `models/execution/transactions/intermediate/int_execution_transactions_info_daily.sql`, `models/execution/transactions/intermediate/int_execution_token_prices_daily.sql`

### Medium

**`change_pct` returns -100% instead of NULL when the prior window is 0.**
Both `fct_execution_transactions_snapshots` and `fct_execution_transactions_by_project_snapshots` compute `(coalesce(curr / nullIf(prev, 0), 0) - 1) * 100`. When `prev=0` (new or dormant project), `nullIf` returns NULL, `coalesce` returns 0, and the result is -100%. This mislabels an absent baseline as a total decline. The fix is to return NULL for this case.
Affected: `models/execution/transactions/marts/fct_execution_transactions_snapshots.sql`, `models/execution/transactions/marts/fct_execution_transactions_by_project_snapshots.sql`

**API-tag typo breaks MCP/endpoint routing convention.**
`api_execution_transactions_by_project_monthly_top5` carries tag `api:transactions_coun_per_project_top5` (missing 't' in 'count'). Per `check_api_tags.py`, the canonical key `transactions_count_per_project_top5` is un-discoverable via convention-aware tooling. Additionally, two semantic models target the same concept with overlapping `question_synonyms`: `execution_transactions_by_project_monthly_top5` (count-only, pointing at the `api_` mart) and `fct_execution_transactions_by_project_monthly_top5` (multi-metric, pointing at the `fct_` mart). Which is canonical is undocumented.
Affected: `models/execution/transactions/marts/api_execution_transactions_by_project_monthly_top5.sql`

**`fct_by_project_monthly_top5` silently excludes the current partial month.**
`WHERE date < toStartOfMonth(today())` always drops the in-progress month. Sibling daily and weekly views serve data up to yesterday or last full week. The exclusion is undocumented; consumers expecting the current month in monthly charts see an unexplained gap.
Affected: `models/execution/transactions/marts/fct_execution_transactions_by_project_monthly_top5.sql`

### Low

**'Unknown' project/sector at ~1.9% with no upper-bound alert.**
1.89% of `by_project_daily` rows are `project='Unknown'` (2,785 of 147,365 rows across all 2,800 distinct dates). Consistent but unmonitored; a labelling regression in `int_crawlers_data_labels` could silently shift significant volume into the Unknown bucket.
Affected: `models/execution/transactions/intermediate/int_execution_transactions_by_project_daily.sql`

**No documented reconciliation between the three active-account counting paths.**
Daily/all-time totals use `groupBitmapMerge` (exact); cumulative uses `cityHash64` first-seen in `unique_addresses`; windowed snapshots use `countDistinct` over `bitmapToArray` expansion of `daily_active_addresses`. Each path is internally exact but no reconciliation test validates that the chain-total matches across paths.
Affected: `models/execution/transactions/marts/fct_execution_transactions_active_accounts_daily.sql`, `models/execution/transactions/marts/fct_execution_transactions_snapshots.sql`, `models/execution/transactions/intermediate/int_execution_transactions_unique_addresses.sql`

---

## Data findings

Queries run by inspector (11 total):

| Query | Result |
|---|---|
| `by_project_daily` grain check | 0 duplicates on `(date, project)` — clean |
| `info_daily` grain check | 0 duplicates on `(date, transaction_type, success)` — clean |
| `by_project_daily` max_date | 2026-06-07 (4 days stale vs today 2026-06-11) |
| `info_daily` max_date | 2026-06-09 (2 days stale) |
| `fee_usd_sum` fallback check | 533 rows with `fee_usd_sum == fee_native_sum > 0`, spanning 2021-03-16 to 2026-06-09 |
| `gas_price_avg=0` with non-zero fees | 1,477 rows (avg), 2,722 rows (median); all `transaction_type='4'` |
| `unique_addresses` row count | `count()=4,506,746` vs `uniqExact=4,297,638` — 209,108 extra rows; 130,568 hashes with conflicting `first_seen_date` |
| `cumulative_daily` latest value | 4,297,638 — matches deduped uniqExact; all-time headline is accurate |
| `daily_active_addresses` horizon | 179 days (2025-12-09 to 2026-06-07) — within the 181-day hard-code |
| `hourly_recent` coverage | `min_hour=2026-06-10`, `max_hour=2026-06-11`, 1,539 rows, 67 projects |
| `fct_execution_transactions_snapshots` | 15 rows; no `-100%` change_pct in current data (no new/dormant projects in active window) |

Key numbers:
- All-time unique initiator addresses: **4,297,638** (verified correct).
- Type-4 transaction impact: **1,477 rows / ~12.5%** of total `info_daily` rows show `gas_price_avg=0` despite `fee_native_sum > 0`.
- USD fee distortion: **533 rows** use `price=1.0` proxy across the full history including the most recent populated day.
- Duplicate address rows: **209,108** — safe only via the current consumer's `min() GROUP BY` guard.

---

## Pros / Cons

**Pros**

- Clean, well-layered pipeline with verified grain uniqueness on both core daily tables (0 duplicates).
- All-time cumulative account count (4,297,638) is correct: the consumer deduplicates the append-strategy `unique_addresses` table with `min(first_seen_date) GROUP BY`.
- Exact active-account deduplication via `groupBitmapState/groupBitmapMerge` over `cityHash64(lower(from_address))` — defensible methodology.
- Comprehensive Elementary coverage (column, volume, freshness, schema-anomaly tests) on key intermediate and mart tables.
- Clear separation of `success=1` (project/sector/account marts) vs both-flags (`info_daily` monitoring), with the asymmetry documented.
- Semantic layer is conservatively scoped: only 4 nodes are `quality_tier:approved`; 69+ auto-generated metrics are explicitly marked candidate.
- No hardcoded contract addresses; project/sector attribution cleanly delegated to `int_crawlers_data_labels`.
- Native-only metrics (xDAI) avoid cross-asset valuation ambiguity for the core count and fee KPIs.

**Cons**

- `gas_price_avg/median` are silently truncated to 0 for sub-1-Gwei type-4 transactions and served to a tier-1 API; `schema.yml` even declares them Float64, masking the integer truncation from consumers and tests.
- `fee_usd_sum` silently substitutes `price=1.0` for 533 rows up to the latest day, distorting USD fee totals with no in-band signal.
- `unique_addresses` carries ~209k unmerged duplicate rows (append + ReplacingMergeTree, no OPTIMIZE FINAL); safe only because every current consumer happens to deduplicate — a latent trap for any future raw reader.
- `hourly_recent` is a full table rebuild over a max_day-anchored window; a run crossing UTC midnight can drop the newest hours from dashboards.
- `by_project_daily` is 4 days stale, cascading to all project/sector/snapshot marts; the freshness test apparently did not alert.
- `change_pct` returns -100% (not NULL) when the prior window is 0, mislabelling new and dormant projects.
- Schema/tag drift: phantom `address` and `max_date` columns documented but not projected; api-tag typo `transactions_coun_per_project_top5` breaks convention-aware MCP routing.
- No documented reconciliation between the three active-account counting mechanisms, and no upper-bound alert on the ~1.9% 'Unknown' project coverage gap.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 | Change `gas_price_avg/median` to emit a float (e.g. `round(..., 4)` as Float32) rather than CAST to Int32; correct `schema.yml` type declarations; backfill affected partitions. | `int_execution_transactions_info_daily.sql`, `intermediate/schema.yml`, `api_execution_transactions_gas_used_daily.sql` |
| P0 | Replace `coalesce(px.price, 1.0)` with NULL on missing price (or add a `price_available` flag); add an Elementary alert when xDAI price coverage gaps appear. Backfill once the Chainlink native-price feed migration is complete. | `int_execution_transactions_info_daily.sql`, `int_execution_token_prices_daily.sql` |
| P1 | Resolve the 4-day staleness on `by_project_daily`; tighten the Elementary freshness threshold so this lag surfaces as an alert, since it cascades to all downstream project/sector/snapshot marts. | `int_execution_transactions_by_project_daily.sql` |
| P1 | Add OPTIMIZE FINAL (or switch to a FINAL read / merge incremental strategy) for `int_execution_transactions_unique_addresses`; add a test asserting `count() == uniqExact(address_hash)`. | `int_execution_transactions_unique_addresses.sql`, `int_execution_cumulative_daily.sql` |
| P1 | Re-anchor or incrementalize `int_execution_transactions_by_project_hourly_recent` so a build crossing UTC midnight cannot drop the newest hours; add a `max(hour) vs now()` freshness test. | `int_execution_transactions_by_project_hourly_recent.sql` |
| P2 | Fix `change_pct` to return NULL when `prior_window = 0` in both snapshot models. | `fct_execution_transactions_snapshots.sql`, `fct_execution_transactions_by_project_snapshots.sql` |
| P2 | Fix the api-tag typo to `api:transactions_count_per_project_top5`; run `check_api_tags.py`; document which of the two `monthly_top5` semantic models is canonical and remove the redundant entry. | `api_execution_transactions_by_project_monthly_top5.sql` |
| P2 | Remove the phantom `address` and `max_date` columns from `schema.yml` (or project them from the SQL) to stop schema-drift warnings and align MCP-documented columns with actual output. | `intermediate/schema.yml`, `marts/schema.yml` |
| P3 | Document and add a reconciliation test/runbook for `by_project_daily` (delete+insert) vs `alltime_state` (insert_overwrite) so late-arriving corrections cannot silently desync the all-time state. | `int_execution_transactions_by_project_daily.sql`, `int_execution_transactions_by_project_alltime_state.sql` |
| P3 | Add a reconciliation test across the three active-account counting paths; add a coverage-threshold alert on the 'Unknown' project share. | `fct_execution_transactions_active_accounts_daily.sql`, `fct_execution_transactions_snapshots.sql`, `int_execution_transactions_unique_addresses.sql` |

---

## Open disagreements

None. Reports converged in 1 round.

---

## Review log

| Round | Agent | Challenge | Outcome |
|---|---|---|---|
| 1 | Arbiter | Verified gas_price CAST to Int32 directly in source SQL | Confirmed critical; row counts revised upward (1,477 avg-zero, 2,722 median-zero) vs inspector's initial 817/1,533 from a narrower query |
| 1 | Arbiter | Verified fee_usd_sum fallback row count | Confirmed high; 533 rows (vs inspector's 284) to latest day 20613 |
| 1 | Context (open question) | Gas-share denominator partial-day mismatch | Closed as non-defect: tot CTE applies `WHERE date < today()` consistently with numerator |
| 1 | Arbiter | All other inspector findings | Accepted without rebuttal; no open disagreements |
