# dbt-cerebro Model Review: Executive Report

> **HISTORICAL SNAPSHOT (2026-06-11).** Findings below may already be remediated — see
> [README.md](README.md) and [docs/lessons/INDEX.md](../lessons/INDEX.md). Re-verify
> before acting on anything here.

**Review date:** 2026-06-11  
**Scope:** 37 sector reports covering all models in the dbt-cerebro project  
**Methodology:** Three-agent review (Inspector + Context + Arbiter) per sector; warehouse queries run in each round; findings graded by severity  

---

## 1. Executive Summary

This review audited all SQL models, schema definitions, and semantic layer artifacts across 37 production and near-production sectors of the dbt-cerebro ClickHouse analytics platform serving Gnosis Chain. The review confirmed 20 active P0 incidents — conditions under which a live endpoint, dashboard, or metric currently returns materially wrong data to real users or downstream systems. In addition, the review identified 56 High-severity findings that degrade data quality, break semantic layer queries, or create latent correctness risks, plus a further 98 Medium/Low issues.

**Systemic root causes repeated across sectors:**

1. `join_use_nulls` absent from LEFT JOIN models — platform-wide ClickHouse default causes unmatched rows to return type defaults (`''`/`0`) instead of NULL. Confirmed data corruption in at least 12 sectors.
2. Semantic layer column mismatches — semantic YAML files reference columns that do not exist in live SQL, silently breaking MCP and `query_metrics` paths. Confirmed in 10 sectors.
3. `dev` tags on production-depended models — models with `tags=['dev']` are excluded from production CI and selective builds, silently breaking downstream production models. Confirmed in 5 sectors.
4. ReplacingMergeTree tables read without FINAL — stale or duplicate rows served to API consumers. Confirmed in 8 sectors.
5. Phantom schema.yml columns — schema.yml documents fabricated or stale column names; dbt tests run against non-existent columns. Confirmed in 14 sectors.
6. No blocking freshness tests on critical sources — stale data reaches live endpoints without CI failure. Confirmed in 9 sectors.
7. ClickHouse partition cap risk — `toStartOfMonth` partitioning on multi-year tables approaches or has already hit the 100-partition-per-insert CH Cloud hard limit (code 252). Confirmed active in 3 sectors; approaching limit in 3 more.
8. All semantic models at `quality_tier: candidate` — Tier 0 and Tier 1 public endpoints served by unreviewed candidate metrics. Confirmed in 9 sectors.

The aggregate health state of the platform is that approximately one-third of sectors contain at least one P0 incident, and fewer than a quarter of sectors are free of High-severity findings.

---

## 2. Active P0 Incidents

P0 is defined as: a live API endpoint, dashboard tile, or semantic metric currently serving materially wrong data to real consumers or silently producing zero/null output where a non-trivial value is expected.

| # | Sector | Incident | Impact |
|---|---|---|---|
| P0-01 | ESG | `int_esg_carbon_intensity_ensemble` partitioned by `toStartOfYear`, evicting all non-December rows. `fct_esg_carbon_footprint_uncertainty` has 31 rows (December-only). All carbon/energy KPIs derived from December data only. | All public ESG carbon and energy endpoints serve December-only values; 11 months of data silently absent |
| P0-02 | ESG | Semantic model measures `network_weighted_cif_value` and `total_estimated_nodes_value` reference nonexistent columns (`network_weighted_cif`, `total_estimated_nodes`; live columns are `effective_carbon_intensity`, `estimated_nodes`). | MCP `query_metrics` against ESG carbon intensity and node count always fails at runtime |
| P0-03 | Consensus | `apy_30d` formula in `fct_consensus_validators_explorer_latest` and `fct_consensus_validators_explorer_members_table` overstates APY by ~30x. Warehouse confirmed: median APY 229%, max 3,322,693% vs true network APY ~10%. | Validator Explorer dashboard headline APY figure is factually wrong for every row |
| P0-04 | Consensus | `int_consensus_validators_labels` tagged `dev` with bare table reference (not `ref()`). `int_consensus_validators_withdrawal_addresses` depends on it. | Production withdrawal-address pipeline broken in any build that excludes dev-tagged models; `is_validator_depositor` in `int_execution_address_roles_current` zeros out |
| P0-05 | Contracts AMM-DEX | 7 newly-whitelisted UniswapV3Pool addresses (added 2026-05-14 and 2026-05-21) have no historical data — incremental gate on `block_number > max(block_number)` is already past their addition dates. | Historical DEX data for 7 pools permanently missing; no backfill mechanism |
| P0-06 | Contracts Circles | 5 calls models (`StandardTreasury_calls`, `InvitationEscrow_calls`, `CirclesBackingFactory_calls`, `ERC20TokenOffer_calls`, `PaymentGateway_calls`) use `execution.transactions` instead of `execution.traces`. 0 rows despite paired events models having thousands of rows. | All Circles contract call-level analytics permanently empty |
| P0-07 | Contracts Lending | `contracts_agave_LendingPool_events`: 63,381,046 rows, 100% `event_name = ''` — Agave ABI never loaded into `event_signatures`. Model burns daily compute for zero analytical value. | Agave lending protocol entirely absent from all analytics; 63M rows of wasted compute per run |
| P0-08 | Contracts Prediction | `OmenAgentResultMapping` 309 days stale (max 2025-08-06). `dbt_incremental_runner.py` `max_slices_per_stage=30` silently drops models with >30-day gap, exits 0. | Omen AI agent resolution data 10 months behind; prediction market analytics unreliable |
| P0-09 | Execution Accounts | `fct_execution_account_token_movements_daily` has 0 rows. 100% of 1,318,764 account profiles have null/zero `token_transfer_count`. | Account portfolio transaction history entirely blank for all users |
| P0-10 | Execution CoW | `crawlers_data.cow_api_trade_fees` max `ingested_at` = 2026-04-30 (42 days stale). 0.18% of recent trades have `fee_source='api'`. `api_execution_cow_kpi_fees_7d` and `api_execution_cow_kpi_solver_value_7d` return NULL. `execution_cow_top_pairs_weekly` semantic model broken (column mismatch: `week/pair/volume_usd/num_trades` vs actual `date/label/value`). | CoW Protocol fee and solver-value KPIs serve NULL; approved CoW top-pairs semantic metric fails at query time |
| P0-11 | Execution GBCDeposit | Approved-tier semantic entity emits garbage addresses for 6,667/14,513 rows (45.9%) — no `0x01` type guard for BLS credentials. `amount` column is raw Gwei; approved metric `GBCDeposit_deposists_daily__amount_value` sums Gwei directly (~1e9x larger than any GNO figure). | GBC deposit semantic metrics and entity addresses factually wrong in production |
| P0-12 | Execution Gnosis App | `int_execution_gnosis_app_user_activity_daily` truncated to 2-month window (2026-05-01 to 2026-06-01) due to persisted `start_month`/`end_month` build vars. 2,477 rows vs 22,644 users — 89% of user history missing. Swap fee revenue universally zero across 40,790 filled trades (`fee_amount = 0`). | App user activity history and fee KPIs both wrong; retention and onboarding metrics computed on a fraction of actual user base |
| P0-13 | Execution GPay | `api_execution_gpay_user_total_cashback` and `api_execution_gpay_user_cashback_daily` publish native GNO as "USD" — schema.yml explicitly says "in USD". GNO trades at $100-$300. | Cashback USD figures overstated by 100-300x for all users |
| P0-14 | Execution GPay | `int_execution_gpay_activity_daily` ReplacingMergeTree `order_by` omits `direction` — engine-level data loss on the central activity spine. 234 (card/token) groups with 2 distinct directions confirmed. 20+ downstream marts inherit corrupted aggregates. | GPay payment direction split silently collapsed; all direction-aware aggregates and dashboards corrupted |
| P0-15 | Execution Lending | Int256 underflow corrupts WxDAI utilization rate. `cumulative_scaled_borrow` goes negative; `toUInt256(negative)` wraps to ~2^256. Live `utilization_rate ~4.5e27` instead of ~42%. Propagates to `fct_execution_yields_opportunities_latest`. | WxDAI lending utilization rate ~4.5e27 on all Yields opportunity and lending dashboard surfaces |
| P0-16 | Execution Prices | SAFE price forward-filled from 2025-11-18 (220 days stale) due to uncapped `last_value IGNORE NULLS OVER (UNBOUNDED PRECEDING)` always winning over Dune priority-3 fallback. Hub SAFE = $0.3667; Dune SAFE = $0.1175 — 3.1x overstatement. | All downstream USD valuations for SAFE (pools TVL/fees, balances, MMM controls) inherit 3.1x overstatement |
| P0-17 | Execution State | `int_execution_state_size_full_diff_daily` counts slot overwrites as new allocations (+32 bytes each). On verified sample day: 58% of rows are overwrites. Cumulative state served as ~70.7 GB vs corrected ~28 GB (2.51x overcount). | Tier-1 API `/v1/execution/state_size/daily` reports Gnosis Chain state as 2.5x larger than reality |
| P0-18 | Execution Shared | `is_lending_user` = 0 for all 5.8 million addresses. Root cause: `fct_execution_yields_user_lending_positions_latest` has 0 rows due to partial-day latest-date selection (`max(date) WHERE date < today()`) landing on a zero-balance partial load. | Graph Explorer role badges, Portfolio overview, and semantic layer show zero lending users across the entire platform |
| P0-19 | Execution Yields | `fct_execution_yields_user_lifetime_metrics`: `least(DateTime, NULL)` coerces `first_yield_date` to 1970-01-01 for all 6,055 wallets; `active_lending_positions = 0` for all wallets simultaneously. | Yields user-portfolio KPI surface completely broken; all API wallet-lookup endpoints return epoch dates and zero lending positions |
| P0-20 | Mixpanel GA | `fct_mixpanel_ga_gnosis_app_users.matched_mp = 1` for every row due to `join_use_nulls=0`; `fct_mixpanel_ga_gpay_crossdomain_daily` activity flags (`users_with_delay_activity_7d`, `users_with_allowance_changes_30d`) inflated to 100% of matched users for same reason. | GA sector health diagnostic and GP cardholder engagement metrics both report 100% values; stored in materialized tables |

---

## 3. Critical Findings by Sector

### ESG
- Partition eviction bug destroys 11 months of carbon intensity data annually (`toStartOfYear` on `int_esg_carbon_intensity_ensemble`). P0-01.
- Semantic column name drift: `network_weighted_cif` / `total_estimated_nodes` vs live `effective_carbon_intensity` / `estimated_nodes`. P0-02.
- 9.7% of rows have `carbon_intensity_gco2_kwh = 0.0` from missing `join_use_nulls` on LEFT JOIN.

### Bridges
- `bridges_kpis_snapshot` semantic model `agg_time_dimension` references nonexistent column `d` — all 14 auto-generated KPI metrics broken for MetricFlow time-series queries.
- `net_usd`, `netflow_usd_week`, `value` declared `UInt64` in schema.yml but are Float64 with 30-39% negative values.

### Consensus
- `apy_30d` overstated 30x in two marts (median 229%, max 3,322,693%). P0-03.
- `int_consensus_validators_labels` dev-tagged with bare table ref. P0-04.
- 6,712 active 0x02 validators excluded from `user_pseudonym` cross-sector join.
- 4-day lag with 6-day snapshot gap (2026-06-01 to 2026-06-06 entirely absent); all freshness tests at `severity: warn`.

### Contracts AMM-DEX
- 7 UniswapV3Pool addresses missing all historical data due to incremental backfill gap. P0-05.
- All 4 `_live` tables return 0 rows (scheduler gap exceeded 2-hour TTL).
- BalancerV2 excluded from all fee_apr/TVL analytics without disclosure (25.9M events, live since Nov 2022).
- Curve 3pool has no decoded Swap events — Curve DEX volume entirely absent.

### Contracts Circles
- 5 calls models permanently 0 rows — `execution.transactions` used instead of `execution.traces`. P0-06.

### Contracts Lending/Tokens
- Agave `LendingPool_events`: 63.4M rows, 100% undecoded. P0-07.
- `contracts_backedfi_bC3M_Oracle_events` 49 days stale.
- Phantom schema.yml columns across majority of models.

### Contracts Prediction Markets
- `OmenAgentResultMapping` 309 days stale. `dbt_incremental_runner.py` 30-slice cap silently drops models. P0-08.
- `FPMMDeterministicFactory_events` captures only factory events — zero Omen trading activity.

### Crawlers Data
- `int_crawlers_data_labels` at 94-month span — ~6 months from CH Cloud 100-partition hard block on full rebuild.
- `stg_crawlers_data__dune_bridge_flows_v2` references non-existent columns.

### Execution Accounts
- `fct_execution_account_token_movements_daily` 0 rows. P0-09.
- `api_execution_account_balance_history_daily` reads RMT without FINAL.

### Execution Circles v2
- `int_execution_circles_v1_transfers` SQL schema drift (9 cols emitted vs 13 in warehouse).
- 3 semantic candidate models reference nonexistent columns.

### Execution CoW
- `cow_api_trade_fees` 42 days stale; `api_execution_cow_kpi_fees_7d` returns NULL. P0-10.
- `execution_cow_top_pairs_weekly` semantic model column mismatch. P0-10.

### Execution GBCDeposit
- Approved entity emits garbage addresses for 46% of rows. Approved metric sums Gwei as GNO. P0-11.

### Execution Gnosis App
- 89% of user activity history missing; swap fee revenue universally zero. P0-12.

### Execution GPay
- Cashback endpoints publish GNO as USD (~100-300x). P0-13.
- Activity spine RMT key omits direction. P0-14.

### Execution Lending
- WxDAI utilization rate ~4.5e27 (Int256 underflow). P0-15.
- `balance_usd = 0` for 51-100% of SparkLend/Aave rows for GNO, sDAI, wstETH, USDC.e.

### Execution Pools
- Balancer V3 negative TVL on ERC4626-wrapper pools (-$2.1M/day, 28% of V3 rows). Reaches downstream TVL aggregations.
- LVR sign contract violated: 12% of rows positive vs schema claim "always <= 0".
- 13 `api_execution_trades_stats_*` models carry `dev` tag, bypassing CI guard.
- Balancer V2 absent from `fees_daily` — zero volume and fees for 93% of pool-rows.

### Execution Prices
- SAFE price 3.1x overstated (220-day forward-fill). P0-16.
- No forward-fill staleness cap — structural defect for any DEX-priced token losing liquidity.

### Execution RWA
- bC3M oracle silent 49 days; stale 126.2 price propagates to all downstream USD valuations.
- `elementary.freshness_anomalies` structurally blind — forward-fill guarantees a row every day regardless of oracle health.

### Execution Safe
- v1.4.1 AddedOwner/RemovedOwner `indexed:false` in `seeds/event_signatures.csv` — ~107k owner events silently NULL. P0-24-class issue.
- 8 duplicate Safe addresses in `int_execution_safes` served without FINAL.
- 16.7% of current owner rows have NULL `current_threshold`.

### Execution Shared
- `is_lending_user = 0` for all 5.8M addresses. P0-18.

### Execution State
- `bytes_diff` counts slot overwrites as new allocations — ~2.5x overcount served via tier-1 API. P0-17.
- Pipeline 132 days stale at source.

### Execution Tokens
- Circulating supply can go negative — no `balance > 0` guard. wstETH currently ~9% understated.
- Semantic model exposes entirely wrong column set.

### Execution Transactions
- `gas_price_avg/median` CAST to Int32 truncates type-4 transactions to 0. 1,477 zero-price rows with non-zero fee served via tier-1 API.
- ~209k unmerged duplicate rows in `unique_addresses`.

### Execution Transfers
- `int_execution_bridges_address_flows_daily` missing `join_use_nulls` — direction always `'out'`, 98.6% rows `bridge_contract=''`, 6.16M historical rows mislabelled.
- `volume_usd` hardcoded NULL in bridges semantic model.

### Execution Yields
- `first_yield_date = 1970-01-01` for all 6,055 wallets; `active_lending_positions = 0`. P0-19.
- Approved-tier semantic measures reference nonexistent columns.

### Mixpanel GA
- `matched_mp = 1` for all rows; GP activity flags inflated to 100%. P0-20.

### P2P
- `pct_successful` for discv4 is always 100% — vacuous metric.
- 53% of discv4 peer rows have empty-string geo from missing `join_use_nulls`.
- Topology map drops 65% of discv4 edges and 54% of discv5 edges due to same bug.

### Probelab
- All 5 `api_*` marts missing `api:` and `granularity:` tags — bypass CI guard and endpoint registry.

### Revenue
- Monthly pipeline ~75% coverage gap: Jan-Sep missing for 2023/2024/2025, Jan-Mar 2026 absent. Q1 2026 entirely unreportable.
- All 28 `api_revenue_*` views globally allowlisted in `check_api_tags.allow`.

---

## 4. Systemic Patterns

### Pattern 1: join_use_nulls absent across the platform
ClickHouse Cloud does not set `join_use_nulls=1` by default. Unmatched LEFT JOIN columns return type defaults (`''`, `0`) instead of NULL. This single misconfiguration class causes data corruption in at least 12 confirmed locations: ESG carbon intensity (9.7% zero rows), GPay amount_usd (22,549 zero rows), P2P discv4 geo (53% empty strings), P2P topology (65% and 54% dropped edges), Bridges direction (always `'out'`), Mixpanel GA (100% match rate), UBO resolved mart (latent), Transfers (direction and bridge filtering completely broken), Safe marts, Zodiac modifier events, and Lending user balances. Project convention documents this fix (`feedback_clickhouse_left_join_nulls.md`) but enforcement is entirely manual.

**Fix pattern:** Add `pre_hook=["SET join_use_nulls = 1"]` and `post_hook=["SET join_use_nulls = 0"]` to every model with a LEFT JOIN expecting NULL on unmatched rows. Consider setting `join_use_nulls=1` globally in the ClickHouse connection profile if no model relies on default-value behavior.

### Pattern 2: Semantic layer column drift at enterprise scale
Semantic YAML files in `semantic/authoring/` are not auto-regenerated from dbt catalog. After model refactors, column references in semantic_models.yml go stale silently. Confirmed broken measures in: ESG (2 column names), Bridges (column `d`), Circles (3 semantic models), CoW (`week/pair/volume_usd/num_trades`), GBCDeposit (Gwei vs GNO), Gnosis App, GPay, Lending, Yields (`apy_7DMA`/`apy_30DMA`), Tokens (`balances_daily` exposes raw source columns). Approved-tier metrics that fail at query time undermine the semantic layer's credibility.

**Fix pattern:** Add a CI step that materializes the catalog after each run and validates every semantic model's `entities`, `dimensions`, and `measures` column names against the dbt catalog. Block promotion from `candidate` to `approved` without this check passing.

### Pattern 3: dev tags bypassing production CI
Models tagged `['dev',...]` are excluded from production CI runs and selective refreshes. Five confirmed cases where a production dependency chain passes through a dev-tagged model: `int_consensus_validators_labels` (blocks withdrawal address pipeline and `is_validator_depositor`), CoW api marts (14 models with `api_` prefix but `dev` tag), DAO Treasury (entire unit dev-tagged while referenced), Pools trades stats (13 models), Circles v1 (6 models all dev-tagged and 70+ days stale). The `check_api_tags.py` guard only validates `production`-tagged models, meaning `api_`-prefixed models can bypass the convention check by carrying `dev`.

**Fix pattern:** Add a CI check that any model with an `api_` prefix must carry either `production` or be explicitly listed in a documented pre-production allowlist. Remove `dev` tags from models that have live downstream dependencies.

### Pattern 4: ReplacingMergeTree without FINAL
RMT tables without `FINAL` on reads expose stale or duplicate rows. Confirmed consumer-facing impact in: Bridges (KPI views), Accounts (balance history), Pools (4 protocol daily models in prev_balances CTEs), Safe (8 duplicate safe_address rows fanning out in API responses), Transfers (bridges model), UBO (3 cumsum models), RWA (fct view), Revenue (weekly user views). The project's `insert_overwrite` strategy assumes atomic partition replacement, but background merge completion is not guaranteed before the next query.

**Fix pattern:** Add FINAL to all mart-level reads from RMT sources. For incremental prev_balances CTEs, apply FINAL consistently on both batch and regular paths.

### Pattern 5: Schema.yml phantom columns and fabricated documentation
14 sectors confirmed schema.yml entries that document columns not present in the model's final SELECT, or that are copied from an earlier model version or upstream source. This causes dbt tests to run against non-existent columns (silently passing), MCP schema registry to expose wrong contracts to API consumers, and Elementary schema-change tests to produce permanent false positives. Most severely affected: Contracts (all 9 BackedFi oracle models, Agave, circles calls), P2P (multiple models), Transfers (entire `whitelisted_daily` contract wrong), Safe (multiple), Tokens, Transactions, State.

**Fix pattern:** Run `dbt run-operation generate_model_yaml` after any model refactor and diff against existing schema.yml. Add a CI step that validates schema.yml column lists against the dbt catalog for all `production`-tagged models.

### Pattern 6: Silent freshness failures on critical sources
9 sectors have stale source data with no blocking CI failure: CoW fees (42 days stale), Consensus (6-day gap, all freshness tests at `severity: warn`), RWA bC3M (49 days stale, freshness test structurally blind due to forward-fill), State (132 days stale at source, tier-1 endpoint), Prediction Markets (309 days for OmenAgentResultMapping), Bridges (4 days stale). Warn-only thresholds allow multi-week stale data to reach production without any CI gate.

**Fix pattern:** Promote at least one freshness test per critical source to `severity: error`. Add `warn_after`/`error_after` to all sources backing Tier 0 and Tier 1 endpoints.

### Pattern 7: Partition cap approaching on multi-year tables
CH Cloud blocks inserts with code 252 when a single statement touches >100 partitions. Any full-rebuild of a table partitioned by `toStartOfMonth` over more than 8 years will fail. Already confirmed active or imminent: ESG carbon intensity (`toStartOfYear` mitigation applied but data lost), Crawlers data labels (94 months), State (88 months), P2P (multi-year).

**Fix pattern:** Add a CI check that any `partition_by` using `toStartOfMonth` triggers a warning if the table's `start_date` is more than 80 months before `today()`. Proactively repartition State, Crawlers labels, and any other tables within 12 months of the cap.

### Pattern 8: Candidate-only semantic tier on public endpoints
9 sectors serve Tier 0 or Tier 1 public REST API endpoints backed exclusively by `quality_tier: candidate` semantic metrics. This includes: ESG (15 models, Tier 0), Bridges (14 KPI metrics), Blocks (Tier 0 gas endpoints), Probelab (5 metrics, Tier 1), Tokens (all), Transactions (only 4 of 70+ approved), Revenue (gnosis_app stream missing entirely).

**Fix pattern:** Block MCP exposure and cerebro-api routing of `candidate`-tier metrics for any endpoint tagged `tier0` or `tier1`. Add a CI check that `tier0`/`tier1`-tagged API models have at least one `approved`-tier semantic measure.

---

## 5. High Findings Table

| Sector | Finding | Severity |
|---|---|---|
| ESG | 159/1,642 rows `carbon_intensity = 0.0` from missing `join_use_nulls` | HIGH |
| ESG | `api_esg_energy_consumption_annualised_latest` missing current-period exclusion filter | HIGH |
| Bridges | `net_usd`/`netflow_usd_week`/`value` typed `UInt64` but are Float64 with negative values | HIGH |
| Bridges | 4-day data stale; no freshness alerting on `dune_bridge_flows` | HIGH |
| Consensus | 6,712 active 0x02 validators excluded from `user_pseudonym` cross-sector join | HIGH |
| Consensus | 4-day lag; 6-day snapshot gap 2026-06-01 to 2026-06-06 | HIGH |
| Consensus | Dashboard "Staked GNO" shows 334k instead of 10.7M (32x discrepancy from `/32`) | HIGH |
| Contracts AMM-DEX | All 4 `_live` tables return 0 rows (scheduler gap exceeded TTL) | HIGH |
| Contracts AMM-DEX | BalancerV2 excluded from all fee_apr/TVL analytics | HIGH |
| Contracts Circles | Registry 4 duplicate address rows — fan-out risk for ~129 downstream models | HIGH |
| Contracts Lending | `contracts_backedfi_bC3M_Oracle_events` 49 days stale | HIGH |
| Crawlers Data | `int_crawlers_data_labels` at 94-month span — 6 months from CH Cloud 100-partition hard block | HIGH |
| Crawlers Data | `stg_crawlers_data__dune_bridge_flows_v2` references non-existent columns | HIGH |
| Crawlers Data | `dune_labels` freshness thresholds (18h warn/30h error) misconfigured for weekly cadence | HIGH |
| Execution Accounts | `api_execution_account_balance_history_daily` reads RMT without FINAL | HIGH |
| Execution Accounts | `fct_execution_account_token_balances_latest` silently empties on >14-day upstream gap | HIGH |
| Execution Circles v2 | `api_execution_circles_v2_wrapper_share_daily` sawtooth: 0-supply on 57 of 566 days | HIGH |
| Execution Circles v2 | V1 Circles stack 70+ days stale; all 6 v1 models dev-tagged | HIGH |
| Execution CoW | No source freshness test despite `loaded_at_field` defined | HIGH |
| Execution CoW | All 14 api_* marts missing `production` tag | HIGH |
| Execution GBCDeposit | Both `contracts_GBCDeposit` schema.yml entries describe fabricated columns | HIGH |
| Execution Gnosis App | Identity bridge silently drops 828 users (22,644 vs 21,816) | HIGH |
| Execution Gnosis App | `api_execution_gnosis_app_kpi_retention_pct_latest` returns 0.0 instead of NULL | HIGH |
| Execution GPay | `coalesce(p.price, 0)` without `join_use_nulls` — 22,549 rows have `amount_usd = 0` | HIGH |
| Execution GPay | `churn_rate` uses current-month denominator; `retention_rate` uses prior-month — non-complementary | HIGH |
| Execution Lending | Negative `cumulative_scaled_borrow` — likely `RepayWithATokens` uses wrong address field | HIGH |
| Execution Lending | `lenders_count_7d` is stock; `borrowers_count_7d` is 7-day flow — sold as equivalent | HIGH |
| Execution Lending | `balance_usd = 0` for 51-100% of SparkLend/Aave rows for GNO, sDAI, wstETH, USDC.e | HIGH |
| Execution Live | Balancer V3 staging: 80% empty token addresses | HIGH |
| Execution Live | 18-hour freshness lag vs 45-second design intent | HIGH |
| Execution Live | No CoW Protocol live coverage — dominant Gnosis Chain DEX absent | HIGH |
| Execution MMM | ~Half declared KPI/media universe permanently empty (5/13 KPIs, 5/8 media) | HIGH |
| Execution Pools | Balancer V3 negative TVL (-$2.1M/day, 28% of V3 rows) | HIGH |
| Execution Pools | LVR sign contract violated — 12% of rows positive vs schema claim "always <= 0" | HIGH |
| Execution Pools | 13 api_execution_trades_stats_* models carry `dev` tag | HIGH |
| Execution Pools | Balancer V2 absent from fees_daily — 93% of pool-rows have zero volume/fees | HIGH |
| Execution Prices | No forward-fill staleness cap — structural defect for all DEX-priced tokens | HIGH |
| Execution Prices | Hub missing `unique_combination_of_columns` test on `(date, symbol)` | HIGH |
| Execution RWA | bC3M 49 days stale; freshness test structurally blind | HIGH |
| Execution RWA | `contracts/backedfi/schema.yml` fabricated columns for all 9 oracle models | HIGH |
| Execution Safe | v1.4.1 AddedOwner/RemovedOwner `indexed:false` — ~107k events silently NULL | HIGH |
| Execution Safe | 8 duplicate Safe addresses served without FINAL | HIGH |
| Execution Safe | 16.7% of owner rows have NULL `current_threshold` | HIGH |
| Execution Shared | `is_safe_owner` returns 1 for GPay sentinel 0x...0002 | HIGH |
| Execution State | Pipeline 132 days stale at source (cryo-indexer) | HIGH |
| Execution Tokens | `supply_usd` can go negative; no test guard prevents negative values shipping | HIGH |
| Execution Tokens | Semantic model for `int_execution_tokens_balances_daily` exposes entirely wrong columns | HIGH |
| Execution Transactions | `gas_price_avg/median` CAST to Int32 truncates sub-Gwei type-4 transactions to 0 | HIGH |
| Execution Transactions | `unique_addresses` carries ~209k unmerged duplicate rows | HIGH |
| Execution Transfers | `whitelisted_daily` schema.yml documents 5 phantom columns; omits actual `amount_raw` | HIGH |
| Execution Transfers | `reinterpretAsInt256` used for `uint256` ERC-20 values — latent negative-volume risk | HIGH |
| Execution UBO | 3 cumsum models read `{{ this }}` without FINAL on regular path | HIGH |
| Execution Yields | Approved `yields_sdai_apy_7dma_value`/`yields_sdai_apy_30dma_value` reference nonexistent columns | HIGH |
| Execution Yields | Balancer V2 profit-as-fee proxy: ~$35.9M of PnL labeled as fee income | HIGH |
| Execution Yields | 7 user API marts expose plaintext wallet addresses with no privacy tier tag | HIGH |
| Mixpanel GA | `unique_devices` approved semantic metric is sum-of-per-event-type counts (methodology wrong) | HIGH |
| P2P | discv4 `pct_successful` always 100% — metric is vacuous | HIGH |
| P2P | 53% of discv4 peer rows have empty-string geo from missing `join_use_nulls` | HIGH |
| P2P | Topology map drops 65% discv4 edges and 54% discv5 edges | HIGH |
| P2P | cerebro-docs documents 4 model names that have never existed | HIGH |
| Probelab | 5 api_* marts missing `api:` and `granularity:` tags | HIGH |
| Quarterly Data | No `is_complete` flag on any quarterly row | HIGH |
| Quarterly Data | Zero dbt data tests across all 8 quarterly schema.yml files | HIGH |
| Revenue | Monthly pipeline ~75% coverage gap (Jan-Sep missing for 3 years) | HIGH |
| Revenue | GPay settlement address may not match post-April 2025 Spender router | HIGH |

---

## 6. Semantic Layer Health

**Summary:** The semantic layer is functioning as a discovery and routing surface but is not trustworthy as an analytical source. The combination of candidate-tier prevalence, broken column references, and non-standard aggregation methods means the MCP `query_metrics` and `quick_metric_chart` paths return incorrect or failed results for a substantial fraction of use cases.

**Confirmed broken approved-tier metrics (will fail or return wrong values at query time):**
- `network_weighted_cif_value` and `total_estimated_nodes_value` (ESG) — nonexistent columns
- `GBCDeposit_deposists_daily__amount_value` (GBCDeposit) — sums Gwei as GNO
- `cow_top_pairs_volume` (CoW) — `execution_cow_top_pairs_weekly` column mismatch
- `yields_sdai_apy_7dma_value` and `yields_sdai_apy_30dma_value` (Yields) — nonexistent wide columns
- `bridges_kpis_snapshot` all 14 auto-generated KPI metrics (Bridges) — `agg_time_dimension` column `d` nonexistent

**Candidate-tier metrics serving Tier 0/1 endpoints:**
ESG (15 models), Bridges (14 KPI metrics), Blocks (gas usage monthly), Probelab (5 metrics), Tokens (all), P2P (20 models). These carry explicit "review before relying" notes but are exposed to MCP without a quality gate.

**Aggregation method errors:**
- `fraq_value` (Blocks): `agg: sum` over pre-computed ratio — MetricFlow produces sum-of-ratios
- `cumulative_accounts` (Mixpanel GA acquisition): `agg: sum` on running total — multi-week queries multiply totals
- State metrics (State): `agg: sum` on cumulative `bytes` column — meaningless cross-row sum
- `execution_token_prices_daily__price_value` (Prices): averages price without symbol filter

**Coverage gaps:**
- DAO Treasury: zero semantic coverage
- Accounts: zero semantic coverage
- Quarterly Data: zero semantic coverage despite being the primary quarterly KPI surface for executive reporting
- Revenue gnosis_app stream: marts and API endpoints exist but no semantic model
- MMM: intentionally SQL-only (documented)

---

## 7. Data Freshness and Pipeline Health

| Sector | Staleness | Root Cause | Blocking Alert? |
|---|---|---|---|
| Execution State | 132 days (since 2026-01-30) | cryo-indexer `storage_diffs` feed down | None — tier-1 endpoint |
| Contracts Prediction Markets | 309 days (OmenAgentResultMapping) | `dbt_incremental_runner.py` 30-slice cap exits 0 | None |
| Contracts Prediction Markets | 71 days (FPMMDeterministicFactory_calls) | Same 30-slice cap | None |
| Execution CoW | 42 days (cow_api_trade_fees) | Source feed stalled | No source freshness test |
| RWA bC3M | 49 days | Oracle contract possibly migrated/delisted | Freshness test blind (forward-fill) |
| Contracts BackedFi bC3M events | 49 days | Root cause unknown | None |
| Consensus | 4-day data lag; 6-day snapshot gap | Pipeline cadence | All tests `severity: warn` |
| Bridges | 4-day data lag | Pipeline cadence | No warn_after/error_after |
| Execution Circles v1 | 70+ days | All v1 models dev-tagged | No test |
| Execution Live | 18-hour lag | Scheduler gap exceeded 2-hour TTL | None |
| Shared time spines | 55 days (since ~2026-04-17) | Not included in any pipeline run | None |
| Crawlers data `dune_labels` | Weekly cadence but 18h/30h alert thresholds | Misconfigured thresholds | Permanent false alert |

**`dbt_incremental_runner.py` silent overflow:** The `max_slices_per_stage=30` cap silently drops models when the date range exceeds 30 slices, exits 0, and emits no error. This is the confirmed root cause of the 309-day and 71-day staleness in the prediction markets sector. Any sector using this runner with a backlog exceeding 30 days is at risk of silent data loss.

---

## 8. CI and Test Coverage Gaps

**check_api_tags.py bypass methods confirmed:**
1. Use `dev` tag instead of `production` — guard only validates `production`-tagged models (Pools trades, CoW api marts, DAO Treasury, Circles v1)
2. Add model to `check_api_tags.allow` — entire revenue module (28 models) globally allowlisted
3. Omit `api:` tag entirely — guard only activates on models that already carry `api:` (Probelab 5 marts)
4. Use `multi_api` rule — allows multiple api: resource names per node

**no_delete_insert.py bypass:**
Revenue `int_revenue_fees_weekly_per_user` is on the allowlist as "acknowledged migration debt" but remains in production with the banned strategy.

**Missing uniqueness tests on high-impact models:**
- `int_execution_token_prices_daily` (hub for all USD valuations) — no grain test
- `int_execution_transfers_whitelisted_daily` (9 downstream models) — only on recent window
- `fct_ubo_supply_claims_resolved_daily` — no uniqueness test
- All quarterly_data schema.yml files — zero data tests
- `int_revenue_fees_weekly_per_user` — no tests at all

**Elementary test coverage gaps:** Freshness anomaly tests on mart views are structurally blind when forward-fill logic guarantees rows regardless of source freshness (RWA, ESG). Most freshness anomaly tests are deployed only on a subset of models without standardized rollout.

**Semantic layer CI:** No CI step validates semantic YAML column references against the dbt catalog. Broken approved-tier metrics silently ship to production.

---

## 9. Priority Fix List

### P0 — Fix immediately (data actively wrong in production now)

| # | Action | Sector | File(s) |
|---|---|---|---|
| 1 | Change `int_esg_carbon_intensity_ensemble` partition from `toStartOfYear` to `toStartOfMonth` and full-refresh; validate `fct_esg_carbon_footprint_uncertainty` has >31 rows | ESG | `models/ESG/intermediate/int_esg_carbon_intensity_ensemble.sql` |
| 2 | Update ESG semantic YAML: rename `network_weighted_cif` to `effective_carbon_intensity`, `total_estimated_nodes` to `estimated_nodes` | ESG | `semantic/authoring/ESG/semantic_models.yml` |
| 3 | Fix `apy_30d` formula in validator explorer marts; backfill | Consensus | `models/consensus/marts/fct_consensus_validators_explorer_latest.sql`, `fct_consensus_validators_explorer_members_table.sql` |
| 4 | Re-tag `int_consensus_validators_labels` from `dev` to `production`; confirm `is_validator_depositor` survives production-only builds | Consensus | `models/consensus/intermediate/int_consensus_validators_labels.sql` |
| 5 | Fix `int_execution_circles_v1_transfers` SQL to emit 13 columns matching warehouse schema | Circles v2 | `models/execution/Circles/intermediate/int_execution_circles_v1_transfers.sql` |
| 6 | Fix 5 circles calls models: replace `execution.transactions` with `execution.traces`; full-refresh | Contracts Circles | `models/contracts/circles/*_calls.sql` (5 files) |
| 7 | Load Agave ABI into `event_signatures`; rebuild `contracts_agave_LendingPool_events` | Contracts Lending | `seeds/event_signatures.csv` |
| 8 | Fix `bytes_diff` formula to distinguish new allocations from overwrites; full-refresh and validate corrected cumulative | Execution State | `models/execution/state/intermediate/int_execution_state_size_full_diff_daily.sql` |
| 9 | Investigate and restore `storage_diffs` cryo-indexer feed (132 days stale) | Execution State | Source infra |
| 10 | Add `join_use_nulls=1` pre_hook/post_hook to `int_execution_bridges_address_flows_daily`; full-rebuild all 6.16M rows | Execution Transfers | `models/execution/transfers/intermediate/int_execution_bridges_address_flows_daily.sql` |
| 11 | Fix `int_execution_gnosis_app_user_activity_daily` to not use persisted `start_month`/`end_month` vars; full-rebuild | Execution Gnosis App | `models/execution/gnosis_app/intermediate/int_execution_gnosis_app_user_activity_daily.sql` |
| 12 | Fix `fee_amount = 0` universal for Gnosis App swap fills; root-cause investigation | Execution Gnosis App | `models/execution/gnosis_app/intermediate/` |
| 13 | Change cashback schema.yml and SQL to label cashback units as GNO not USD in `api_execution_gpay_user_total_cashback` and `api_execution_gpay_user_cashback_daily` | Execution GPay | `models/execution/gpay/marts/api_execution_gpay_user_total_cashback.sql`, `api_execution_gpay_user_cashback_daily.sql`, `schema.yml` |
| 14 | Fix `int_execution_gpay_activity_daily` RMT `order_by` to include `direction`; full-rebuild all downstream marts | Execution GPay | `models/execution/gpay/intermediate/int_execution_gpay_activity_daily.sql` |
| 15 | Fix WxDAI utilization Int256 underflow: investigate `RepayWithATokens` address field; rebuild lending pipeline | Execution Lending | `models/execution/lending/intermediate/` |
| 16 | Cap SAFE forward-fill to N days (7-30) in `int_execution_prices_native_daily` so Dune fallback activates; validate SAFE returns ~$0.117 | Execution Prices | `models/execution/prices/intermediate/int_execution_prices_native_daily.sql`, `int_execution_token_prices_daily.sql` |
| 17 | Fix `fct_execution_yields_user_lifetime_metrics`: wrap `least()` args with `coalesce`; full-refresh to also clear `active_lending_positions=0` | Execution Yields | `models/execution/yields/marts/fct_execution_yields_user_lifetime_metrics.sql` |
| 18 | Fix `fct_execution_yields_user_lending_positions_latest` latest-date selection to skip zero-balance partial days; verify `sum(is_lending_user) > 0` | Execution Shared/Yields | `models/execution/yields/marts/fct_execution_yields_user_lending_positions_latest.sql` |
| 19 | Add `join_use_nulls=1` pre_hook/post_hook to `fct_mixpanel_ga_gnosis_app_users` and `fct_mixpanel_ga_gpay_crossdomain_daily`; full-rebuild both | Mixpanel GA | `models/mixpanel_ga/marts/fct_mixpanel_ga_gnosis_app_users.sql`, `fct_mixpanel_ga_gpay_crossdomain_daily.sql` |
| 20 | Fix GBCDeposit semantic YAML: add `0x01` type guard on entity address; correct `amount` Gwei-to-GNO conversion | GBCDeposit | `semantic/authoring/execution/gbcdeposit/semantic_models.yml` |

### P1 — Fix within 1 sprint (high impact, not immediately catastrophic)

| # | Action | Sector |
|---|---|---|
| 21 | Fix 4 seed rows in `seeds/event_signatures.csv`: set `indexed:true` on owner for AddedOwner/RemovedOwner on v1.4.1 and v1.4.1L2 Safe singletons | Safe |
| 22 | Add FINAL to `int_execution_safes` reads in `api_execution_safe_details_latest` and `fct_execution_account_safes_latest` | Safe |
| 23 | Fix `int_execution_tokens_supply_holders_daily`: add `balance > 0` guard; add `not_negative` test | Tokens |
| 24 | Fix `gas_price_avg/median` CAST to Int32 to Float32 in `int_execution_transactions_info_daily`; backfill | Transactions |
| 25 | Run monthly revenue backfill for Jan-Sep 2023/2024/2025 and Jan-Mar 2026 | Revenue |
| 26 | Investigate GPay settlement address vs post-April 2025 Spender router architecture | Revenue |
| 27 | Add FINAL to `contracts_safe_registry` reads and `contracts_zodiac_modules_registry` subquery | Safe/Zodiac |
| 28 | Triage bC3M oracle: determine if migrated/delisted; recover via backfill or suppress stale price | RWA |
| 29 | Add per-ticker freshness test on `int_execution_rwa_backedfi_prices` (not on forward-filled mart) | RWA |
| 30 | Fix Balancer V3 negative TVL: reconcile ERC4626 wrapper sign conventions; add `not_negative` test | Pools |

### P2 — Fix within current quarter

| # | Action | Sector |
|---|---|---|
| 31 | Add `join_use_nulls` hooks to `int_p2p_discv4_peers` and both topology intermediates | P2P |
| 32 | Fix discv4 `pct_successful`: suppress or correct at source (always 100% metric) | P2P |
| 33 | Repartition `int_execution_state_size_full_diff_daily` to `toStartOfYear`; document months of headroom | State |
| 34 | Repartition `int_crawlers_data_labels` to `toStartOfYear`; add partition-count CI check | Crawlers |
| 35 | Remove `dev` tags from all 13 `api_execution_trades_stats_*` models; add required api tags | Pools |
| 36 | Add `is_complete` boolean column to all 20 quarterly mart models | Quarterly Data |
| 37 | Add `dbt_utils.unique_combination_of_columns` to `int_execution_token_prices_daily` hub | Prices |
| 38 | Fix `fct_execution_yields_user_activity` grain: add `token_address` to ORDER BY key | Yields |
| 39 | Fix approved semantic measures for Yields sDAI: repoint `yields_sdai_apy_7dma_value`/`yields_sdai_apy_30dma_value` to label-filtered rows | Yields |
| 40 | Run `dbt run --select shared` immediately to restore 55-day time spine shortfall; add to daily pipeline | Shared |

---

## 10. Architectural Strengths

Despite the breadth of issues documented above, the review identified genuine architectural strengths that should be preserved.

**Incremental strategy design:** The `insert_overwrite` + monthly partition pattern, combined with the `apply_monthly_incremental_filter` macro and `start_month`/`end_month` backfill variables, is a well-engineered solution to ClickHouse's atomicity constraints. The pattern is applied consistently across most sectors and correctly handles late-arriving records.

**UBO attribution layer:** The 5-protocol Ultimate Beneficial Owner unwinding pipeline (Aave V3, SparkLend, Balancer V2, Uniswap V3, Swapr V3, Curve, sDAI) is architecturally sound. The coverage diagnostic (`pct_direct_terminal` / `pct_unwound_terminal` / `pct_known_container` / `pct_unclassified`) is an excellent self-describing data quality signal. Second-level container resolution verified correct with zero containers leaking through.

**Privacy architecture in Mixpanel GA:** The two-tier exclusion pattern (`dbt_project.yml` blanket ban + per-model `expose_to_mcp: false`), salted `sipHash64` pseudonymization applied at ingestion, and k-anonymity floor of 5 on campaign metrics constitute a coherent, documented privacy boundary. This pattern should be extended to the 7 Yields user API marts currently lacking it.

**Cross-sector pseudonym bridge:** The `sipHash64(lowercased_address, pii_salt)` hash space is consistently applied across Safe, GPay, Gnosis App, Mixpanel, and Revenue sectors with a documented compatible pseudonym contract, enabling verified cross-sector joins without raw address exposure.

**MMM three-persona pipeline:** The `mmm_analyst / mmm_causal_reviewer / mmm_simulator` MCP persona chain with structural causal-review enforcement before attribution is published is a sound econometric governance framework. The 730-day trailing ISO-weekly spine with correct missing-week semantics (0 for sum methods, NULL for last/avg), adstock toolkit (lambda=0.5 geometric decay, Hill S-curve), and simulator guardrails (±30%/period cap, no zero-out, no extrapolation beyond 1.5x historical max) represent a principled analytical design.

**Safe catalog centrality:** The `int_execution_safes` pipeline correctly enumerates all Safe proxy versions (v0.1.0 through v1.4.1L2) via delegatecall + singleton seed + setup-selector, providing a single authoritative source that gpay, gnosis_app, zodiac, and accounts all depend on. The backfill orchestration correctly stays under the CH Cloud partition cap.

**Event-delta accumulation design:** The pools sector's approach to computing TVL via event-delta accumulation rather than `balanceOf` snapshots, with consistent two's-complement int256 decoding and ASOF daily price joins, is architecturally correct and uniform across four AMM protocols.

**Quarterly data API convention:** All 20 quarterly endpoints consistently apply the `api:/granularity:quarterly/tier:0` tag convention, providing reliable REST API routing. The `argMax`-based end-of-quarter snapshot pattern avoids FINAL overhead on RMT sources.

**Token price hub architecture:** The four-tier priority design (native Chainlink > BackedFi RWA > Dune > $1 hardcoded peg) with per-sub-layer grain-uniqueness tests and the hub serving as the single `(date, symbol)` JOIN contract for all ~26 downstream consumers is a sound architecture. The Chainlink oracle migration is hub-internal with no downstream model changes required at cutover.

---

*Report generated from 37 sector review files in `/Users/hugser/Documents/Gnosis/repos/dbt-cerebro/docs/model_review/`.*
