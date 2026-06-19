# Model review: execution/lending

**Convergence:** converged in 1 round — inspector and context reports are mutually consistent; all critical findings are independently corroborated with no material contradictions.

---

## Scope and inventory

The unit covers DeFi lending activity on Gnosis Chain for **Aave V3** (live 2023-10-04) and **SparkLend** (live 2023-10-06). Despite the unit title listing "Aave/Agave/Spark", Agave (Aave V2 fork, live 2022-04-19) is intentionally absent from all processing models. The pipeline tracks 15 (protocol, reserve) pairs across 8 reserve tokens with supply, borrow, withdrawal, repayment, and liquidation events.

| Layer | Count | Notes |
|---|---|---|
| Intermediates | 5 | diffs, user balances, utilization, daily aggregates, balance cohorts |
| Fact tables (fct_) | 4 | latest, weekly, top_lenders_ranked, top_lenders_latest |
| API marts (api_) | 10 | APY daily, TVL, counts, volumes, cohorts, top lenders |
| Semantic models | 2 | APY daily + weekly lending |
| Metrics exposed | 6 | deposits/borrows weekly volume, APY daily/weekly (2 duplicates flagged) |
| Seeds | 2 | `lending_market_mapping.csv`, `atoken_reserve_mapping.csv` |

---

## Business context

The unit answers four consumer categories: (1) rate monitoring — supply APY, variable borrow APY, spread, and utilization per reserve/protocol; (2) user-count KPIs — active lenders and borrowers; (3) TVL and concentration — per-token TVL, top lenders, balance-size cohorts; (4) weekly trend series — deposit/borrow volumes and APYs.

**Canonical definitions:**

- **Supply APY** (`apy_daily`): `(1 + liquidityRate_RAY / 1e27 / 31536000)^31536000 - 1`, forward-filled on no-event days.
- **Variable Borrow APY** (`borrow_apy_variable_daily`): same formula applied to `variableBorrowRate_RAY`.
- **Utilization Rate**: `(cumulative_scaled_borrow * variableBorrowIndex_eod) / (cumulative_scaled_supply * liquidityIndex_eod) * 100`, computed from exact Int256 WadRayMath running sums ASOF-joined to per-transaction ReserveDataUpdated events.
- **Active Lenders** (intended: STOCK): wallets with `balance > 0` in `int_execution_lending_aave_user_balances_daily` on the latest date.
- **Active Borrowers** (intended: STOCK per schema description, actual: FLOW): bitmap-merged wallets that borrowed within the last 7 days, sourced from `fct_execution_lending_latest`.
- **Scaled Balance**: exact Int256 RAY-scaled aToken balance using `rayDivFloor` for inflows and `rayDivCeil` for outflows, mirroring Aave's on-chain WadRayMath.
- **Balance Cohorts**: 10 bands (0–0.01 through 1M+) applied in both USD and native token units.
- **Week**: ISO week start (Monday UTC) via `toStartOfWeek(date, 1)`; current incomplete week excluded.

**Contract context:** Aave V3 Pool `0xb50201558B00496A145fE76f7424749556E326D8`; SparkLend Pool `0x2Dae5307c5E3FD1CF5A72Cb6F698f915860607e0`. SparkLend emits `ReserveDataUpdated` on every `FlashLoan` (54,223+ FlashLoan-emitted RDUs for WETH alone), making rank-based RDU pairing incorrect; all utilization and diffs models correctly use `ASOF INNER JOIN on log_index < action log_index`. All contract addresses, aTokens, and reserve mappings verified against seeds and the docs site.

---

## Implementation assessment

### CRITICAL

**Int256 underflow corrupts WxDAI utilization rate — live in production**
`models/execution/lending/intermediate/int_execution_lending_aave_utilization_daily.sql`

`cumulative_scaled_borrow` for Aave V3 WxDAI (`0xe91d153...`) is consistently negative (e.g., `-975056198952215439362630`). The CASE guard only checks `c.cumulative_scaled_supply > toInt256(0)`, so `toUInt256(negative_Int256)` wraps to `~2^256` and yields `utilization_rate ~4.4–4.5e27` instead of ~42%. Confirmed on all 14 recent WxDAI rows (15 total rows with negative borrow across the table). These values are currently live and propagate to `fct_execution_yields_opportunities_latest` and any report reading utilization. Fix requires two steps: (a) add `AND c.cumulative_scaled_borrow >= toInt256(0)` to the CASE guard as an immediate symptom mask; (b) investigate the root cause — the negative running sum in the borrow delta accounting (see next finding).

---

### HIGH

**Negative cumulative_scaled_borrow: delta-accounting drift with unresolved root cause**
`models/execution/lending/intermediate/int_execution_lending_aave_diffs_daily.sql`, `int_execution_lending_aave_utilization_daily.sql`

15 rows (all recent Aave V3 WxDAI) show `cumulative_scaled_borrow < 0`, meaning the borrow-side running sum has drifted below zero. The likely candidate is that `RepayWithATokens` events in `int_execution_lending_aave_diffs_daily` use `decoded_params['repayer']` (the `msg.sender`) rather than `decoded_params['user']` (the debt-position holder) for the aToken delta. When a third party repays on behalf of another (repayer != user), the deduction is credited to the wrong address, corrupting the running balance. A full-refresh would reset the symptom without fixing the cause. Needs reconciliation against on-chain variable-debt `totalSupply`.

**Lenders KPI (balance STOCK) vs borrowers KPI (7-day event FLOW) sold as equivalent `*_count_7d`**
`models/execution/lending/marts/api_execution_lending_lenders_count_7d.sql`, `api_execution_lending_borrowers_count_7d.sql`, `fct_execution_lending_latest.sql`

`api_execution_lending_lenders_count_7d` counts wallets with `balance > 0` on the latest date (point-in-time STOCK from `user_balances_daily`, tagged `granularity:latest`). `api_execution_lending_borrowers_count_7d` counts wallets that borrowed in the last 7 days (bitmap FLOW from `fct_execution_lending_latest`, tagged `granularity:last_7d`). Both schema descriptions claim "positive balance". An external consumer comparing "lenders vs borrowers" side-by-side is misled into a category error. Note that `user_balances_daily` tracks supply positions only (no debt balances), so a true borrower STOCK measure may not currently be available without additional work.

**balance_usd=0 on positive native balance due to price gap**
`models/execution/lending/intermediate/int_execution_lending_aave_user_balances_daily.sql`

The LEFT JOIN to `int_execution_token_prices_daily` coalesces missing price to 0. On 2026-06-07 (the latest date in the pipeline): 100% of SparkLend GNO rows, 96% SparkLend sDAI, 60% SparkLend wstETH, 51% Aave V3 GNO, and 57% Aave V3 USDC.e show `balance_usd=0` with positive native balance. TVL and USD-denominated cohort data are severely understated for these assets. Likely causes are price-feed staleness and/or a symbol-matching failure (e.g., `USDC.e` vs `USDC`). There is no guard, alert, or test that surfaces this condition.

**Agave silently excluded despite "Aave/Agave/Spark" scope and first-class docs**
`models/execution/lending/intermediate/int_execution_lending_aave_daily.sql`, `api_execution_lending_tvl_by_token_latest.sql`

The unit title, the public docs site, and `protocols/index.html.md` all treat Agave as a first-class Gnosis lending protocol (live 2022-04-19, decoded source model `contracts_agave_LendingPool_events` present). No Agave event is processed by any intermediate or mart in this unit. Any MCP or dashboard query expecting "all Gnosis lending TVL/users" silently undercounts. Agave uses `Deposit` instead of `Supply` as the event name, requiring a mapping layer — this is likely an intentional deferral, but it is not documented in the unit.

---

### MEDIUM

**`is_incremental()` / `lka`-JOIN branches are unreachable dead code**
`models/execution/lending/intermediate/int_execution_lending_aave_daily.sql`

`int_execution_lending_aave_daily` is `materialized='table'`, so `is_incremental()` is always `False` at compile time. Approximately 50 lines — the `lka` LEFT JOIN, incremental filter branches, and `COALESCE(f.apy_daily, lka.last_apy)` forward-fill logic — never execute. The model rebuilds from scratch every run. The dead branches create a false impression of incremental safety and add maintenance burden.

**Data 4 days stale; SparkLend returns 0 active users without documentation**
`models/execution/lending/intermediate/int_execution_lending_aave_daily.sql`, `fct_execution_lending_latest.sql`

`int_execution_lending_aave_daily` max date is 2026-06-07 (4 days behind today), propagating to all downstream marts. `fct_execution_lending_latest` reports SparkLend lenders=0, borrowers=0 vs Aave V3 lenders=354, borrowers=42. Both may be benign (refresh scheduler lag; SparkLend wind-down on Gnosis Chain), but neither is documented or covered by a freshness test. Silent zeros for a mapped protocol will be read as real data by consumers.

**`toUInt64(lenders_bitmap_state)` on merged AggregateFunction in fct_weekly (non-idiomatic)**
`models/execution/lending/marts/fct_execution_lending_weekly.sql`

`fct_execution_lending_weekly` calls `toUInt64(lenders_bitmap_state)` directly on the `AggregateFunction` result of `groupBitmapMerge()`, separating the merge and the cardinality extraction. `bitmapCardinality()` is the documented ClickHouse function for this. `fct_execution_lending_latest` correctly wraps `toUInt64(groupBitmapMerge(...))` in one expression. Works on current ClickHouse via implicit cast but is fragile across versions.

**`api_execution_lending_tvl_by_token_latest` schema.yml missing `protocol` column; description wrong**
`models/execution/lending/marts/api_execution_lending_tvl_by_token_latest.sql`, `schema.yml`

The SQL outputs four columns (`protocol`, `token`, `value`, `as_of_date`) but `schema.yml` documents only three (`token`, `value`, `as_of_date`). The `protocol` column is undocumented and untyped. The schema description incorrectly states the data is "aggregated across protocols" when it outputs per-protocol rows.

**Treasury `mintToTreasury` deltas inflate user balances, TVL, and top-lender rankings**
`models/execution/lending/intermediate/int_execution_lending_aave_diffs_daily.sql`, `fct_execution_lending_top_lenders_ranked.sql`, `api_execution_lending_tvl_by_token_latest.sql`

`int_execution_lending_aave_diffs_daily` adds a `treasury_mint_deltas` CTE capturing aToken Mint events where `caller = Pool` (protocol treasury minting), using half-up rounding (`intDiv(...+intDiv(index,2),index)`) rather than standard `rayDiv`. The treasury address therefore accumulates a position in `user_balances_daily` and can dominate top-lender rankings for high-fee reserves. A label LEFT JOIN exists but no explicit filter or exclusion is applied.

---

### LOW

**Top-lenders stack tagged `dev` — excluded from CI and production runs**
`models/execution/lending/marts/fct_execution_lending_top_lenders_ranked.sql`, `fct_execution_lending_top_lenders_latest.sql`, `api_execution_lending_top_lenders_latest.sql`

All three carry `tags=['dev',...]`, excluding them from the production CI api-tag guard and scheduled refresh. `api_execution_lending_top_lenders_latest` has a `tier1` + `granularity:latest` tag and an `elementary.schema_changes` test, indicating promotion intent. The decision to promote or formally keep internal should be made explicitly.

**Integer Date arithmetic, duplicate tags, and weekly `week->date` allowlist**
`fct_execution_lending_top_lenders_latest.sql`, `fct_execution_lending_weekly.sql`, `fct_execution_lending_latest.sql`, `api_execution_lending_activity_counts_weekly.sql`, `api_execution_lending_activity_volumes_weekly.sql`

`fct_execution_lending_top_lenders_latest` uses `max(date) - 7` (Date integer arithmetic) where `subtractDays(max(date), 7)` is the project-consistent idiom (used in `api_execution_lending_lenders_count_7d`). `fct_execution_lending_latest` and `fct_execution_lending_weekly` both carry malformed duplicate `'lending,lending'` tags. The two weekly api marts alias `week AS date` and are permanently allowlisted with `no_grain_col`, papering over the grain-column CI check.

**Near-zero test coverage on intermediates**
`int_execution_lending_aave_diffs_daily.sql`, `int_execution_lending_aave_user_balances_daily.sql`, `int_execution_lending_aave_utilization_daily.sql`

The Int256/UInt256 intermediates carry schema-change migration notes and append-strategy allowlists but have effectively no grain, duplicate, non-null, or range tests. There is no reconciliation of cumulative scaled balances against on-chain aToken/variable-debt `totalSupply`. The negative-borrow drift finding is exactly the class of defect that reconciliation tests would have caught before it reached production.

**Semantic-layer gaps and duplicate auto-generated metrics**
`fct_execution_lending_weekly.sql`, `api_execution_lending_daily.sql`

Utilization, TVL, lender/borrower counts, balance cohorts, and top lenders have no semantic model; MCP/cerebro queries for e.g. "current WETH utilization on SparkLend" must drop to raw SQL. Additionally, `execution_lending_apy_weekly_value` and `execution_lending_borrow_apy_weekly_value` are auto-generated candidates duplicating the curated `lending_apy_daily` at weekly grain — these should be reviewed and pruned or curated.

**sDAI potential double-count between SparkLend reserve and Savings xDAI module**
`int_execution_lending_aave_user_balances_daily.sql`, `api_execution_lending_tvl_by_token_latest.sql`

The docs note sDAI/sxDAI is listed as a SparkLend reserve. If a separate savings/sDAI unit also tracks the same token's TVL, cross-unit aggregation (dashboards, quarterly reporting) could double-count sDAI value. The scoping boundary is not documented.

---

## Data findings

Eight queries were run against production tables:

| Query | Result |
|---|---|
| `int_execution_lending_aave_daily` freshness/grain | max_date=2026-06-07 (-4d), 13,063 rows, 2 protocols, 9 tokens, 0 grain duplicates |
| Null APY rates | 0 null supply APY; 991 (Aave) + 1,746 (Spark) null borrow APY rows — expected for non-variable-borrow tokens |
| `int_execution_lending_aave_user_balances_daily` | 27.9M rows, max_date=2026-06-07, 4.1M rows with `balance_usd=0` |
| `int_execution_lending_aave_utilization_daily` sanity | 0 negative supply, **15 negative borrow**, 29 rows util>100, 0 null util |
| WxDAI negative borrow detail | All 14 recent Aave V3 WxDAI rows + 1 SparkLend WETH row (util=101.38) |
| WxDAI Int256 confirmed | Scaled borrow string value confirmed negative (~-975 trillion) |
| `balance_usd=0` breakdown on latest date | GNO, sDAI, wstETH, USDC.e worst affected (51-100% of rows) |
| `fct_execution_lending_latest` (token=ALL) | SparkLend lenders=0, borrowers=0; Aave V3 lenders=354, borrowers=42 |

---

## Pros / Cons

**Pros:**

- Sophisticated on-chain-faithful accounting: exact Int256 WadRayMath (`rayDivFloor`/`rayDivCeil` mirroring Aave), ASOF-join RDU pairing that correctly handles SparkLend's FlashLoan RDU event storm, and bitmap deduplication for active-user counts.
- Comprehensive surface area: 5 intermediates and 14 marts covering APY/spread, utilization, TVL, balance cohorts, top lenders, and weekly trends with semantic-layer metrics.
- Contract addresses, aTokens, and reserve mappings are fully verified against seeds and the docs site.
- `ASOF INNER JOIN on log_index` correctly guards against a real and well-documented SparkLend misalignment trap.
- Forward-fill of APY on no-event days is a defensible canonical definition, documented in `schema.yml` and the docs site.
- Token classification (STABLECOIN/OTHERS) and per-(protocol, reserve) grain support clean cross-protocol comparison.
- Most `api_` endpoints carry correct `api:/granularity:/tier` tags, and grain integrity on the core daily model was confirmed (zero duplicates).
- Schema-change migration notes and full-refresh requirements are explicitly documented on the Int256/UInt256 models.

**Cons:**

- A critical Int256 underflow is serving astronomically wrong WxDAI utilization (~4.5e27 vs ~42%) to production consumers right now.
- Two KPIs sold side-by-side as `*_count_7d` measure fundamentally different things: lenders = point-in-time balance STOCK, borrowers = 7-day event FLOW.
- Unit titled "Aave/Agave/Spark" but Agave is silently absent from the entire pipeline despite having a decoded source model and first-class docs.
- USD valuation collapses to 0 on price gaps (`coalesce(price,0)`), severely understating TVL/cohorts for GNO, sDAI, wstETH, USDC.e on recent dates with no alert.
- Treasury `mintToTreasury` deltas inflate user balances/TVL and can dominate top-lender rankings with no filter or label-based exclusion.
- Near-zero test coverage on intermediates; no reconciliation of cumulative scaled balances against on-chain aToken/variable-debt `totalSupply`.
- Dead `is_incremental()`/`lka`-JOIN branches in a table-materialized model create a false impression of incremental safety.
- Data is 4 days stale through all downstream marts, and SparkLend silently returns 0 lenders/borrowers without a documented cause.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 — immediate | Add `AND c.cumulative_scaled_borrow >= toInt256(0)` to the utilization CASE guard; verify `fct_execution_yields_opportunities_latest` no longer serves ~e27 values | `int_execution_lending_aave_utilization_daily.sql` |
| P0 — immediate | Investigate negative WxDAI running sum: validate `RepayWithATokens` 'repayer' vs 'user' attribution in `diffs_daily` against on-chain third-party repayments; add reconciliation test vs on-chain variable-debt `totalSupply` | `int_execution_lending_aave_diffs_daily.sql`, `int_execution_lending_aave_utilization_daily.sql` |
| P1 | Resolve lenders/borrowers KPI semantics: decide STOCK vs FLOW for each, align `granularity` tags, fix both `schema.yml` descriptions, and document the chosen definition on the dashboard | `api_execution_lending_lenders_count_7d.sql`, `api_execution_lending_borrowers_count_7d.sql` |
| P1 | Add a non-null/non-zero price guard or alert on the `int_execution_token_prices_daily` join; investigate and fix the GNO/sDAI/wstETH/USDC.e price gap (check symbol matching such as USDC.e vs USDC) | `int_execution_lending_aave_user_balances_daily.sql` |
| P1 | Add `dbt source freshness` on `contracts_aaveV3_PoolInstance_events` and `contracts_spark_Pool_events`; investigate and document the 4-day staleness and SparkLend zero-user result | `int_execution_lending_aave_daily.sql`, `fct_execution_lending_latest.sql` |
| P2 | Make the Agave scoping decision explicit: either add Agave to the pipeline (with `Deposit`→`Supply` event mapping) or update the unit title and docs to state "Aave V3 + SparkLend only" | `int_execution_lending_aave_daily.sql`, `api_execution_lending_tvl_by_token_latest.sql` |
| P2 | Decide and implement treasury (`mintToTreasury`) handling: filter or label the treasury address out of top lenders/TVL/lender counts; revisit the non-standard half-up rounding in `treasury_mint_deltas` | `int_execution_lending_aave_diffs_daily.sql`, `fct_execution_lending_top_lenders_ranked.sql` |
| P2 | Promote or demote the top-lenders stack deliberately: if `api_execution_lending_top_lenders_latest` is intended for consumers, change `dev` to `production` so the CI api-tag guard validates its schema | `fct_execution_lending_top_lenders_ranked.sql`, `fct_execution_lending_top_lenders_latest.sql`, `api_execution_lending_top_lenders_latest.sql` |
| P3 | Document the `protocol` column in `api_execution_lending_tvl_by_token_latest` schema.yml; correct the "aggregated across protocols" description | `api_execution_lending_tvl_by_token_latest.sql`, `schema.yml` |
| P3 | Remove dead `is_incremental()`/`lka`-JOIN branches from `int_execution_lending_aave_daily` or restore incremental materialization deliberately | `int_execution_lending_aave_daily.sql` |
| P3 | Standardize `bitmapCardinality()` over `toUInt64(groupBitmapMerge(...))` in `fct_execution_lending_weekly`; clean up duplicate `'lending,lending'` tags; replace `max(date)-7` with `subtractDays(max(date), 7)` | `fct_execution_lending_weekly.sql`, `fct_execution_lending_latest.sql`, `fct_execution_lending_top_lenders_latest.sql` |
| P3 | Confirm sDAI scoping boundary vs Savings xDAI module to prevent cross-unit TVL double-counting; close semantic-layer gaps (utilization, TVL, counts) and prune duplicate auto-generated weekly APY metrics | `api_execution_lending_tvl_by_token_latest.sql`, `semantic_models.yml` |
| P3 | Add grain, non-null, and range tests to Int256/UInt256 intermediates (`diffs_daily`, `user_balances_daily`, `utilization_daily`) | intermediate models |

---

## Open disagreements

None — reports converged in round 1.

---

## Review log

| Round | Agent | Challenge | Outcome |
|---|---|---|---|
| 1 | Inspector | No challenges issued to context agent | N/A |
| 1 | Context | No challenges issued to inspector agent | N/A |
| 1 | Verdict | Confirmed convergence: inspector and context mutually corroborate all high-severity findings; divergent granularity tags independently confirm the lenders/borrowers KPI asymmetry | Converged |
