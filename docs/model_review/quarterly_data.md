# Model review: quarterly_data

**Convergence:** converged in 2 rounds — all inspector and context challenges resolved with concrete file reads and warehouse queries; agents reached mutual consistency on every material finding.

---

## Scope and inventory

| Layer | Count | Subsectors |
|---|---|---|
| Mart models (views) | 20 | circles (4), esg (2), gnosis_app (4), gnosis_chain (3), gnosis_pay (4), stablecoins (3) |
| Intermediate models | 3 | esg (2), stablecoins (1) |
| Intermediate (shared) | 2 | esg carbon fallback, stablecoin cohorts stats |
| Schema YML files | 8 | one per subsector mart directory |
| **Total SQL files** | **25** | — |

All 20 mart models are materialized as views, tagged `api:` / `granularity:quarterly` / `tier:0`, and expose REST endpoints via the Cerebro API factory. Every endpoint allows unfiltered access and is paginated. None have semantic layer authoring. No project-level config overrides exist in `dbt_project.yml` for this sector — all models inherit defaults.

---

## Business context

The unit answers a single class of question: what was the state or activity level for a given product area in a given quarter? It is the authoritative quarterly roll-up layer for cross-product trend analysis, consumed by the Cerebro REST API, quarterly executive and investor reporting, and external dashboards on analytics.gnosischain.com. It does not serve real-time or daily operational monitoring.

**Canonical metric definitions (authoritative across both review rounds):**

- `registered_humans` (circles): Cumulative Human-type Circles v2 avatars at quarter-end. `argMax(total, date)` from `fct_execution_circles_v2_avatars WHERE avatar_type = 'Human'`.
- `active_trusts` (circles): Active trust links at quarter-end. `argMax(active_trusts, date)` from `fct_execution_circles_v2_active_trusts_daily`.
- `co2_tonnes_yr` / `energy_mwh_yr` (ESG): Annualised CO2 (tonnes/yr) and energy (MWh/yr) at quarter-end via `argMax` from `int_quarterly_esg_carbon_footprint_with_fallback`, filtered `WHERE toStartOfMonth(date) < toStartOfMonth(today())`. This excludes the current in-progress calendar month only; completed months of the current quarter are included. The `is_estimated` flag is True when Ember carbon-intensity data is missing and forward-filled values are used.
- `nodes_estimated` / `nodes_lower_95` / `nodes_upper_95` / `nodes_observed` (ESG): Quarter-end node counts with 95% CI bounds, sourced from `int_esg_node_classification`. Observed = directly reachable via Nebula P2P crawler; estimated includes Chao-1 hidden population.
- `swaps` / `swaps_filled` / `volume_usd` (gnosis_app): Quarterly swap totals from `fct_execution_gnosis_app_swaps_monthly` (which correctly guards `toStartOfMonth < toStartOfMonth(today())`).
- `peak_daily_swappers` (gnosis_app): `max(n_swappers)` from `fct_execution_gnosis_app_swaps_daily`. Unlike the monthly-source siblings, this source carries no `date < today()` guard.
- `transactions` (gnosis_chain): `sum(n_txs) WHERE success=1` from `int_execution_transactions_info_daily`.
- `staked_gno` (gnosis_chain): `argMax(effective_balance, date) / 32` from `int_consensus_validators_balances_daily`. The `/32` derives from the Gnosis Chain validator slot convention (1 GNO per slot, matching Ethereum's 32 ETH analogue). The intermediate table stores values in whole GNO (not Gwei); the formula is used identically in `api_consensus_staked_daily`.
- `validators_active` (gnosis_chain): `argMax(cnt, date) WHERE status='active_ongoing'` from `int_consensus_validators_status_daily`.
- `payments` / `volume_usd` / `cashback_usd` (gnosis_pay): Quarterly totals from `fct_execution_gpay_kpi_monthly`.
- `peak_monthly_active_users` (gnosis_pay): `max(mau)` from `fct_execution_gpay_kpi_monthly` — peak monthly active user count within the quarter, not a quarterly aggregate. The column name and schema descriptions are unambiguous; the endpoint name `api_quarterly_data_gpay_active_users` omits "peak", creating a discoverability risk for API consumers.
- `peg_class` (stablecoins): Binary — `USD-pegged` = {WxDAI, sDAI, USDC, USDC.e, USDT}; `non-USD` = all other `token_class='STABLECOIN'` tokens except BRZ. xDAI (native gas token, symbol `'xDAI'`) is not in the USD-pegged list and silently classifies as `non-USD`.
- `tokens_included` (stablecoins): Hardcoded CASE strings — `'WxDAI, sDAI, USDC, USDC.e, USDT'` and `'EURe, GBPe, BRLA, ZCHF, svZCHF'`. New tokens entering `tokens_whitelist.csv` are aggregated but not reflected in this label without a model edit.
- ESG PUE constants: `home_staker=1.0`, `professional_operator=1.58`, `cloud_hosted=1.15`. These are CCRI-calibrated values used consistently in both `int_esg_dynamic_power_consumption.sql` and `int_quarterly_esg_carbon_footprint_with_fallback.sql`. The `carbon-footprint.md` docs cite approximate industry ranges (`~1.2`, `~1.1-1.4`) as conceptual guidance, not production constants; there is no divergence between fallback and production ESG layers.

**Contract context:** Circles models aggregate full Circles v2 ecosystem scope (all apps, all on-chain interactions). Gnosis App swaps use CoW Protocol order fills via EntryPoint v0.7 + `gnosis_app_relayers` seed scoping. GP active user = wallet with >= 1 Payment event (deposits and cashback excluded). GP cashback and Circles gCRC cashback are entirely separate programs and must never be compared or unioned. Stablecoin token classification comes from `seeds/tokens_whitelist.csv`.

---

## Implementation assessment

### High

**`peak_swappers` reads unfiltered daily source — partial-day count inflation**
`models/quarterly_data/gnosis_app/marts/api_quarterly_data_gnosis_app_peak_swappers.sql` reads `fct_execution_gnosis_app_swaps_daily` with no `date < today()` guard. All three monthly-source siblings (`swaps`, `swaps_filled`, `swap_volume`) read `fct_execution_gnosis_app_swaps_monthly` which filters `toStartOfMonth < toStartOfMonth(today())`. The `peak_daily_swappers` value for the current quarter includes today's in-progress partial-day swapper count and can overstate the peak. Fix: add `WHERE date < today()` to the daily source read in this model.

### Medium

**Zero dbt data tests across all quarterly_data schema.yml files**
None of the 8 schema.yml files in `models/quarterly_data/` contains a single `unique`, `not_null`, or `accepted_values` test. For aggregated views this means no CI detection of grain violations, null primary key columns, or unexpected enum values. Minimum required: `unique` on `(quarter)` for single-grain models; `unique` on `(quarter, peg_class)` for stablecoin multi-dimensional models; `not_null` on `quarter` across all 20 models.

**ESG CROSS JOIN fallback produces silent zero-row output on empty source**
`models/quarterly_data/esg/intermediate/int_quarterly_esg_carbon_footprint_with_fallback.sql` performs `CROSS JOIN last_existing_date` (a single-row `max(date)` subquery) on both `node_distribution` and `client_efficiency_by_category` CTEs. If `fct_esg_carbon_footprint_uncertainty` is ever empty, `last_existing_date` returns zero rows, the CROSSes produce empty output, and the quarterly ESG models serve no estimated rows — with no error raised and no alerting. Two of three currently exposed ESG quarters are already estimated rows, making this failure mode live-impactful.

**Stablecoin `tokens_included` label is static — new tokens silently enter aggregates unlabelled**
The CASE expression generating `tokens_included` in `models/quarterly_data/stablecoins/marts/api_quarterly_data_stablecoin_transfers.sql`, `api_quarterly_data_stablecoin_supply.sql`, and `api_quarterly_data_stablecoin_holders.sql` is a hardcoded string. Any new token added to `tokens_whitelist.csv` with `token_class='STABLECOIN'` is included in supply/holder/transfer aggregates but does not appear in `tokens_included` without a model edit. Consumers reading `tokens_included` as the canonical token list receive a misleading view of what is aggregated.

### Low

**`argMax(is_estimated, date)` collapses mixed-flag quarters to worst-case label**
For a quarter where real data covers early months and forward-filled estimated data covers the tail, `argMax` always picks `is_estimated` from the last date — always True for any quarter with a forward-filled tail day. An 80%-real quarter reports `is_estimated=True`, losing the distinction between mostly-real and mostly-estimated quarters. Affected models: `models/quarterly_data/esg/marts/api_quarterly_data_carbon_emissions.sql` and `api_quarterly_data_energy_consumption.sql`.

---

## Business-logic assessment

### High

**All subsectors serve incomplete current-quarter rows without a completeness flag**
As of 2026-06-11 every quarterly mart exposes a 2026-Q2 row. For sum-type metrics (transactions, swaps, volume, cashback, payments) this row represents approximately 2.5 months of a 3-month quarter. No `is_complete`, `is_partial`, or `quarter_end_date` column is exposed across any subsector. ESG exposes `is_estimated` but not quarter completeness. External consumers and dashboards have no programmatic way to exclude in-progress quarters from trend calculations or annotate them in quarterly reporting. Affected: all marts under `models/quarterly_data/gnosis_app/marts/`, `gnosis_chain/marts/`, `gnosis_pay/marts/`, `circles/marts/`, `stablecoins/marts/`.

### Medium

**ESG and non-ESG subsectors apply different current-period cutoffs — Q-in-progress reporting is inconsistent**
ESG models filter `WHERE toStartOfMonth(date) < toStartOfMonth(today())`, excluding the current calendar month (lag up to 31 days on the most recent month of the current quarter). All other subsectors filter `WHERE date < today()`, including data through yesterday. This means Q2-2026 rows across subsectors simultaneously reflect different time windows: circles/gnosis_chain/gnosis_pay/gnosis_app/stablecoins include data through 2026-06-10; ESG includes data only through 2026-05-31. Cross-subsector quarterly dashboards silently compare different windows. Affected: `models/quarterly_data/esg/marts/api_quarterly_data_carbon_emissions.sql` and `api_quarterly_data_energy_consumption.sql`.

**xDAI (USD-pegged native token) silently classified as non-USD stablecoin**
xDAI (symbol `'xDAI'` in `tokens_whitelist.csv`, `token_class='STABLECOIN'`) does not match the USD-pegged CASE list and flows into non-USD aggregates. It inflates non-USD supply and holder counts with a USD-pegged asset. Whether intentional or not, the schema.yml `peg_class` description does not document xDAI's classification, leaving consumers no way to understand or replicate the split. Affected: all three stablecoin mart models and `models/quarterly_data/stablecoins/marts/schema.yml`.

**GP active users endpoint name omits "peak" — aggregation method not discoverable from path**
`api_quarterly_data_gpay_active_users` reports `max(mau)` — peak monthly active user count within the quarter. The column name `peak_monthly_active_users` and schema.yml descriptions are unambiguous. However, the REST API endpoint path itself lacks "peak", and a consumer discovering the endpoint by name in an API catalogue or dashboard query without reading schema descriptions will assume quarterly-aggregate or average semantics and make incorrect Q-over-Q comparisons. Affected: `models/quarterly_data/gnosis_pay/marts/api_quarterly_data_gpay_active_users.sql`.

**No semantic layer authoring for any of the 20 tier0 quarterly endpoints**
The `semantic/authoring/` directory has no `quarterly_data/` subdirectory. Grep of the entire semantic tree returns zero hits for `api_quarterly_data`. None of the 20 quarterly mart models are accessible to the MCP semantic layer for metric composition, `query_metrics`, or `quick_metric_chart`. These models are the primary quarterly KPI surface for executive reporting, making the coverage gap high-impact. Affected: all models under `models/quarterly_data/`.

### Low

**ESG carbon data coverage starts at Q3-2025 only — mismatched history depth across subsectors**
Warehouse confirms `api_quarterly_data_carbon_emissions` has 3 rows (Q3-2025, Q1-2026, Q2-2026) while circles has 7 quarters, stablecoin cohorts has 24 quarters, and gnosis_chain validators has 19 quarters. This is a data-availability limitation rather than a model defect. The schema.yml does not document the coverage start date, so consumers have no expectation anchor. Affected: `models/quarterly_data/esg/marts/api_quarterly_data_carbon_emissions.sql`, `api_quarterly_data_energy_consumption.sql`.

**Stablecoin holder counts are token-level not address-level — double-count not prominently surfaced**
The schema.yml documents the caveat for the holders model only; the supply and transfers models do not note it. For quarterly investor reporting where "number of stablecoin holders" is a KPI, the token-level count can substantially overstate address-level reach. Affected: `models/quarterly_data/stablecoins/marts/api_quarterly_data_stablecoin_holders.sql` and `schema.yml`.

---

## Data findings

Warehouse queries executed across 8 distinct queries during the review:

- `api_quarterly_data_circles_active_trusts`: 7 rows, max quarter 2026-Q2, 0 null keys.
- `int_quarterly_stablecoin_cohorts_stats` grain check: 383 rows, 24 distinct quarters, max 2026-Q2, 0 duplicates on `(quarter, balance_bucket)`.
- `api_quarterly_data_gnosis_app_peak_swappers`: 3 rows, max 2026-Q2.
- `api_quarterly_data_staked_gno`: 19 rows.
- `api_quarterly_data_carbon_emissions`: 3 rows (Q3-2025, Q1-2026, Q2-2026), 2 rows `is_estimated=True`.
- ESG fallback daily row distribution: 61 rows for Q2-2026 covering April and May 2026 only, confirming partial-month cutoff.
- `stg_consensus__validators` effective balance range: 2024-Q1 end shows all 199,091 validators between 25e9–32e9 Gwei (uniform 32 GNO cap); 2026-Q1 end shows 140,671 validators at ~32 GNO but 4,070 validators averaging 1,785 GNO (max 2,048 GNO), confirming EIP-7251-style validator consolidation as the cause of the rising staked_gno-per-validator ratio (1.07 in 2024-Q1 to 3.0 in 2026-Q2). This is not a formula error.
- Cross-check of `staked_gno` formula: `int_consensus_validators_balances_daily` stores values in whole GNO; dividing by 32 is consistent with the project convention and produces ~334,875 GNO, matching `api_consensus_info_staked_latest` for the same period.

---

## Pros / Cons

**Pros**
- Consistent `api:` / `granularity:quarterly` / `tier:0` tag convention across all 25 models — REST API routing is reliable and unambiguous.
- `argMax`-based end-of-quarter snapshot pattern is correct for ReplacingMergeTree sources and avoids `FINAL` overhead.
- Monthly source tables (gnosis_app swaps, gnosis_pay KPI) correctly guard against partial-month inflation for sum-type metrics.
- ESG `is_estimated` flag correctly propagates forward-fill status to API consumers.
- `stablecoin peg_class` and `tokens_included` label covers the live token set as of mid-2025 (EURe, GBPe, BRLA, ZCHF, svZCHF).
- All 20 mart models are materialized as views — zero storage cost, always-current reads.
- Cohort bucket model correctly excludes non-USD `cohort_unit` rows and uses `nullIf` to avoid divide-by-zero in `avg_balance_usd`.
- PUE constants (professional 1.58, cloud 1.15) are consistent between the production ESG model and the fallback intermediate — no divergence.

**Cons**
- No `is_complete` or `is_partial` flag on any quarterly row — consumers cannot distinguish an in-progress 2026-Q2 row (covering ~2.5 months) from a closed historical quarter.
- Zero dbt data tests across all 8 schema.yml files — no CI detection of grain violations, null primary keys, or unexpected enum values.
- `peak_swappers` reads `fct_execution_gnosis_app_swaps_daily` with no `date < today()` guard, unlike all monthly-source sibling models.
- ESG uses month-level date exclusion while all other subsectors use day-level — current-quarter completeness is inconsistent across subsectors.
- No semantic layer authoring for any of the 20 tier0 quarterly endpoints — MCP-driven quarterly comparisons are impossible without raw SQL.
- Hardcoded `tokens_included` label strings will silently diverge from actual aggregated tokens when new stablecoins are added to `tokens_whitelist.csv`.
- xDAI (native gas token, symbol `'xDAI'`) silently classifies as non-USD stablecoin despite being USD-pegged.
- ESG CROSS JOIN fallback against `last_existing_date` subquery silently produces zero rows if the carbon footprint source table is ever empty — no alerting or guard.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P1 | Add an `is_complete` boolean column to all quarterly mart models (`True` when `today() > toStartOfQuarter(today()) + INTERVAL 3 MONTH`) — single computed column, no join changes required | All 20 mart models |
| P1 | Add `WHERE date < today()` to `fct_execution_gnosis_app_swaps_daily` reads in `api_quarterly_data_gnosis_app_peak_swappers` to align with the partial-day guard used by all sibling models | `models/quarterly_data/gnosis_app/marts/api_quarterly_data_gnosis_app_peak_swappers.sql` |
| P1 | Add minimum dbt tests to all quarterly_data schema.yml files: `unique` on `(quarter)` for single-grain models; `unique` on `(quarter, peg_class)` for stablecoin models; `not_null` on `quarter` across all 20 models | All 8 schema.yml files |
| P2 | Add a guard against empty-source CROSS JOIN in the ESG fallback intermediate: add a dbt source freshness test on `fct_esg_carbon_footprint_uncertainty` and document that the model produces zero rows (not an error) when the source is empty | `models/quarterly_data/esg/intermediate/int_quarterly_esg_carbon_footprint_with_fallback.sql` |
| P2 | Standardise current-period cutoffs: either align ESG to use `WHERE date < today()` (accepting partial-month carbon estimates) or document in every non-ESG quarterly schema.yml that the current quarter includes partial-day data and note the ESG exception | `models/quarterly_data/esg/marts/api_quarterly_data_carbon_emissions.sql`, `api_quarterly_data_energy_consumption.sql`; all non-ESG schema.yml files |
| P2 | Document xDAI classification explicitly in `models/quarterly_data/stablecoins/marts/schema.yml`: state whether xDAI is intentionally excluded from the USD-pegged bucket (as a gas token), and if so add `AND symbol != 'xDAI'` or a `gas-token-excluded` label to make the exclusion programmatically visible | `models/quarterly_data/stablecoins/marts/schema.yml` and stablecoin mart SQLs |
| P2 | Rename `api_quarterly_data_gpay_active_users` to `api_quarterly_data_gpay_peak_active_users` or add an `api_description` meta field to the model config stating "peak monthly active users within the quarter, not a quarterly aggregate" | `models/quarterly_data/gnosis_pay/marts/api_quarterly_data_gpay_active_users.sql` |
| P3 | Replace hardcoded `tokens_included` CASE strings with a dynamic aggregation (e.g., `groupArray(DISTINCT symbol)` within each `peg_class` per quarter) so new stablecoins added to `tokens_whitelist.csv` automatically appear without a model edit | `models/quarterly_data/stablecoins/marts/api_quarterly_data_stablecoin_transfers.sql`, `api_quarterly_data_stablecoin_supply.sql`, `api_quarterly_data_stablecoin_holders.sql` |
| P3 | Prioritise semantic authoring for the five highest-traffic quarterly endpoints: `api_quarterly_data_staked_gno`, `api_quarterly_data_transactions`, `api_quarterly_data_gpay_payments`, `api_quarterly_data_circles_registered_humans`, `api_quarterly_data_stablecoin_supply` | `models/quarterly_data/` (all subsectors) |
| P3 | Document ESG coverage start date (Q3-2025) and the `argMax(is_estimated, date)` worst-case-label behaviour in `api_quarterly_data_carbon_emissions` and `api_quarterly_data_energy_consumption` schema.yml descriptions | `models/quarterly_data/esg/marts/schema.yml` |

---

## Open disagreements

None. The review converged fully in round 2.

---

## Open questions (product decisions, not analytical)

- Should the quarterly API expose an `is_complete` boolean so consumers can distinguish in-progress quarters from historical ones across all subsectors — especially for sum-type metrics?
- xDAI (symbol `'xDAI'`) falls into non-USD peg class: intentional exclusion from the USD-pegged group (as a gas token), or should it be excluded from stablecoin reporting altogether?
- Should ESG use `WHERE date < today()` for consistency with other subsectors, or is the month-level cutoff intentional to avoid partial-month carbon estimates?
- Should `api_quarterly_data_gpay_active_users` be renamed to surface the peak-MAU semantics at the endpoint level?
- Is there a plan to expose a validator consolidation metric (e.g., validators above 32 GNO, or total GNO held by consolidated validators) given the bimodal effective-balance distribution now visible in 2026-Q1 data?
- Should semantic authoring be created for the 20 tier0 quarterly endpoints to enable MCP-driven quarterly comparisons? Which models are priority approval candidates?

---

## Review log

| Round | Event | Outcome |
|---|---|---|
| 1 | Inspector raised `bucket_order` absent from schema.yml as a CI risk | Round 2: RETRACTED — `check_api_tags.py` reads the dbt manifest (from schema.yml), not `DESCRIBE TABLE`; `bucket_order` appears only in `ORDER BY` and is never a SELECT output column |
| 1 | Inspector flagged rising staked_gno / validators_active ratio (1.07 in 2024-Q1 to 3.0 in 2026-Q2) as potentially a data or formula error | Round 2: CONFIRMED as validator consolidation — warehouse queries on `stg_consensus__validators` show 4,070 validators averaging 1,785 GNO in 2026-Q1 (EIP-7251-equivalent max effective balance increase); formula is correct |
| 1 | Context report caveat cited PUE ~1.2 for professional_operator, diverging from SQL value of 1.58 | Round 2: RETRACTED — carbon-footprint.md uses `~1.2` as an approximate typical-range description, not a calibrated constant; 1.58 is the CCRI-sourced canonical value used consistently in both production and fallback intermediates |
| 1 | Context report caveat stated ESG quarterly models exclude the entire current quarter | Round 2: CORRECTED — the `toStartOfMonth` filter excludes only the current in-progress calendar month; completed months of the current quarter are included; lag is at most 31 days on the most recent month |
| 1 | Context report raised GP active users schema documentation sufficiency as a concern | Round 2: RESOLVED — schema.yml descriptions and column name are explicit; the issue is narrowed to endpoint-level naming discoverability only (path omits "peak") |
| 2 | Final convergence check: no remaining disagreements | Converged — all challenges resolved or rebutted with warehouse evidence and file reads |
