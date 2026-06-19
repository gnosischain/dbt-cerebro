# Model review: ESG

**Convergence:** Converged in 2 rounds — all challenges resolved with warehouse evidence; inspector and context agents agree on root causes and no material disagreements remain.

---

## Scope and inventory

18 SQL models across three layers, plus semantic-layer coverage for all exposed models.

| Layer | Count | Notes |
|---|---|---|
| Staging | 2 | `stg_crawlers_data__ember_electricity_data`, `stg_crawlers_data__country_codes` |
| Intermediate | 7 | Chao-1 estimator, carbon intensity ensemble, node classification/client/geo distribution, dynamic power consumption, + 1 excluded `.sqlxxx` Monte Carlo stub |
| Marts | 9 | 1 fact table (`fct_esg_carbon_footprint_uncertainty`), 8 API views |
| Semantic models | 16 | All `quality_tier: candidate`; 5 expose intermediate-layer models directly |

The pipeline is a fully off-chain estimation system: Nebula P2P crawler data feeds a Chao-1 population estimator, which combines with CCRI hardware power tiers and Ember monthly grid carbon intensity to produce daily CO2 kg and energy kWh estimates with explicit 90%/95% confidence intervals. All models are owned by `analytics_team`.

---

## Business context

The ESG unit answers five questions for the Gnosis analytics dashboard (`analytics.gnosischain.com` ESG tab), the `/v1/esg/` REST API (9 Tier 0 public endpoints), the MCP semantic layer, and periodic Gnosis Foundation sustainability reporting:

1. Daily and annualised carbon footprint (kg CO2/day; tonnes CO2/year) with uncertainty bands
2. Daily and annualised energy consumption (kWh/day; MWh/year) with uncertainty bands
3. Network effective carbon intensity (gCO2/kWh) vs countries and other blockchains
4. Validator node population by operator archetype and geography, including hidden-node estimation
5. Per-node energy and carbon footprint per day

**Canonical definitions** (all confirmed against SQL and docs):

- `daily_co2_kg_mean`: sum over node categories and countries of `N_{c,k} * P_k * 24 * PUE_k * CI_c / 1e6`. Source: `fct_esg_carbon_footprint_uncertainty.sql`, `docs/esg-reporting/carbon-footprint.md`.
- `annual_co2_tonnes_projected`: `daily_mean_co2_kg * 365 / 1000` — a point-in-time daily rate annualised assuming steady state. The annualised carbon mart (`api_esg_carbon_emissions_annualised_latest`) excludes the current partial month; its energy twin does not (asymmetric — see findings).
- Node archetypes: `home_staker` (22 W, PUE 1.00), `professional_operator` (48 W, PUE 1.58), `cloud_hosted` (155 W, PUE 1.15). Constants from CCRI 2022 study; nodes unmatched by classification rules default to `professional_operator`.
- `carbon_intensity_gco2_kwh`: Ember Global Electricity Review gCO2/kWh per country-month. Fallback: country -> world average (~440 gCO2/kWh) -> hardcoded 450 gCO2/kWh.
- `effective_carbon_intensity` (live warehouse column name): node-count-weighted average CI across all nodes. Note: the semantic layer and `schema.yml` reference this column by the stale name `network_weighted_cif` — a live breakage (see findings).
- Chao-1 estimator: `S_obs + f1^2 / (2*f2)`, bias-corrected when f2=0, applied to monthly P2P crawler discovery data to estimate hidden nodes.
- Confidence intervals: 90% CI uses 1.645 multiplier; 95% CI uses 1.96. Combined relative uncertainty 22-40%.

No smart contracts or on-chain event feeds are involved. The only external data dependency is the Ember loader (confirmed live through April 2026) and the Nebula P2P crawler (confirmed live through 2026-06-08).

---

## Implementation assessment

### Critical

**`int_esg_carbon_intensity_ensemble` — toStartOfYear partition eviction destroys 11 months of CI data per year**
`models/ESG/intermediate/int_esg_carbon_intensity_ensemble.sql`, `macros/db/get_incremental_filter.sql`

The model uses `materialized='incremental'`, `incremental_strategy='insert_overwrite'`, `partition_by='toStartOfYear(month_date)'`. The `apply_monthly_incremental_filter` macro in insert_overwrite mode emits a filter that selects only the most recent complete Ember month. Each daily incremental run therefore REPLACEs the entire calendar-year partition with a single month's rows, silently evicting January-November on the December run. Warehouse-confirmed: the ensemble table contains only December 2023, December 2024, and December 2025 for all dates >= 2023-12-01. For non-December months, the downstream `INNER JOIN` in `int_esg_dynamic_power_consumption` produces zero rows (both the country-specific and world-average CI rows are absent), causing `fct_esg_carbon_footprint_uncertainty` to contain only 31 rows — one calendar month of history. Fix: change `partition_by` to `toStartOfMonth(month_date)`, then run `dbt run --full-refresh --select int_esg_carbon_intensity_ensemble+`.

**Semantic-layer ESG measures reference warehouse columns that do not exist**
`models/ESG/marts/fct_esg_carbon_footprint_uncertainty.sql`, `semantic/authoring/ESG/semantic_models.yml`, `models/ESG/marts/schema.yml`

`semantic_models.yml` defines measures `network_weighted_cif_value` (expr: `network_weighted_cif`) and `total_estimated_nodes_value` (expr: `total_estimated_nodes`). `describe_table` on the live `dbt.fct_esg_carbon_footprint_uncertainty` table confirms neither column exists. The final SELECT in the fact table aliases these as `effective_carbon_intensity` (line 288) and `estimated_nodes` (line 294). `schema.yml` lines 351 and 367 also document the stale names. Any MetricFlow or MCP semantic-layer query for these two measures fails today with a column-not-found error. Fix: update `semantic_models.yml` and `schema.yml` to reference `effective_carbon_intensity` and `estimated_nodes`.

### High

**LEFT JOIN NULL handling absent in `int_esg_dynamic_power_consumption` — zero carbon intensity for 7 country-category pairs**
`models/ESG/intermediate/int_esg_dynamic_power_consumption.sql`

The `carbon_intensity_lookup` CTE double-LEFT-JOINs `int_esg_carbon_intensity_ensemble` without a `join_use_nulls=1` session setting. ClickHouse returns Float64 default (0.0) for unmatched LEFT JOIN rows instead of NULL, so `COALESCE(ci_country.carbon_intensity_mean, ci_world.carbon_intensity_mean, 450.0)` resolves to `0.0` rather than the world fallback for 7 country-category pairs (AD, AE, CW, GI, HK, UA). Warehouse-confirmed: 159 rows (9.7% of 1,642 total) have `carbon_intensity_gco2_kwh = 0.0`. The malformed comment in the model (`-- nu;;s ghet repl;ace by date...`) signals a known but unresolved workaround attempt. Fix: add `pre_hook: 'SET join_use_nulls=1'` and `post_hook: 'SET join_use_nulls=0'` to the model config.

**Chao-1 incremental window is day-level due to delete+insert macro branch**
`models/ESG/intermediate/int_esg_node_population_chao1.sql`, `macros/db/get_incremental_filter.sql`

`apply_monthly_incremental_filter` emits a whole-month lower bound only when `incremental_strategy == 'insert_overwrite'` (line 33 of the macro). This model uses `delete+insert`, so the macro fires the else branch (lines 54-72), emitting `date >= max(observation_date)` — a single-day watermark. The source CTE `peer_connection_analysis` therefore only pulls rows from the last-processed date, not the full current-month partition. For `delete+insert` with `partition_by=toStartOfMonth(observation_date)`, Chao-1 is computed from one day's crawl data rather than the month's accumulated observations, producing a noisier and systematically lower population estimate. The sub-CTEs (`successful_chao1`, `all_attempts_chao1`, `peer_status_summary`, `failure_analysis`) each add an exclusive `> MAX()` filter (one step stricter than the source's inclusive `>=`), but the root defect is at the source level. Fix: switch to `incremental_strategy='insert_overwrite'` to activate the whole-month macro branch, or pass an explicit `lookback_days` value sufficient to cover the elapsed days of the current month.

**Missing current-month guard in `api_esg_energy_consumption_annualised_latest`**
`models/ESG/marts/api_esg_energy_consumption_annualised_latest.sql`

The inner subquery has no `WHERE toStartOfMonth(date) < toStartOfMonth(today())` filter. On any day after the 1st of a month, LIMIT 1 returns the current incomplete day's annualised projection. Its carbon twin (`api_esg_carbon_emissions_annualised_latest`) correctly excludes the current month. The two headline KPIs are therefore derived from different temporal windows and are not directly comparable (see also Business-logic assessment). Fix: add the standard partial-month exclusion filter.

### Medium

**`api_esg_carbon_timeseries_bands` exposes partial current-month data**
`models/ESG/marts/api_esg_carbon_timeseries_bands.sql`
Unlike every other mart in the layer, this model has no partial-month exclusion filter. Consumers see incomplete MTD and moving-average values for the current month. Fix: add `WHERE toStartOfMonth(date) < toStartOfMonth(today())`.

**Seasonal uncertainty inconsistency in `int_esg_carbon_intensity_ensemble`**
`models/ESG/intermediate/int_esg_carbon_intensity_ensemble.sql`
`carbon_intensity_std` is derived from unadjusted `base_ci` while `carbon_intensity_mean` applies `seasonal_factor`. In winter months for high-seasonality regions (e.g. Europe, `seasonal_factor=1.18`), the CI bands are erroneously tight relative to the elevated mean. The std should scale by the same `seasonal_factor` to maintain consistent coefficient of variation.

**`is_incremental()` blocks are dead code in three table-materialized models**
`models/ESG/intermediate/int_esg_node_classification.sql`, `models/ESG/intermediate/int_esg_node_client_distribution.sql`, `models/ESG/intermediate/int_esg_node_geographic_distribution.sql`
All three models are `materialized='table'` but contain `{% if is_incremental() %}` WHERE guards that can never fire. If any is later changed to `incremental`, the guards activate without review and may under-populate the table. Fix: remove the dead blocks or convert to incremental if that is the intent.

**Division-by-zero risk in `int_esg_node_classification` hidden_nodes_percentage**
`models/ESG/intermediate/int_esg_node_classification.sql`
Line 187: `round(100.0 * (s.estimated_total_nodes - s.observed_nodes) / s.estimated_total_nodes, 2)` has no NULLIF guard. If the LEFT JOIN to Chao-1 data returns the ClickHouse default of 0 and `observed_nodes` is also 0, ClickHouse returns NaN/inf, which propagates downstream. Fix: wrap the denominator in `nullIf(s.estimated_total_nodes, 0)`.

**`fct_esg_carbon_footprint_uncertainty` has no `order_by` config**
`models/ESG/marts/fct_esg_carbon_footprint_uncertainty.sql`
All other incremental ESG models specify `order_by` in their config. This model omits it, falling back to ClickHouse MergeTree default `tuple()` ordering. Point queries on `date` scan more granules than necessary. Low impact at 31 rows; degrades as history grows. Fix: add `order_by=['date']`.

**Double incremental filter in `fct_esg_carbon_footprint_uncertainty` `daily_power_data` CTE**
`models/ESG/marts/fct_esg_carbon_footprint_uncertainty.sql`
`node_country_distribution` already filters `date > MAX(date)`. `daily_power_data` adds the identical filter (lines 83-84). Currently redundant, but if `node_country_distribution` is widened for a lookback fix, the inner filter will silently suppress the additional rows. Fix: remove the duplicate filter from `daily_power_data`.

### Low

**`schema.yml` for `int_esg_node_classification` documents per-peer columns absent from aggregated output**
`models/ESG/intermediate/schema.yml`
Documents `observation_date`, `peer_id`, `ip_address`, `client_type`, `country_code`, `generic_provider`, `peer_org`, `last_seen_that_day`, and `node_category` — all from intermediate per-peer CTEs, not the final aggregated output (`date`, `node_category`, `observed_nodes`, `estimated_total_nodes`, etc.). Breaks schema validation tests and misleads downstream developers.

**`stg_crawlers_data__ember_electricity_data` schema.yml has duplicate and mismatched column names**
`models/ESG/staging/schema.yml`
Lists both original mixed-case raw column names (`Area`, `Date`, `Value`, etc.) and a second set of lowercase snake_case aliases (`area`, `date`, `value`, etc.) that do not exist in the view, which passes through raw column names verbatim.

---

## Business-logic assessment

### Critical

**Effective carbon intensity and CO2 figures reflect only December of each calendar year — undisclosed to API consumers**
`models/ESG/intermediate/int_esg_carbon_intensity_ensemble.sql`, `models/ESG/intermediate/int_esg_dynamic_power_consumption.sql`, `models/ESG/marts/fct_esg_carbon_footprint_uncertainty.sql`

Because `int_esg_carbon_intensity_ensemble` retains only December rows per year (partition eviction defect above), the `INNER JOIN` in `int_esg_dynamic_power_consumption` produces zero rows for January-November of every year. All published ESG carbon and energy KPIs are derived from December 2025 data and presented to API consumers as if they represent current or annual network state. For countries with grids that deviate significantly from the world average (e.g. France at ~60 gCO2/kWh vs world ~440 gCO2/kWh), CI is replaced with the world fallback for 11 months per year, potentially overstating emissions by up to 6x for those node populations with no disclosure in the API response.

### High

**Annualised KPI endpoints return non-comparable values**
`models/ESG/marts/api_esg_energy_consumption_annualised_latest.sql`, `models/ESG/marts/api_esg_carbon_emissions_annualised_latest.sql`

The energy annualised mart returns the current partial day's rate; the carbon annualised mart returns the last complete month's rate. A consumer dividing annual CO2 by annual energy to derive implied carbon intensity gets a number corresponding to no real period, and the ratio drifts day-by-day within each month.

**9.7% of power-consumption rows carry zero carbon intensity, understating network CO2 without disclosure**
`models/ESG/intermediate/int_esg_dynamic_power_consumption.sql`

159 of 1,642 rows have `carbon_intensity_gco2_kwh = 0.0` (countries AD, AE, CW, GI, HK, UA). These rows contribute zero CO2 to network totals. The API and dashboard do not flag affected countries. ESG reports derived from these figures carry uncommunicated downward bias.

**Chao-1 monthly population estimate is computed from a single day's crawl data**
`models/ESG/intermediate/int_esg_node_population_chao1.sql`

The estimator requires a reasonably complete observation sample. With only a single day's crawl data as input (day-level incremental filter), the f1 (singletons) and f2 (doubletons) counts reflect one crawl session rather than the month's accumulated discovery. This produces a noisier, potentially lower Chao-1 estimate, understating estimated node population and therefore understating energy and CO2 totals. The methodology documentation describes Chao-1 as applied to monthly observation accumulations.

### Medium

**All 15 semantic-layer ESG models are `quality_tier: candidate` serving Tier 0 public endpoints**
`semantic/authoring/ESG/semantic_models.yml`

No ESG metric has been promoted to production quality. The `candidate` label means "auto-generated; review and promote before relying on it." These metrics are cited in Gnosis Foundation sustainability reporting and served without authentication to external consumers. The quality tier and the actual exposure tier are inconsistent with the platform's quality governance framework.

**Seasonal CI adjustment applied to point estimate but not to uncertainty bands**
`models/ESG/intermediate/int_esg_carbon_intensity_ensemble.sql`

In winter months for high-seasonality regions, the published CI mean is elevated by `seasonal_factor` but the band width is not. The 90%/95% intervals are therefore erroneously narrow relative to the elevated mean, giving consumers false precision during the months when grid carbon intensity is most variable.

### Low

**Ember source coverage decreasing for 2026 months — world-average fallback activates for more countries without disclosure**
`models/ESG/intermediate/int_esg_dynamic_power_consumption.sql`, `models/ESG/intermediate/int_esg_carbon_intensity_ensemble.sql`

The Ember source contains monthly data through April 2026 with decreasing country coverage (43 countries for April 2026 vs 85-95 for prior years). Countries absent from recent Ember releases silently fall back to world-average CI. No indicator in the API response identifies which countries are using country-specific vs world-average CI for any given month.

---

## Data findings

Warehouse queries run across both review rounds (8 in round 1, additional in round 2). Key numbers:

| Query | Result |
|---|---|
| `int_esg_carbon_intensity_ensemble` max `month_date` | 2025-12-01 (December only per year confirmed) |
| `int_esg_carbon_intensity_ensemble` distinct months >= 2023-12-01 | Only 3: 2023-12-01, 2024-12-01, 2025-12-01 |
| `int_esg_dynamic_power_consumption` total rows | 1,642 (December 2025 only) |
| `int_esg_dynamic_power_consumption` zero-CI rows | 159 (9.7%); countries: AD, AE, CW, GI, HK, UA |
| `fct_esg_carbon_footprint_uncertainty` row count | 31 (2025-12-01 to 2025-12-10) |
| `fct_esg_carbon_footprint_uncertainty` max date | 2025-12-10 |
| `int_esg_node_geographic_distribution` row count | 22,824 rows, spanning 2025-03-27 to 2026-06-08 (live) |
| `stg_crawlers_data__ember_electricity_data` coverage | Monthly through April 2026; grain confirmed as one row per country per calendar month |
| Current annualised energy (API return) | ~512.94 MWh |
| Current annualised CO2 (API return) | ~153 tonnes CO2 |
| `fct_esg_carbon_footprint_uncertainty` `calculated_at` on CI ensemble | 2026-06-04 10:21:13 (last rebuild one week ago) |

The node geographic distribution table is fully live (16 months of data through 2026-06-08). The pipeline stall is confined to the CI ensemble, dynamic power, and fact layers — entirely explained by the partition eviction defect, not a crawler or scheduler outage.

---

## Pros / Cons

**Pros:**
- Methodology is principled: Chao-1 estimator, CCRI empirical power tiers, Ember CI data, and analytical uncertainty propagation are all credible, peer-reviewed choices appropriate for an estimation pipeline with explicitly stated 22-40% uncertainty.
- Uncertainty is first-class: 90% and 95% CI bands are propagated end-to-end through the fact table and exposed in the mart layer.
- Node-category taxonomy is well-specified with documented power/PUE constants traceable to CCRI 2022.
- API tag convention passes the CI guard; mart-layer models follow the canonical `api:/granularity:/window:/tier` tagging scheme.
- Fallback hierarchy for carbon intensity prevents hard failures when Ember coverage is incomplete.
- Incremental strategies are used throughout; pipeline endpoints map directly to documented consumer needs.
- Source data is fully off-chain and under the project's control.

**Cons:**
- The CI ensemble `toStartOfYear` partition eviction silently destroys 11 months of country-specific CI data each year, causing the entire downstream fact table to cover only December of each year — a critical undisclosed data quality defect.
- Semantic-layer measures `network_weighted_cif_value` and `total_estimated_nodes_value` reference columns absent from the live warehouse table, making these metrics broken for all current consumers.
- The Chao-1 model's incremental window is a single-day watermark (strategy/macro branch mismatch), so the monthly population estimate is never computed from the full month's crawl data.
- The annualised carbon and energy headline KPIs use asymmetric partial-month filters, making them non-comparable from the same API call.
- 159 rows (9.7%) of power-consumption data carry zero carbon intensity, understating network CO2 without disclosure.
- All 15 semantic-layer ESG models are `quality_tier: candidate` yet serve Tier 0 public endpoints cited in foundation sustainability reports.
- The fact table has only 31 rows of history (one calendar month), making trend views effectively one-dimensional.
- `schema.yml` for `int_esg_node_classification` documents per-peer columns that do not exist in the aggregated output.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| IMMEDIATE | Change `partition_by` in `int_esg_carbon_intensity_ensemble` from `toStartOfYear(month_date)` to `toStartOfMonth(month_date)`, then run `dbt run --full-refresh --select int_esg_carbon_intensity_ensemble+` to restore monthly CI coverage for all Ember data (2017-April 2026). Without this fix all downstream ESG figures are meaningless outside December months. | `models/ESG/intermediate/int_esg_carbon_intensity_ensemble.sql` |
| IMMEDIATE | Update `semantic_models.yml` and `schema.yml` to reference `effective_carbon_intensity` and `estimated_nodes` (not `network_weighted_cif` and `total_estimated_nodes`). These measures fail at query time today. | `semantic/authoring/ESG/semantic_models.yml`, `models/ESG/marts/schema.yml` |
| HIGH | Add `pre_hook: 'SET join_use_nulls=1'` and `post_hook: 'SET join_use_nulls=0'` to `int_esg_dynamic_power_consumption`. Verify the 159 zero-CI rows disappear before rerunning downstream models. | `models/ESG/intermediate/int_esg_dynamic_power_consumption.sql` |
| HIGH | Add `WHERE toStartOfMonth(date) < toStartOfMonth(today())` to `api_esg_energy_consumption_annualised_latest` to match the carbon twin and make the two headline KPIs comparable. Apply the same filter to `api_esg_carbon_timeseries_bands`. | `models/ESG/marts/api_esg_energy_consumption_annualised_latest.sql`, `models/ESG/marts/api_esg_carbon_timeseries_bands.sql` |
| HIGH | Fix the Chao-1 incremental strategy mismatch: switch `int_esg_node_population_chao1` to `incremental_strategy='insert_overwrite'` to activate the whole-month macro branch, or set `lookback_days` to cover the current month's elapsed days. Validate that f1/f2 counts reflect the full month's cumulative observations. | `models/ESG/intermediate/int_esg_node_population_chao1.sql` |
| MEDIUM | After the CI ensemble full-refresh, run a historical backfill for `int_esg_dynamic_power_consumption` and `fct_esg_carbon_footprint_uncertainty` covering March 2025-present (node geo-distribution data already available: 22,824 rows confirmed). | `models/ESG/intermediate/int_esg_dynamic_power_consumption.sql`, `models/ESG/marts/fct_esg_carbon_footprint_uncertainty.sql` |
| MEDIUM | Scale `carbon_intensity_std` by `seasonal_factor` in `int_esg_carbon_intensity_ensemble` to maintain consistent coefficient of variation across seasonally-adjusted months. | `models/ESG/intermediate/int_esg_carbon_intensity_ensemble.sql` |
| MEDIUM | Fix `schema.yml` for `int_esg_node_classification` to document actual aggregated output columns. Remove duplicate/incorrect column entries from `stg_crawlers_data__ember_electricity_data` schema.yml. | `models/ESG/intermediate/schema.yml`, `models/ESG/staging/schema.yml` |
| MEDIUM | Add `nullIf(s.estimated_total_nodes, 0)` to the `hidden_nodes_percentage` denominator in `int_esg_node_classification`. Remove dead `{% if is_incremental() %}` blocks from the three `materialized='table'` models. Add `order_by=['date']` to `fct_esg_carbon_footprint_uncertainty`. Remove the duplicate incremental filter from `fct_esg_carbon_footprint_uncertainty` `daily_power_data` CTE. | `models/ESG/intermediate/int_esg_node_classification.sql`, `models/ESG/intermediate/int_esg_node_client_distribution.sql`, `models/ESG/intermediate/int_esg_node_geographic_distribution.sql`, `models/ESG/marts/fct_esg_carbon_footprint_uncertainty.sql` |
| LONG TERM | Formally promote the core ESG semantic models (`esg_carbon_footprint_uncertainty`, `esg_carbon_emissions_annualised_latest`, `esg_energy_consumption_annualised_latest`) from `quality_tier: candidate` to production after the above fixes are deployed and validated. Serving candidate-tier metrics on Tier 0 public endpoints cited in foundation sustainability reports is inconsistent with the platform's quality governance framework. | `semantic/authoring/ESG/semantic_models.yml` |

---

## Review log

| Round | Challenge | Outcome |
|---|---|---|
| 1 | Inspector finding: Chao-1 source CTE "correctly emits a whole-month lower bound" for delete+insert models | Round 2: UPHELD AND UPGRADED — macro re-read confirmed the whole-month branch only fires for `insert_overwrite`; delete+insert takes the day-level else branch; severity confirmed high |
| 1 | Context finding: Ember data is "annual-grain" causing CI to apply for only one month per year | Round 2: SUBSTANTIALLY REFUTED — warehouse query on source table confirmed monthly grain (12 rows per country per year); the CI ensemble defect is a partition eviction bug in the incremental model, not a loader grain issue |
| 1 | Context finding: ESG pipeline stale since 2025-12-10 due to possible scheduler/crawler failure | Round 2: ROOT CAUSE REVISED — node geo-distribution table is live through 2026-06-08; stall is caused entirely by the CI ensemble partition eviction defect; no scheduler or crawler outage found |
| 2 | Inspector challenge: semantic-layer measures reference stale column names | Round 2: UPHELD — `describe_table` on live warehouse confirmed `effective_carbon_intensity` and `estimated_nodes` are the actual column names; `network_weighted_cif` and `total_estimated_nodes` are absent; live breakage confirmed |
