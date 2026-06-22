# Model review (revisit 2026-06-21): ESG

Re-verification of baseline `docs/model_review/ESG.md` (dated `2026-06-11`) across 21 cases, 3 rounds each: `0` resolved, `4` changed (symptom shifted but root cause persists), `17` still confirmed; the two `critical` defects (toStartOfYear partition eviction in `int_esg_carbon_intensity_ensemble` and 13 broken semantic measures on `fct_esg_carbon_footprint_uncertainty`) remain wide open, and no case is attributable to the June `insert_overwrite` wipe incident.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| ESG-C01 | P0-01 | `toStartOfYear` partition_by + insert_overwrite evicts 11/12 months per year in CI ensemble | critical | CONFIRMED | critical | high | none | 3 |
| ESG-C02 | P0-02 | Semantic measures reference columns absent from live fct (broken MetricFlow/MCP) | critical | CONFIRMED | critical | high | none | 3 |
| ESG-C03 | P0-09 | CI lookup double-LEFT-JOIN without `join_use_nulls` -> 0.0 not 450 fallback | high | CONFIRMED | high | high | none | 3 |
| ESG-C04 | - | Chao-1 model `delete+insert` watermark yields single-day, not whole-month, input | high | CONFIRMED | high | medium | none | 3 |
| ESG-C05 | P0-10 | Energy annualised mart lacks current-month exclusion its carbon twin has | high | CONFIRMED | high | high | none | 3 |
| ESG-C06 | - | `api_esg_carbon_timeseries_bands` has no partial-month exclusion filter | medium | CONFIRMED | medium | high | none | 3 |
| ESG-C07 | - | `carbon_intensity_std` from unadjusted base_ci while mean scaled by seasonal_factor | medium | CONFIRMED | medium | high | none | 3 |
| ESG-C08 | - | Dead `is_incremental()` guards in 3 `materialized='table'` models | medium | CONFIRMED | low | high | none | 3 |
| ESG-C09 | - | `hidden_nodes_percentage` divides by `estimated_total_nodes` with no NULLIF | medium | CHANGED | low | high | none | 3 |
| ESG-C10 | - | `fct_esg_carbon_footprint_uncertainty` has no `order_by` in config | medium | CONFIRMED | low | high | none | 3 |
| ESG-C11 | - | `daily_power_data` CTE repeats redundant `date > MAX(date)` filter | medium | CONFIRMED | low | high | none | 3 |
| ESG-C12 | - | schema.yml documents phantom per-peer cols incl test-bearing `peer_id` | low | CONFIRMED | medium | high | none | 3 |
| ESG-C13 | - | Ember staging schema.yml lists phantom lowercase column aliases | low | CONFIRMED | low | high | none | 3 |
| ESG-C14 | P0-01 | CI eviction -> Jan-Nov fct rows fallback-substituted, presented as current | critical | CHANGED | high | high | none | 3 |
| ESG-C15 | P0-10 | Energy (current-day) vs carbon (last-complete-month) marts -> incoherent implied CI | high | CONFIRMED | high | high | none | 3 |
| ESG-C16 | - | Zero-CI rows contribute 0 CO2; uncommunicated downward bias, growing | high | CONFIRMED | high | high | none | 3 |
| ESG-C17 | - | Single-day Chao-1 understates node population -> energy/CO2 totals | high | CONFIRMED | high | high | none | 3 |
| ESG-C18 | P0-19 | All ESG semantic models `quality_tier: candidate` backing tier0/tier1 endpoints | medium | CONFIRMED | medium | medium | none | 3 |
| ESG-C19 | - | Seasonal mean elevated but CI bands not widened -> false winter precision | medium | CONFIRMED | medium | high | none | 3 |
| ESG-C20 | - | Ember country coverage decaying (~80 -> 41); silent world-avg fallback, no indicator | low | CHANGED | low | high | none | 3 |
| ESG-C21 | P0-01 | Geographic distribution live; published annualised energy/CO2 figures | high | CHANGED | high | high | none | 3 |

Rollup: confirmed `16` / changed `4` / resolved `0` / new `0` / unverifiable `0`. (ESG-C09 settled CHANGED+low; the remaining 16 non-changed cases are CONFIRMED.)

## Delta vs baseline

### RESOLVED (0)
None. No case cleared re-verification.

### CHANGED (4) — symptom shifted, root cause persists
- **ESG-C09** (`models/ESG/intermediate/int_esg_node_classification.sql` L187): code unchanged (no `NULLIF` on the `estimated_total_nodes` denominator), but the claimed NaN/inf is structurally unreachable — live `min(observed_nodes)=1`, `min(estimated_total_nodes)=2`, `0` rows at zero across `1792` rows. Reclassified to a latent defensive-only defect; severity `medium -> low`. Incident: none.
- **ESG-C14** (`fct_esg_carbon_footprint_uncertainty.sql`): the baseline "fact is Dec-only / INNER-join yields zero rows Jan-Nov" symptom is gone — fct now holds `182` rows over 6 contiguous months (`2025-12-01`..`2026-05-31`). But the root cause persists: because the CI ensemble retains only Dec + May 2026, the Jan-Apr 2026 LEFT JOIN returns CH-default `0.0` mislabelled `carbon_intensity_source='country_specific'` (the ESG-C03 mechanism), so those months are silently fallback-derived. Severity stays `high`. Incident: NOT the June `insert_overwrite` wipe — this is the standalone `toStartOfYear` partition-grain defect.
- **ESG-C20** (Ember staging source): source advanced from April to May 2026; latest-month country coverage now `41` (vs `~79-80` historically; `57` in April, `72` in March). Decaying-coverage pattern confirmed and steepening. Severity `low`. Incident: none.
- **ESG-C21** (`int_esg_node_geographic_distribution`): liveness claim holds (`23,366` rows to `2026-06-20`), but the published figures moved materially — annualised energy `512.94 -> 708.13` MWh, annualised CO2 `153 -> 125.05` t, implied network CI `~298 -> ~177` g/kWh. The implied-CI drop is fully explained by the rising zero-CI fallback share from ESG-C16 (no residual unexplained delta). Severity stays `high`. Incident: none.

### STILL CONFIRMED (17)
- **ESG-C01** (critical): `partition_by='toStartOfYear(month_date)'` still in `int_esg_carbon_intensity_ensemble.sql`; only `4` distinct months survive `>= 2023-12-01` (`2023-12`, `2024-12`, `2025-12`, `2026-05`). Per-row `calculated_at` proves ongoing per-run eviction (2026-05 row written today `2026-06-21 15:13`, evicting Jan-Apr 2026). NOT the June incident.
- **ESG-C02** (critical): `13` semantic measures (not 2) reference phantom columns; live fct exposes `effective_carbon_intensity`/`estimated_nodes`/`baseline_observed_nodes`/etc. Any MetricFlow/MCP build over these measures fails `UNKNOWN_IDENTIFIER`.
- **ESG-C03** (high): `1221/8970` (`13.61%`) rows have `carbon_intensity_gco2_kwh=0.0`; all tagged `country_specific`, `world_average=0` across all rows — the 450 world fallback never fires. Malformed `WHERE` comment `-- nu;;s ghet repl;ace by date...` still at L176; no `join_use_nulls=1`.
- **ESG-C04** (high): `incremental_strategy='delete+insert'` -> single-day Chao-1 input; single-day f1/f2 reconstruction for `2026-06-15` (f1=`6`, f2=`15`, s_obs=`373`) matches stored values exactly. No methodology doc supports "monthly accumulation"; framed as a window-choice concern.
- **ESG-C05** (high): energy mart inner `SELECT ... ORDER BY date DESC LIMIT 1` has no current-month filter; carbon twin has `WHERE toStartOfMonth(date) < toStartOfMonth(today())`. Latent today (fct max `2026-05-31`, no June rows); fires when June lands.
- **ESG-C06** (medium): `api_esg_carbon_timeseries_bands.sql` has no `WHERE` at all; MTD/7-day-MA windows bleed the current partial month. Part of a systemic daily/timeseries pattern (only the annualised carbon mart carries the guard).
- **ESG-C07** (medium): `carbon_intensity_std = sqrt(temporal^2 + measurement^2)` built from unadjusted base_ci while mean `*= seasonal_factor`. Quantified on XKX `2025-12` (factor 1.18): CoV `0.132` vs `0.156` at factor 1.0 — bands ~15% too tight.
- **ESG-C08** (low): all three node models `materialized='table'` yet retain `is_incremental()` guards (classification 2 blocks, client_distribution 3, geographic 1). Inert dead code; zero present-day impact (downgraded `medium -> low`).
- **ESG-C10** (low): fct config has no `order_by` (only `materialized`/`incremental_strategy`/`unique_key`/`partition_by`/`tags`); peers all specify one. Granule-pruning impact negligible on a `182`-row, one-row-per-date table (downgraded `medium -> low`).
- **ESG-C11** (low): `daily_power_data` CTE repeats the `date > MAX(date)` filter already in `node_country_distribution`. Zero present-day impact (identical date ranges); latent maintenance footgun (downgraded `medium -> low`).
- **ESG-C12** (medium): `intermediate/schema.yml` documents `8-9` phantom per-peer columns and attaches `unique`+`not_null` tests to non-existent `peer_id`. Compiled test SQL at `target/compiled/gnosis_dbt/.../{unique,not_null}_int_esg_node_classification_peer_id.sql` selects `peer_id` from the aggregated table -> `UNKNOWN_IDENTIFIER` (code 47) -> breaks CI (raised `low -> medium`).
- **ESG-C13** (low): `staging/schema.yml` ember entry lists both real mixed-case (`Area`/`Date`/`Value`) and phantom lowercase aliases (`area`/`date`/`value`). Lowercase set carries no tests -> purely descriptive doc drift.
- **ESG-C15** (high): energy mart (unfiltered LIMIT 1) and carbon mart (current-month-excluded) use mismatched windows; coincide today (both as_of `2026-05-31`, month boundary) by calendar accident. Implied CI `176.6` g/kWh vs fct `effective_carbon_intensity` `172.25` g/kWh.
- **ESG-C16** (high): zero-CI rows contribute exactly `0` CO2 (CI=0 forces `daily_co2_kg_mean=0`); `carbon_intensity_source` exists only in `int_esg_dynamic_power_consumption`, dropped before every api mart -> no downstream flag. Bias growing `9.68% (Dec25) -> 25.27% (May26)`.
- **ESG-C17** (high): Chao-1 from single-day input; 30-day accumulated Chao-1 ending `2026-06-10` = `2701` vs single-day `enhanced_total_reachable` `1148` (~`2.3x` understatement), consistent across dates. Energy/CO2 scale linearly with node population -> understated totals.
- **ESG-C18** (medium): `195/195` `quality_tier: candidate` in `semantic/authoring/ESG/semantic_models.yml`; marts tagged `tier0`/`tier1`. No external exposure registry reachable to prove unauthenticated serving beyond the in-repo tags (held at medium).
- **ESG-C19** (medium): same code locus as C07, business framing. XKX `2025-12` band half-width `292.25` = `1.96*sqrt(114.55^2+95.46^2)` from unadjusted base; unadjusted std propagates to published `fct.effective_carbon_intensity_lower_95/upper_95` without re-derivation.

### NEW (0)
None.

### UNVERIFIABLE / UNRESOLVED (0)
None. All 21 cases reached the >=3-round minimum with self-consistent evidence and settled.

## Evidence appendix

### ESG-C01 — partition eviction (CI ensemble)
SQL: `SELECT toStartOfMonth(month_date) AS m, toString(max(calculated_at)) AS calc_at FROM dbt.int_esg_carbon_intensity_ensemble WHERE month_date >= '2023-12-01' GROUP BY m ORDER BY m DESC`
Returned: `4` distinct months — `2023-12-01` (calc `2026-03-27 14:21`), `2024-12-01` (calc `2026-06-04 10:21`), `2025-12-01` (calc `2026-06-21 06:04`), `2026-05-01` (calc `2026-06-21 15:13`); max `month_date=2026-05-01`. Full history: pre-2023-12 all 12 months/yr present (~85-88 countries); 2024 holds only Dec, 2025 only Dec (81 ctys), 2026 only May (42 ctys). `partition_by='toStartOfYear(month_date)'` still in `.sql` L4.

### ESG-C02 — broken semantic measures
Tool: `describe_table dbt.fct_esg_carbon_footprint_uncertainty` vs measure exprs in `semantic/authoring/ESG/semantic_models.yml`.
Returned: live cols `effective_carbon_intensity`, `effective_carbon_intensity_lower_95/upper_95`, `estimated_nodes`, `nodes_lower_95/upper_95`, `baseline_observed_nodes`, `chao1_total_estimated`, `network_reachability_pct`, `discovery_completeness_pct`, `node_categories_active`, `countries_with_nodes`. `13` measure exprs reference non-existent columns: `network_weighted_cif`, `network_cif_std`, `network_carbon_intensity_lower_95`, `network_carbon_intensity_upper_95`, `total_estimated_nodes`, `total_nodes_lower_95`, `total_nodes_upper_95`, `chao1_observed`, `chao1_estimated`, `chao1_success_rate`, `chao1_coverage`, `active_categories`, `max_countries_in_category`. `explain_metric_query(['network_weighted_cif_value'])` returned `manifest_hash_mismatch` (registry stale), so breakage demonstrated at compile level via the missing fact columns. schema.yml L351/L367 carry the same stale names.

### ESG-C03 / ESG-C16 — zero-CI fallback (shared query)
SQL: `SELECT toStartOfMonth(date) m, count(*), countIf(carbon_intensity_gco2_kwh=0.0), countIf(carbon_intensity_source='country_specific'), countIf(carbon_intensity_source='world_average') FROM dbt.int_esg_dynamic_power_consumption GROUP BY m ORDER BY m`
Returned: zero-CI rows by month `159, 153, 131, 160, 245, 373` (Dec25..May26); all rows `country_specific`, `world_average=0` every month. Total `1221/8970 = 13.61%`. `sumIf(daily_co2_kg_mean, carbon_intensity_gco2_kwh=0.0)=0` of total `65,715.25` kg. Zero-CI share `9.68% (Dec25) -> 25.27% (May26)`. Model has no `join_use_nulls` (config sets only `allow_nullable_key=1`); malformed `-- nu;;s ghet repl;ace by date...` at L176. carbon_intensity_source CASE (L160-180): `country_specific` WHEN `ci_country.carbon_intensity_mean IS NOT NULL`; unmatched LEFT JOIN returns CH-default `0.0` (passes IS NOT NULL), so COALESCE lands on `0.0` not `450`. `carbon_intensity_source` absent from fct and all api marts.

### ESG-C04 / ESG-C17 — Chao-1 single-day input
SQL: single-day reconstruction from `int_p2p_discv5_peers` for `2026-06-15` (GROUP BY peer_id, COUNT(DISTINCT crawl_id)) vs stored chao1 row; 30-day window recompute.
Returned: `2026-06-15` f1=`6`, f2=`15`, s_obs=`373` == stored `successful_singletons=6`, `successful_doubletons=15`, `observed_successful_nodes=373`. chao1 table = one row per `observation_date` (448 rows = 448 days). 30-day window ending `2026-06-10`: s_obs=`2542`, f1=`422`, f2=`559`, Chao-1=`2701` vs single-day `enhanced_total_reachable=1148` (~`2.3x`); single-day at `2026-05-31`=`1320`. `incremental_strategy='delete+insert'` in `.sql`. `search_docs` returned the model spec describing daily/hourly aggregation, NOT monthly accumulation.

### ESG-C05 / ESG-C15 — annualised mart asymmetry
SQL: `SELECT 'energy', annual_energy_Mwh_projected, as_of_date FROM dbt.api_esg_energy_consumption_annualised_latest UNION ALL SELECT 'carbon', annual_co2_tonnes_projected, as_of_date FROM dbt.api_esg_carbon_emissions_annualised_latest`; `SELECT date, effective_carbon_intensity FROM dbt.fct_esg_carbon_footprint_uncertainty ORDER BY date DESC LIMIT 1`.
Returned: energy `708.13` MWh as_of `2026-05-31`; carbon `125.05` t as_of `2026-05-31`. fct max date `2026-05-31`, `effective_carbon_intensity=172.25` g/kWh. Implied CI `125.05/708.13*1000 = 176.6` g/kWh. Code: energy mart inner SELECT no WHERE; carbon mart `WHERE toStartOfMonth(date) < toStartOfMonth(today())`. Both coincide only at the month boundary; energy mart picks the partial-June row the instant one lands.

### ESG-C06 — bands partial-month
Read `models/ESG/marts/api_esg_carbon_timeseries_bands.sql`: final `SELECT ... FROM {{ ref('fct_esg_carbon_footprint_uncertainty') }}` with NO `WHERE`. `mtd_avg`/`mtd_total` partitioned by `toStartOfMonth(date)`, plus 7-row MA. Sibling `api_esg_carbon_emissions_daily`/`api_esg_estimated_nodes_daily` also lack the guard; only `*_annualised_latest` carbon mart has it.

### ESG-C07 / ESG-C19 — seasonal std not scaled
SQL: `SELECT base_carbon_intensity, carbon_intensity_mean, seasonal_adjustment, temporal_std, measurement_std, ci_upper_95, coefficient_of_variation FROM dbt.int_esg_carbon_intensity_ensemble WHERE seasonal_adjustment=1.18 AND base_carbon_intensity>100`
Returned: XKX `2025-12` (factor 1.18): base_ci=`954.55`, mean=`1126.37` (ratio 1.18), temporal_std=`114.55` (=`954.55*0.12`, unadjusted), measurement_std=`95.46` (=`954.55*0.10`, unadjusted), ci_upper_95=`1418.62`, half-width=`292.25` = `1.96*sqrt(114.55^2+95.46^2)`, CoV=`0.132` vs `0.156` at factor 1.0. Propagation: ensemble `carbon_intensity_std` -> `int_esg_dynamic_power_consumption.carbon_intensity_std_gco2_kwh` (COALESCE forward, no re-scale) -> fct `effective_carbon_intensity_lower_95/upper_95`. No downstream re-derivation from the elevated mean.

### ESG-C08 — dead is_incremental guards
grep: `int_esg_node_classification.sql` L3 `materialized='table'`, `is_incremental()` at L27/L99; `int_esg_node_client_distribution.sql` L3 table, `is_incremental()` at L24/L40/L55; `int_esg_node_geographic_distribution.sql` L3 table, `is_incremental()` at L21. `is_incremental()` returns false under table materialization -> inert.

### ESG-C09 — missing NULLIF
SQL: `SELECT min(observed_nodes), countIf(observed_nodes=0), min(estimated_total_nodes), countIf(estimated_total_nodes=0), count(*) FROM dbt.int_esg_node_classification`
Returned: `min(observed_nodes)=1`, `0` rows at 0; `min(estimated_total_nodes)=2`, `0` rows at 0; `1792` rows. L187: `round(100.0 * (s.estimated_total_nodes - s.observed_nodes) / s.estimated_total_nodes, 2)` — no `nullIf`. Denominator = `greatest(observed_nodes, scaled) >= 1`, so NaN/inf unreachable. Latent defensive-only.

### ESG-C10 — missing order_by
Read fct config L2-9: `materialized='incremental'`, `incremental_strategy='delete+insert'`, `unique_key='date'`, `partition_by='toStartOfMonth(date)'`, `tags=[...]` — no `order_by`. `system.*`/`SHOW CREATE` blocked by read-only query guard. Peers specify `order_by` (ensemble `(month_date,country_code)`). Table is `182` rows, one per date.

### ESG-C11 — redundant filter
Read fct: `node_country_distribution` CTE `{% if is_incremental() %} WHERE date > (SELECT MAX(date) FROM this)`; `daily_power_data` reads `node_country_distribution` and repeats the same filter. fct `182` rows, contiguous `2025-12-01`..`2026-05-31` — no rows dropped today.

### ESG-C12 — phantom test-bearing peer_id
Read `intermediate/schema.yml` int_esg_node_classification (L231+): documents `observation_date`, `peer_id` (unique+not_null tests L242-244), `ip_address`, `client_type`, `country_code`, `generic_provider`, `peer_org`, `last_seen_that_day`. `describe_table dbt.int_esg_node_classification` actual cols: `date, node_category, observed_nodes, estimated_total_nodes, nodes_lower_95, nodes_upper_95, avg_confidence, sample_coverage, scaling_factor, category_percentage, hidden_nodes_estimated, hidden_nodes_percentage, geographic_distribution, top_countries, calculated_at` (no peer_id). Compiled `unique_int_esg_node_classification_peer_id.sql`: `select peer_id as unique_field ... from dbt.int_esg_node_classification` -> `UNKNOWN_IDENTIFIER` (code 47). Model enabled, in DAG, feeds 4 downstream models.

### ESG-C13 — phantom ember aliases
Read `staging/schema.yml` ember entry (L54+): mixed-case `Area` (L58), `Date` (L66), `Value` (L121) AND lowercase `date` (L129), `value` (L133), `area` (L173). `describe_table dbt.stg_crawlers_data__ember_electricity_data`: only mixed-case pass-through (`Area`, `ISO 3 code`, `Date`, `Continent`, ..., `Value`). Lowercase set carries no tests; only real `Date` has a not_null test.

### ESG-C14 — fct fallback substitution
SQL: `SELECT count(*), min(date), max(date), count(DISTINCT toStartOfMonth(date)) FROM dbt.fct_esg_carbon_footprint_uncertainty`
Returned: `182` rows, `2025-12-01`..`2026-05-31`, 6 months, 182 distinct days. Per ESG-C03 source breakdown: all 6 months `country_specific=fullcount`, `world_average=0`; zero-CI `159 -> 373` (Dec25->May26). fct has no `calculated_at` column; CI ensemble 2026 partition rewritten today evicted Jan-Apr -> those fct rows are stale snapshots not refreshed against the evicted CI (delete+insert re-pulls only `date>MAX`).

### ESG-C18 — quality_tier vs exposure
grep: `195` occurrences of `quality_tier: candidate`, `0` approved/production in `semantic/authoring/ESG/semantic_models.yml`. Mart configs tagged `tier0` (`api_esg_energy_consumption_annualised_latest`, `api_esg_carbon_emissions_annualised_latest`), `tier1` (`api_esg_carbon_timeseries_bands`), with `api:*` endpoint tags. No `exposures.yml`/external registry reachable from repo or warehouse.

### ESG-C20 — Ember coverage decay
SQL: `SELECT toStartOfMonth("Date") m, count(DISTINCT "ISO 3 code") FROM dbt.stg_crawlers_data__ember_electricity_data WHERE "Unit"='gCO2/kWh' AND "Value">0 AND "ISO 3 code"!='' GROUP BY m ORDER BY m DESC`
Returned: `2026-05`=`41`, `2026-04`=`57`, `2026-03`=`72`, `2026-02`=`74`, `2026-01`=`75`, `2025-12`=`80`; historical `~79-80`. Max month advanced from April (baseline) to May 2026. Countries dropping out (CA, IN, AR, BR, RU, TH, TW, JP, SG) appear in the May-2026 zero-CI list per ESG-C03.

### ESG-C21 — geographic liveness + figures
SQL: `SELECT count(*), min(date), max(date) FROM dbt.int_esg_node_geographic_distribution`; annualised marts; CI ensemble `max(calculated_at)`.
Returned: `23,366` rows, `2025-03-27`..`2026-06-20`, 448 days (live). Energy `708.13` MWh, CO2 `125.05` t (both as_of `2026-05-31`). CI ensemble `max(calculated_at)` = epoch `1782054829` = `2026-06-21` (today). Implied CI `298 -> 177` g/kWh, explained by rising zero-CI share (C16). Baseline: `22,824` rows to `2026-06-08`, energy `512.94`, CO2 `153`.

## Review log (>=3 rounds per case)

- **ESG-C01**: R1 CONFIRMED (partition_by unchanged, 4 months) -> challenge: prove ongoing per-run eviction via partition row counts / calculated_at -> R2 CONFIRMED (GROUP BY shows 2026 partition holds only May, calc=today; system.parts blocked) -> challenge: quote per-row calculated_at for 2026-05 vs 2024-12/2025-12 -> R3 CONFIRMED (2026-05 calc `2026-06-21 15:13` vs older year-partition writes). Settled critical.
- **ESG-C02**: R1 CONFIRMED (2 measures broken) -> challenge: cross-check ALL exprs -> R2 CONFIRMED (`13` broken measures) -> challenge: demonstrate live query failure -> R3 CONFIRMED (explain_metric_query hit manifest_hash_mismatch; breakage shown via missing fact columns -> UNKNOWN_IDENTIFIER). Settled critical.
- **ESG-C03**: R1 CONFIRMED (159/1642 Dec, no join_use_nulls) -> challenge: prove CH-default zeros not genuine ~0 grids -> R2 CONFIRMED (all Dec rows `country_specific`, world_average=0) -> challenge: quote CASE + L176 WHERE -> R3 CONFIRMED (world_average=0 across all 8970 rows). Settled high.
- **ESG-C04**: R1 CONFIRMED (delete+insert, day-grain) -> challenge: reconstruct single-day f1/f2 + quote doc -> R2 CONFIRMED (exact match `2026-06-15`) -> challenge: quote methodology doc -> R3 CONFIRMED (spec describes daily/hourly, not monthly; rests on reconstruction; severity tempered to window-choice). Settled high (medium confidence).
- **ESG-C05**: R1 CONFIRMED (no current-month filter vs carbon twin) -> challenge: prove latent-but-real, fires mid-month -> R2 CONFIRMED (fct max `2026-05-31`, both as_of coincide) -> challenge: demonstrate divergence deterministically -> R3 CONFIRMED (code asymmetry unambiguous). Settled high.
- **ESG-C06**: R1 CONFIRMED (no WHERE) -> challenge: check sibling daily marts -> R2 CONFIRMED (systemic; only annualised carbon mart guarded) -> challenge: quote sibling WHERE clauses -> R3 CONFIRMED (bands no WHERE; siblings lack guard). Settled medium.
- **ESG-C07**: R1 CONFIRMED (std from unadjusted base) -> challenge: quantify CoV on winter row -> R2 CONFIRMED (XKX CoV 0.132 vs 0.156) -> challenge: confirm not cancelled downstream -> R3 CONFIRMED (unadjusted std propagates to fct). Settled medium.
- **ESG-C08**: R1 CONFIRMED (3 table models with guards) -> challenge: severity + full-rebuild check -> R2 CONFIRMED, severity medium->low (inert, contiguous history) -> R3 CONFIRMED (grep re-confirmed). Settled low.
- **ESG-C09**: R1 CONFIRMED (no NULLIF L187) -> challenge: NaN/inf reachability -> R2 CHANGED, severity low (min denom=2, unreachable) -> R3 (verifier relabelled medium; orchestrator settled CHANGED + low, latent defensive-only). Settled CHANGED/low.
- **ESG-C10**: R1 CONFIRMED (no order_by) -> challenge: read live ORDER BY -> R2 CONFIRMED, severity low (system.* blocked; small table) -> R3 (verifier relabelled medium; orchestrator held low, negligible pruning). Settled low.
- **ESG-C11**: R1 CONFIRMED (duplicate filter) -> challenge: confirm zero current impact -> R2 CONFIRMED, severity low (identical date ranges) -> R3 (verifier relabelled medium; orchestrator held low, latent footgun). Settled low.
- **ESG-C12**: R1 CONFIRMED (phantom per-peer cols) -> challenge: escalate? do tests error in CI? -> R2 CONFIRMED, severity low->medium (peer_id has unique+not_null on missing col) -> challenge: confirm compiled test SQL -> R3 CONFIRMED (compiled SQL selects peer_id -> UNKNOWN_IDENTIFIER). Settled medium.
- **ESG-C13**: R1 CONFIRMED (phantom lowercase aliases) -> challenge: do they carry tests? -> R2 CONFIRMED (no tests, doc drift) -> R3 CONFIRMED (re-measured). Settled low.
- **ESG-C14**: R1 CHANGED (31->182 rows, INNER-zero symptom resolved) -> challenge: quantify Jan-Apr fallback share -> R2 CHANGED (all months country_specific via CH-default 0.0, not world/450) -> challenge: prove stale-snapshot via calculated_at -> R3 CHANGED (fct has no calculated_at; proven via CI ensemble eviction timing). Settled CHANGED/high.
- **ESG-C15**: R1 CONFIRMED (mismatched windows) -> challenge: compute implied CI vs fct -> R2 CONFIRMED (176.6 g/kWh) -> challenge: compare to fct effective_carbon_intensity -> R3 CONFIRMED (176.6 vs 172.25, coincide at boundary). Settled high.
- **ESG-C16**: R1 CONFIRMED (zero-CI -> 0 CO2) -> challenge: prove uncommunicated + trend -> R2 CONFIRMED (source dropped before marts; 9.68%->25.27%) -> R3 CONFIRMED (zero_frac 0.1361). Settled high.
- **ESG-C17**: R1 CONFIRMED (per-day input) -> challenge: size single-day vs 30-day -> R2 CONFIRMED (1210 vs 2663, ~2.2x) -> challenge: extend to 2-3 dates -> R3 CONFIRMED (~2.3x across dates). Settled high.
- **ESG-C18**: R1 CONFIRMED (all candidate) -> challenge: confirm exposure side + count -> R2 CONFIRMED (17 models, tier0/tier1 tags) -> challenge: check exposures.yml -> R3 CONFIRMED (195/195 candidate; no external registry reachable). Settled medium.
- **ESG-C19**: R1 CONFIRMED (bands not scaled) -> challenge: quote one-row numbers -> R2 CONFIRMED (XKX half-width 292.25 unadjusted) -> challenge: trace to published bands -> R3 CONFIRMED (propagates to fct lower_95/upper_95). Settled medium.
- **ESG-C20**: R1 CHANGED (Apr->May, 42 ctys) -> challenge: trace downstream consequence -> R2 CHANGED (named dropped countries flip to zero-CI) -> R3 CONFIRMED/CHANGED (41 ctys May26; orchestrator settled CHANGED). Settled CHANGED/low.
- **ESG-C21**: R1 CHANGED (liveness holds, figures moved) -> challenge: confirm CI freshness + reconcile CO2-down/energy-up -> R2 CONFIRMED (calc=today; implied CI 298->177) -> challenge: reconcile status label -> R3 CHANGED (liveness CONFIRMED but figures moved -> CHANGED governs). Settled CHANGED/high.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P0 ESCALATE | Replace `partition_by='toStartOfYear(month_date)'` with `toStartOfMonth(month_date)` (or drop insert_overwrite to delete+insert at month grain) so each month survives instead of being evicted; backfill the lost 11/12 months per year. This drives C14/C16/C20/C21. | `models/ESG/intermediate/int_esg_carbon_intensity_ensemble.sql` |
| P0 ESCALATE | Fix the 13 stale semantic measure exprs to the live aliased fact columns (`network_weighted_cif -> effective_carbon_intensity`, `total_estimated_nodes -> estimated_nodes`, `chao1_observed -> baseline_observed_nodes`, etc.); reconcile schema.yml L351/L367. | `semantic/authoring/ESG/semantic_models.yml`, `models/ESG/marts/fct_esg_carbon_footprint_uncertainty.sql`, `models/ESG/marts/schema.yml` |
| P1 KEEP | Add `pre_hook`/`post_hook` `SET join_use_nulls=1` (per project convention) so unmatched CI LEFT JOINs return NULL not `0.0`; fix the malformed `WHERE` at L176 so the 450 world fallback fires. Removes the `13.61%` zero-CI bias. | `models/ESG/intermediate/int_esg_dynamic_power_consumption.sql` |
| P1 KEEP | Add `WHERE toStartOfMonth(date) < toStartOfMonth(today())` to the energy annualised mart inner SELECT (match the carbon twin) to stop the partial-month drift and restore cross-mart coherence (C05/C15). | `models/ESG/marts/api_esg_energy_consumption_annualised_latest.sql` |
| P1 KEEP | Recompute Chao-1 over a trailing-window accumulation rather than single-day input (delete+insert day watermark); ~`2.3x` understatement of node population -> energy/CO2 (C04/C17). | `models/ESG/intermediate/int_esg_node_population_chao1.sql` |
| P2 KEEP | Scale `carbon_intensity_std` (and CI bands) by `seasonal_factor` so winter uncertainty widens with the elevated mean; removes false precision (C07/C19). | `models/ESG/intermediate/int_esg_carbon_intensity_ensemble.sql` |
| P2 KEEP | Surface `carbon_intensity_source` (and a world-avg/zero-CI affected flag) in the published api marts so the downward bias and source decay are disclosed (C16/C20). | `models/ESG/marts/*`, `models/ESG/intermediate/int_esg_dynamic_power_consumption.sql` |
| P2 KEEP | Add the partial-month exclusion to `api_esg_carbon_timeseries_bands` and sibling daily marts (C06). | `models/ESG/marts/api_esg_carbon_timeseries_bands.sql`, `api_esg_carbon_emissions_daily.sql`, `api_esg_estimated_nodes_daily.sql` |
| P2 KEEP | Fix `intermediate/schema.yml` to document the aggregated output columns and remove the `unique`/`not_null` tests on phantom `peer_id` (breaks CI, code 47) (C12). | `models/ESG/intermediate/schema.yml` |
| P3 KEEP | Reconcile ESG `quality_tier: candidate` with tier0/tier1 public exposure (promote or gate the endpoints) (C18). | `semantic/authoring/ESG/semantic_models.yml` |
| P3 KEEP | Dev-hygiene cleanups: add `order_by=['date']` to fct (C10); remove the redundant `date>MAX` filter in `daily_power_data` (C11); strip dead `is_incremental()` guards from the 3 table models (C08); remove phantom lowercase ember aliases (C13). All low. | `fct_esg_carbon_footprint_uncertainty.sql`, `int_esg_node_classification.sql`, `int_esg_node_client_distribution.sql`, `int_esg_node_geographic_distribution.sql`, `staging/schema.yml` |
| P3 KEEP (defensive) | Add `nullIf(s.estimated_total_nodes, 0)` to L187 — latent defensive-only (NaN/inf unreachable today) (C09). | `models/ESG/intermediate/int_esg_node_classification.sql` |
| - DROP | No DROP recommendations — zero cases resolved. | - |

Note: ESG-C01 is explicitly NOT the June `insert_overwrite` wipe incident — it is a standalone `toStartOfYear` partition-grain design defect that evicts 11/12 months per year on every run.
