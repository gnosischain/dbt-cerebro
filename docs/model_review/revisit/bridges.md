# Model review (revisit 2026-06-21): bridges

Baseline `docs/model_review/bridges.md` (dated 2026-06-11), re-verified 2026-06-21 across 3 rounds; 17 cases re-checked — `1` RESOLVED (freshness alert proven to exist via source-level inheritance), `3` CHANGED (phantom-`d` MetricFlow scope, allowlist hygiene, one quality-tier promotion), `13` STILL CONFIRMED including the two high-severity items (signed-negative USD documented as `UInt64`, and the phantom-`d`/`DateTime`-vs-`Date` schema drift across 5 marts).

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| BRIDGES-C01 | — | schema.yml `UInt64` vs live `Float64`; signed-negative USD in net/value cols | high | CONFIRMED | high | high | none | 3 |
| BRIDGES-C02 | — | `bridges_kpis_snapshot` defaults `agg_time_dimension: d`, no `d` column | high | CHANGED | medium | high | none | 3 |
| BRIDGES-C03 | — | phantom column `d` in 5 marts + `as_of_date` `DateTime` vs live `Date` | high | CONFIRMED | high | high | none | 3 |
| BRIDGES-C04 | — | `dune_bridge_flows` no `warn_after`/`error_after`; 4-day stale, no alert | medium | RESOLVED | low | high | none | 3 |
| BRIDGES-C05 | — | `chg_vol_7d`/`chg_net_7d` raw fraction served as `change_pct` ("percentage") | medium | CONFIRMED | medium | high | none | 3 |
| BRIDGES-C06 | — | `int_bridges_flows_daily_v2` dev-tagged, unwired, empty semantic model | medium | CONFIRMED | medium | high | none | 3 |
| BRIDGES-C07 | — | no `row_count=1`/`not_null(as_of_date)` test on single-row snapshot | medium | CONFIRMED | low | high | none | 3 |
| BRIDGES-C08 | — | `ReplacingMergeTree` read without `FINAL` downstream | medium | CONFIRMED | low | high | none | 3 |
| BRIDGES-C09 | — | `coalesce(w.netflow_usd_week, 0)` on LEFT JOIN, no `join_use_nulls` hook | medium | CONFIRMED | low | high | none | 3 |
| BRIDGES-C10 | — | `api_bridges_cum_netflow_weekly_by_bridge.date` untyped + allowlisted | low | CHANGED | low | high | none | 3 |
| BRIDGES-C11 | — | `range_order` in GROUP BY but not SELECT (both Sankey range models) | low | CONFIRMED | low | high | none | 3 |
| BRIDGES-C12 | — | all semantic models `quality_tier: candidate`; none approved | high | CHANGED | medium | high | none | 3 |
| BRIDGES-C13 | — | `volume_usd` aggregated with no direction filter (xchain unfiltered) | medium | CONFIRMED | low | high | none | 3 |
| BRIDGES-C14 | — | single-row full-rebuild snapshot; no historical KPI trend retained | medium | CONFIRMED | low | high | none | 3 |
| BRIDGES-C15 | — | `volume_usd = CAST(NULL AS Nullable(Float64))` feeds live sum measure + graph weight | medium | CONFIRMED | medium | high | none | 3 |
| BRIDGES-C16 | — | dual semantic models for one fact (fct + api `All`-rollup view) | low | CONFIRMED | low | high | none | 3 |
| BRIDGES-C17 | P0-16 | bridges USD sourced only from Dune; no Chainlink fallback / SLA | low | CONFIRMED | low | high | none | 3 |

Final distribution: high=`2` (C01, C03), medium=`5` (C02, C05, C06, C12, C15), low=`9`, resolved=`1` (C04). Statuses: CONFIRMED=`13`, CHANGED=`3` (C02, C10, C12), RESOLVED=`1` (C04). No incident attribution on any case; no NEW cases.

## Delta vs baseline

### RESOLVED (1)
- **BRIDGES-C04** — the baseline premise "no `warn_after`/`error_after` => no automated alert + 4 days stale" is now false on both counts. `models/crawlers_data/sources.yml` has a SOURCE-level freshness default (`warn_after 18h` / `error_after 30h`, lines ~6-8); `dune_bridge_flows` (lines 24-25) sets only `loaded_at_field: timestamp` with no table override, so it **inherits** the `18h/30h` thresholds. Live `max(timestamp)=2026-06-21`, `hours_since=21h` — already past the `18h` warn (so the inherited alert demonstrably fires on normal D-1 morning lag), well within the `30h` error. Data is current to `2026-06-21` (baseline claimed stale at `2026-06-07`). Residual is purely cosmetic: `18h` is slightly tight for a daily D-1 cadence and may warn on benign morning lag. Severity dropped `medium -> low`, status RESOLVED.

### CHANGED (3)
- **BRIDGES-C02** — baseline claim "every MetricFlow time-series query fails" is overstated. `semantic/authoring/bridges/semantic_models.yml` declares both the phantom default `agg_time_dimension: d` (line 175) / `d` time dimension (lines 177-181) AND a valid `as_of_date` time dimension (lines 182-186). Live `fct_bridges_kpis_snapshot` has `17` columns (`as_of_date` `Date` + 16 metrics), no `d`. The break is scoped to the default/`metric_time`-keyed path (binds to non-existent `d`); an explicit `as_of_date`-keyed query resolves. Severity `high -> medium`.
- **BRIDGES-C10** — the schema.yml gap is fixed: `api_bridges_cum_netflow_weekly_by_bridge.date` now carries `data_type: Date` (schema.yml lines 8-10). The `api_bridges_cum_netflow_weekly_by_bridge::no_grain_col` allowlist line in `scripts/checks/check_api_tags.allow` persists, but `check_api_tags.py` keys `no_grain_col` on column NAME (`GRAIN_COL['weekly']={'week'}`) and the endpoint renames `week -> date`, so the allowlist line is the project-standard resolution (same precedent as `api_execution_cow_top_pairs_weekly::no_grain_col`, which carries an explicit comment that renaming would break the metrics-dashboard widget keyed on `xField:date`). Actionable part resolved; only the by-design allowlist remains. Severity stays low.
- **BRIDGES-C12** — baseline "none promoted to approved" is now false in YAML: the metric `bridge_netflow_weekly_by_bridge` (semantic_models.yml line 774) carries `quality_tier: approved`; all semantic_models (incl. line 267) and the other ~40 metrics remain `candidate`. BUT the live registry still rejects it: `get_metric_details('bridge_netflow_weekly_by_bridge')` returns "exists, but it is not approved for semantic execution yet", and `discover_metrics('bridges token netflow by bridge')` surfaced ZERO bridge metrics (only approved execution metrics — `cow_top_pairs_volume`, gpay topups/actions). So the consumer concern "no trustworthy/queryable bridge metric via MCP" effectively still holds. Severity `high -> medium`.

### STILL CONFIRMED (13)
- **BRIDGES-C01** (high) — `int_bridges_flows_daily` live types `net_usd`/`volume_usd`/`volume_token` all `Float64` while schema.yml lines 52/59/66 declare `UInt64`; downstream `netflow_usd_week` (`Float64`, schema `UInt64`) and `value` (`Float64`, schema `UInt64`) likewise. Negatives are real and serving-impacting: `fct_bridges_token_netflow_daily_by_bridge` `7,550/19,148` (39%) negative, min `-9,843,904.83`; `fct_bridges_netflow_weekly_by_bridge` `312/1,024` (30%) negative, min `-11,577,460.73`; on the API view `api_bridges_token_netflow_daily_by_bridge` `11,840/29,773` (39.8%) negative, and the `All` UNION ALL rollup itself goes negative on `4,290` rows. No `config(contract.enforced)` anywhere in the unit (grep over the 3 models, both schema.yml files, and `dbt_project.yml` found none), so the `UInt64` declaration can never trigger a build failure — pure documentation drift, but it documents a real defect (`UInt64` cannot represent the negatives CH stores as `Float64`). Fix is a schema.yml correction `UInt64 -> Float64`.
- **BRIDGES-C03** (high) — phantom column `d` still declared in `models/bridges/marts/schema.yml` for `fct_bridges_kpis_snapshot` (line 417), `api_bridges_sankey_gnosis_in_by_token_7d` (202), `api_bridges_sankey_gnosis_out_by_token_7d` (270), `api_bridges_sankey_gnosis_in_ranges` (236), `api_bridges_sankey_gnosis_out_ranges` (304); none output `d` (SQL emits `mx.d AS as_of_date` / `max(date) AS as_of_date`). `as_of_date` declared `DateTime` (line 423) vs live `Date`. No CI guard compares schema.yml `data_type` to live CH types (`check_api_tags.py` lines 86-88 only enforce data_type PRESENCE). Blast radius is broader than schema.yml-only: `get_model_details('fct_bridges_kpis_snapshot')` (manifest/schema.yml-sourced) DID list `d (Date)` as column 1 and `as_of_date (DateTime)`, so a real MCP discovery consumer is misled; `describe_table` (live-catalog) correctly shows `17` cols, no `d`, `as_of_date=Date`.
- **BRIDGES-C05** (medium) — `chg_vol_7d`/`chg_net_7d` are raw decimal fractions: live `chg_vol_7d=-0.3758` (i.e. -37.58%), `chg_net_7d=-4.8435` (-484.35%). SQL computes `(cur-prev)/prev` with no `*100`; `api_bridges_kpi_volume_7d`/`api_bridges_kpi_netflow_7d` expose them as `change_pct` with no multiplication; schema.yml descriptions say "expressed as a percentage". No consumer compensates — no `dashboards/` or `grafana/` directory exists; `change_pct` appears only in dbt schema.yml + tests. Latent semantic mismatch; severity medium.
- **BRIDGES-C06** (medium) — `int_bridges_flows_daily_v2.sql` still `tags=['dev','intermediate','bridges','v2']` (line 8), companion semantic model `bridges_flows_daily_v2` has no measures/dimensions (lines 73-81), no production mart refs it. Build-and-fail proven: `stg_crawlers_data__dune_bridge_flows_v2.sql` SELECTs `date` and `txs` `FROM source('crawlers_data','dune_bridge_flows')`, but `describe_table crawlers_data.dune_bridge_flows` lists only `timestamp,bridge,source_chain,dest_chain,token,amount_token,amount_usd,net_usd` — no `date`, no `txs`. A dev run picking up v2 raises `UNKNOWN_IDENTIFIER` at query time.
- **BRIDGES-C07** (low) — `fct_bridges_kpis_snapshot` schema.yml entry (lines 412-495) has only `elementary.schema_changes` (warn); no `row_count`/`unique`/`not_null(as_of_date)`. SQL is `FROM mx, cum, cur7, prev7, bridges, chains` (CROSS JOIN of single-row sub-aggregate CTEs). Empty-input behavior corrected: CH `sum()`/`count()` over zero rows return one row, so an empty `int_bridges_flows_daily` yields exactly `1` all-zero/NULL snapshot row (not 0 rows). Failure mode is a misleading all-zero snapshot, not silent-empty; severity `medium -> low`.
- **BRIDGES-C08** (low) — `int_bridges_flows_daily` `engine=ReplacingMergeTree()` + `incremental_strategy=insert_overwrite` + `partition_by toStartOfMonth(date)`; downstream marts read it without `FINAL` (grep empty under `models/bridges`). Sole write path is the configured `insert_overwrite` (REPLACE PARTITION, atomic per month); no raw-append/`full_refresh` path in the model or in `scripts/` (grep for `int_bridges_flows_daily` returns nothing). Pre-merge duplicates cannot accumulate in normal ops; severity `medium -> low`.
- **BRIDGES-C09** (low) — `fct_bridges_netflow_weekly_by_bridge.sql` line 38 `coalesce(w.netflow_usd_week, 0)` on the grid LEFT JOIN, no `join_use_nulls` pre/post hook. Under default `join_use_nulls=0` unmatched grid cells already read 0, so coalesce is a no-op; convention deviation only. Severity `medium -> low`.
- **BRIDGES-C11** (low) — both `api_bridges_sankey_gnosis_in_ranges.sql` and `api_bridges_sankey_gnosis_out_ranges.sql` carry `r.range_order` in GROUP BY but not SELECT; `range_order` is a deterministic 1:1 constant per `range` literal (1D=1...All=5) in the ranges CTE, so no row fan-out. Cosmetic; low.
- **BRIDGES-C13** (low) — `fct_bridges_kpis_snapshot` aggregates `sum(volume_usd)` in cum/cur7/prev7 with no direction filter. Live `int_bridges_flows_daily` has `countIf(direction='xchain')=0` (directions `{'in','out'}` only); staging `stg_crawlers_data__dune_bridge_flows` also has 0 xchain over `360,351` rows (the CASE's `ELSE 'xchain'` never triggers). Latent doc gap, zero numeric impact today; severity `medium -> low`.
- **BRIDGES-C14** (low) — `fct_bridges_kpis_snapshot` is a single-row full-rebuild table (`count()=1`, `uniqExact(as_of_date)=1`, `max=2026-06-21`), `materialized='table'`, `partition_by toStartOfMonth(as_of_date)`; API views take `ORDER BY as_of_date DESC LIMIT 1`. No `_history`/trend model retains snapshot-KPI history. By-design snapshot; severity `medium -> low`.
- **BRIDGES-C15** (medium) — `int_execution_bridges_address_flows_daily.sql` line 50 `CAST(NULL AS Nullable(Float64)) AS volume_usd`. Live: `348,498/348,498` (100%) NULL since 2026-05-01, `sum(volume_usd)=NULL`. The semantic measure `bridges_address_flows_daily__volume_usd_value` (agg `sum`, line 532) and the graph profile `bridge_user_flows` `weight_column: volume_usd` (line 557) therefore serve NULL/0 — every (address->bridge) edge weight is NULL. Documented placeholder ("currently NULL — placeholder for when whitelisted_daily carries USD"); upstream `int_execution_transfers_whitelisted_daily` has no priced USD column, so it is a structural pricing gap. `transfer_count` (`sum=220,344,649`) is a usable fallback weight. Severity medium.
- **BRIDGES-C16** (low) — `semantic_models.yml` defines both `bridges_token_netflow_daily_by_bridge` (ref `api_...`, line 440) and `fct_bridges_token_netflow_daily_by_bridge` (ref `fct_...`, line 471), each `sum(value)` at date/bridge/token grain, sharing the synonym "bridges token netflow daily by bridge" (lines 469, 497). No live routing collision today: both are `candidate` and `discover_metrics('bridges token netflow by bridge')` surfaced NEITHER. Definition-level ambiguity only; low. Would become a real collision if both are promoted to approved.
- **BRIDGES-C17** (P0-16, low) — `int_bridges_flows_daily.sql` derives USD purely from Dune (`sum(amount_usd) AS volume_usd`, `sum(net_usd) AS net_usd` from `stg_crawlers_data__dune_bridge_flows`); no Chainlink/native-price join. Dune coverage is currently full (over last 30d: `3,434` rows, `0` with `amount_usd` NULL/0 while `amount_token!=0`). The `project_native_prices_chainlink` plan is unapplied to bridges; gap is purely forward-looking. Low.

### NEW (0)
None.

### UNVERIFIABLE / UNRESOLVED (0)
None. All 17 cases reached >=3 rounds of evidence and were ruled `all_sufficient` in round 3.

## Evidence appendix

**BRIDGES-C01** — `describe_table` on the three models confirms live `Float64` for `net_usd`/`volume_usd`/`volume_token`/`netflow_usd_week`/`value` (schema.yml declares `UInt64`).
```sql
SELECT count(), countIf(value<0), min(value) FROM dbt.fct_bridges_token_netflow_daily_by_bridge;
-- 19,148 ; 7,550 (39%) ; -9,843,904.83
SELECT count(), countIf(netflow_usd_week<0), min(netflow_usd_week) FROM dbt.fct_bridges_netflow_weekly_by_bridge;
-- 1,024 ; 312 (30%) ; -11,577,460.73
SELECT countIf(value<0), count(), min(value), countIf(bridge='All' AND value<0), min(if(bridge='All',value,NULL))
FROM dbt.api_bridges_token_netflow_daily_by_bridge;
-- 11,840 ; 29,773 (39.8%) ; -9,843,904.83 ; 4,290 ; -9,843,904.83
```
Contract check: grep for `contract` across the 3 model SQLs, both bridges schema.yml, and `dbt_project.yml` — no `config(contract.enforced: true)`; only hit is the word "contract" in a column description.

**BRIDGES-C02 / BRIDGES-C03** — `describe_table fct_bridges_kpis_snapshot` returns `17` columns: `as_of_date (Date)` + 16 metric cols, no `d`. `semantic_models.yml bridges_kpis_snapshot`: `defaults.agg_time_dimension: d` (line 175), dimension `d` type:time (177-181), `as_of_date` type:time (182-186); 14 KPI metrics list `allowed_dimensions: [d, as_of_date]` (lines ~917-1316). `get_model_details('fct_bridges_kpis_snapshot')` (manifest-sourced) listed phantom `d (Date)` as col 1 and `as_of_date (DateTime)`. Phantom `d` in marts schema.yml at lines 202, 236, 270, 304, 417; `as_of_date data_type: DateTime` at line 423.

**BRIDGES-C04** —
```sql
SELECT max(timestamp), dateDiff('hour', max(timestamp), now()) FROM crawlers_data.dune_bridge_flows;
-- 2026-06-21 ; 21 (hours)
SELECT max(date) FROM dbt.int_bridges_flows_daily;  -- 2026-06-21
```
`models/crawlers_data/sources.yml`: source-level freshness `warn_after 18h` / `error_after 30h` (lines ~6-8); `dune_bridge_flows` (lines 24-25) only `loaded_at_field: timestamp`, no table override -> inherits. `21h > 18h` warn (alert fires), `< 30h` error.

**BRIDGES-C05** —
```sql
SELECT round(chg_vol_7d,4), round(chg_net_7d,4) FROM dbt.fct_bridges_kpis_snapshot;
-- -0.3758 ; -4.8435
```
`api_bridges_kpi_volume_7d.sql`/`api_bridges_kpi_netflow_7d.sql` expose `chg_vol_7d AS change_pct` / `chg_net_7d AS change_pct` with no `*100`. grep `change_pct` across `*.json/*.yml/*.ts/*.tsx/*.py` -> only dbt schema.yml descriptions + `tests/test_semantic_registry.py`; no `dashboards/`/`grafana/` dir.

**BRIDGES-C06** — `stg_crawlers_data__dune_bridge_flows_v2.sql` SELECT list: `date, bridge, source_chain, dest_chain, token, toFloat64(amount_token) AS volume_token, toFloat64(amount_usd) AS volume_usd, toFloat64(net_usd) AS net_usd, toUInt64(txs) AS txs, CASE...AS direction FROM source('crawlers_data','dune_bridge_flows')`. `describe_table crawlers_data.dune_bridge_flows` -> `timestamp,bridge,source_chain,dest_chain,token,amount_token,amount_usd,net_usd` (no `date`, no `txs`).

**BRIDGES-C07** — schema.yml lines 412-495: only `elementary.schema_changes`. SQL `FROM mx, cum, cur7, prev7, bridges, chains` (CROSS JOIN of single-row CTEs). `SELECT count() AS snap_rows FROM dbt.fct_bridges_kpis_snapshot` -> `1`. Empty-input simulation: CROSS JOIN of single-row aggregate CTEs yields `1` all-zero/NULL row, not 0.

**BRIDGES-C08** — `int_bridges_flows_daily.sql` lines 1-9: `engine=ReplacingMergeTree()`, `incremental_strategy=insert_overwrite`, `partition_by toStartOfMonth(date)`. grep `FINAL` under `models/bridges` -> empty. grep `int_bridges_flows_daily` under `scripts/` -> empty (no raw-append path).
```sql
SELECT count(), uniqExact((date,bridge,source_chain,dest_chain,token,direction)) FROM dbt.int_bridges_flows_daily;
-- 65,818 ; 65,818 (no live dup)
```

**BRIDGES-C09** — `fct_bridges_netflow_weekly_by_bridge.sql` line 38 `coalesce(w.netflow_usd_week, 0)` on LEFT JOIN w (40-42); no `join_use_nulls` hook (grep empty under `models/bridges`).

**BRIDGES-C10** — marts schema.yml lines 8-10: `name: date`, `data_type: Date`. `scripts/checks/check_api_tags.allow`: `api_bridges_cum_netflow_weekly_by_bridge::no_grain_col` still present; precedent `api_execution_cow_top_pairs_weekly::no_grain_col` (line 37) with comment about `xField:date`. `check_api_tags.py` `GRAIN_COL['weekly']={'week'}` keys on column NAME.

**BRIDGES-C11** — `api_bridges_sankey_gnosis_in_ranges.sql`: SELECT `r.range,e.source,e.target,sum(e.value)` (20-23), GROUP BY `r.range,e.source,e.target,r.range_order` (28). `out_ranges` identical (SELECT 19-22, GROUP BY 27). `range_order` literals 1:1 with `range`.

**BRIDGES-C12** — `semantic_models.yml` line 774 metric `bridge_netflow_weekly_by_bridge` `quality_tier: approved`; all semantic_models (incl. line 267) + other metrics `candidate`. `get_metric_details('bridge_netflow_weekly_by_bridge')` -> "exists, but it is not approved for semantic execution yet". `discover_metrics('bridges token netflow by bridge')` -> only `cow_top_pairs_volume`, gpay topups/actions (zero bridge metrics).

**BRIDGES-C13** —
```sql
SELECT countIf(direction='xchain'), uniqExact(direction) FROM dbt.int_bridges_flows_daily;  -- 0 ; 2
SELECT uniqExact(direction), countIf(direction='xchain'), count() FROM dbt.stg_crawlers_data__dune_bridge_flows;
-- 2 ; 0 ; 360,351
```
`fct_bridges_kpis_snapshot.sql` cum/cur7/prev7 CTEs aggregate `sum(volume_usd)` with no direction filter.

**BRIDGES-C14** —
```sql
SELECT count(), uniqExact(as_of_date), max(as_of_date) FROM dbt.fct_bridges_kpis_snapshot;  -- 1 ; 1 ; 2026-06-21
```
`materialized='table'`, `partition_by toStartOfMonth(as_of_date)`; API views `ORDER BY as_of_date DESC LIMIT 1`. No `_history` model in marts dir.

**BRIDGES-C15** —
```sql
SELECT count(), countIf(volume_usd IS NULL), sum(volume_usd) FROM dbt.int_execution_bridges_address_flows_daily WHERE date>='2026-05-01';
-- 348,498 ; 348,498 ; NULL
```
SQL line 50 `CAST(NULL AS Nullable(Float64)) AS volume_usd`; column desc "USD volume (currently NULL — placeholder for when whitelisted_daily carries USD)". Semantic measure `volume_usd_value` agg `sum` (line 532); graph `weight_column: volume_usd` (line 557). `transfer_count` populated (`sum=220,344,649`).

**BRIDGES-C16** — `semantic_models.yml`: `bridges_token_netflow_daily_by_bridge` (line 440) + `fct_bridges_token_netflow_daily_by_bridge` (line 471), shared synonym at lines 469/497, generated metrics at lines 1513/1565. `discover_metrics('bridges token netflow by bridge')` surfaced neither (both candidate). `api_bridges_token_netflow_daily_by_bridge.sql` adds the `All` rollup via UNION ALL (lines 11-24).

**BRIDGES-C17** —
```sql
SELECT count(), countIf((amount_usd IS NULL OR amount_usd=0) AND amount_token!=0), countIf(amount_usd IS NULL OR amount_usd=0)
FROM dbt.stg_crawlers_data__dune_bridge_flows WHERE timestamp>=today()-30;
-- 3,434 ; 0 ; 0
```
`int_bridges_flows_daily.sql` lines 21-25 derive USD from Dune `amount_usd`/`net_usd`; no Chainlink join.

## Review log (>=3 rounds per case)

- **BRIDGES-C01**: R1 CONFIRMED/high (type mismatch + 30%/39% negatives reproduced) -> challenge: quantify SERVING impact on the API view + check for contract coercion -> R2 CONFIRMED/high (`11,840/29,773` neg, `All` rollup `4,290` neg; CH stores Float64 regardless) -> challenge: any `contract.enforced`? -> R3 CONFIRMED/high (no contract anywhere; pure doc drift, real defect).
- **BRIDGES-C02**: R1 CONFIRMED/high (phantom `d` default time dim) -> challenge: `as_of_date` dim coexists, prove failure mode -> R2 CHANGED/medium (break scoped to default/`metric_time` path; dynamic explain blocked by env `manifest_hash_mismatch`, settled statically) -> R3 CONFIRMED-as-CHANGED/medium (16-col live table, no `d`; default path breaks, `as_of_date` resolves).
- **BRIDGES-C03**: R1 CONFIRMED/high (5 phantom `d` + `as_of_date` `DateTime` vs `Date`) -> challenge: is drift caught by any CI guard? -> R2 CONFIRMED/high (no type-vs-live validator; `check_api_tags.py` only presence) -> challenge: does phantom `d` reach a discovery artifact? -> R3 CONFIRMED/high (`get_model_details` IS misled, lists `d (Date)`; `describe_table` is not — blast radius broader than schema.yml-only).
- **BRIDGES-C04**: R1 CHANGED/medium (data now ~1-day fresh; thresholds still absent) -> challenge: source-level `18h/30h` is inherited, "no alert" is wrong -> R2 CHANGED/low (inheritance confirmed; re-scoped to tuning) -> challenge: does the inherited warn have teeth at D-1? -> R3 RESOLVED/low (`hours_since=21h > 18h` warn fires; data current).
- **BRIDGES-C05**: R1 CONFIRMED/medium (`chg_vol_7d` raw fraction) -> challenge: does the API view multiply by 100? -> R2 CONFIRMED/medium (`change_pct` served raw, no `*100`) -> challenge: any consumer compensates? -> R3 CONFIRMED/medium (no dashboards dir; latent only).
- **BRIDGES-C06**: R1 CONFIRMED/medium (dev tag, empty semantic, unwired) -> challenge: does v2 staging source exist in prod? -> R2 CONFIRMED/medium (reads prod source, selects missing `date`/`txs`) -> challenge: prove build-and-fail with column list -> R3 CONFIRMED/medium (`describe_table` confirms `date`/`txs` absent -> `UNKNOWN_IDENTIFIER`).
- **BRIDGES-C07**: R1 CONFIRMED/medium (no row_count/not_null test; CROSS JOIN) -> challenge: prove empty-input behavior (0 rows vs 1 all-zero) -> R2 CONFIRMED/low (1 all-zero row, not empty; "silently empties" overstated) -> R3 CONFIRMED/low.
- **BRIDGES-C08**: R1 CONFIRMED/medium (RMT, no FINAL; no live dup) -> challenge: does insert_overwrite REPLACE PARTITION preclude the window? -> R2 CONFIRMED/low (atomic monthly swap) -> challenge: any raw-append write path? -> R3 CONFIRMED/low (sole path is insert_overwrite; scripts grep empty).
- **BRIDGES-C09**: R1 CONFIRMED/medium (coalesce on LEFT JOIN, no hook) -> challenge: does grid produce unmatched cells / is it a no-op? -> R2 CONFIRMED/low (no-op under default `join_use_nulls=0`) -> R3 CONFIRMED/low.
- **BRIDGES-C10**: R1 CHANGED/low (date now typed; allowlist line stale) -> challenge: would removing allowlist pass check_api_tags? -> R2 CONFIRMED/low (still fails — `no_grain_col` keys on name `week`) -> challenge: what is the project convention (cow_top_pairs precedent)? -> R3 CHANGED/low (allowlist is project-standard; schema gap resolved).
- **BRIDGES-C11**: R1 CONFIRMED/low (range_order in GROUP BY not SELECT) -> challenge: any row fan-out? -> R2 CONFIRMED/low (1:1 functional dependency, no fan-out) -> R3 CONFIRMED/low.
- **BRIDGES-C12**: R1 CHANGED/medium (one metric promoted to approved at line 774; rest candidate) -> challenge: does MCP trust key on semantic_model or metric tier? -> R2 CHANGED/medium (per-metric; but discovery surfaced no bridge metric) -> challenge: is the one approved metric live-queryable? -> R3 CHANGED/medium (registry rejects it: "not approved for semantic execution yet").
- **BRIDGES-C13**: R1 CONFIRMED/medium (no direction filter; 0 xchain) -> challenge: can the staging source ever emit xchain? -> R2 CONFIRMED/low (0 xchain over 360,351 staging rows; latent doc gap) -> R3 CONFIRMED/low.
- **BRIDGES-C14**: R1 CONFIRMED/medium (single-row snapshot) -> challenge: does a separate history/trend model cover the gap? -> R2 CONFIRMED/low (none exists; by-design snapshot) -> R3 CONFIRMED/low.
- **BRIDGES-C15**: R1 CONFIRMED/medium (`CAST(NULL)`, sum measure -> 0/NULL) -> challenge: confirm served result + graph weight + fallback -> R2 CONFIRMED/medium (graph `weight_column: volume_usd` all-NULL; `transfer_count` fallback) -> challenge: placeholder vs oversight; upstream USD available? -> R3 CONFIRMED/medium (documented placeholder; no upstream priced column — structural gap).
- **BRIDGES-C16**: R1 CONFIRMED/low (dual semantic models, same grain) -> challenge: do both generate metrics with colliding synonyms? -> R2 CONFIRMED/low (both metrics, shared synonyms) -> challenge: does discover_metrics surface both (live collision)? -> R3 CONFIRMED/low (neither surfaces — both candidate; definition-level only).
- **BRIDGES-C17**: R1 CONFIRMED/low (Dune-only USD, no Chainlink) -> challenge: quantify missing-USD coverage today -> R2 CONFIRMED/low (0 missing-USD rows over 30d; forward-looking) -> R3 CONFIRMED/low.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P1 (KEEP) | Correct schema.yml `data_type` from `UInt64 -> Float64` for `net_usd`/`volume_usd`/`volume_token`/`netflow_usd_week`/`value`; the columns legitimately carry signed negatives (40% of API rows, `All` rollup min `-9.84M`). Pure documentation fix (no contract enforces it). | `models/bridges/intermediate/schema.yml`, `models/bridges/marts/schema.yml`, `int_bridges_flows_daily`, `fct_bridges_netflow_weekly_by_bridge`, `fct_bridges_token_netflow_daily_by_bridge`, `api_bridges_token_netflow_daily_by_bridge` |
| P1 (KEEP) | Remove the phantom column `d` from all 5 marts schema.yml entries and fix `fct_bridges_kpis_snapshot.as_of_date` `DateTime -> Date`. `get_model_details` is manifest-sourced and surfaces the phantom `d`, misleading MCP discovery consumers. | `models/bridges/marts/schema.yml`, `fct_bridges_kpis_snapshot`, `api_bridges_sankey_gnosis_in_by_token_7d`, `api_bridges_sankey_gnosis_out_by_token_7d`, `api_bridges_sankey_gnosis_in_ranges`, `api_bridges_sankey_gnosis_out_ranges` |
| P2 (CHANGED/KEEP) | Fix the `bridges_kpis_snapshot` semantic model: change `defaults.agg_time_dimension` from `d` to `as_of_date` and remove the phantom `d` time dimension + its `allowed_dimensions` references, so default/`metric_time`-keyed queries bind to a real column. | `semantic/authoring/bridges/semantic_models.yml`, `fct_bridges_kpis_snapshot` |
| P2 (CHANGED/KEEP) | Resolve the quality-tier governance gap: reload/rebuild the semantic registry so the YAML-approved `bridge_netflow_weekly_by_bridge` (line 774) is actually live-queryable, and decide a promotion path for the remaining bridge metrics — today no bridge metric is discoverable/queryable via MCP. | `semantic/authoring/bridges/semantic_models.yml` |
| P2 (KEEP) | Either populate `volume_usd` at address grain (no upstream priced column exists today in `int_execution_transfers_whitelisted_daily`) or drop the `volume_usd_value` sum measure and switch the `bridge_user_flows` graph `weight_column` to `transfer_count`. The current state serves NULL/0 edge weights. | `int_execution_bridges_address_flows_daily`, `semantic/authoring/bridges/semantic_models.yml` |
| P3 (KEEP) | Either rename `change_pct` source columns to a fraction-suffixed name, multiply by `*100` in the API views, or update the schema descriptions to say "fraction, not percent". No consumer compensates today (latent). | `fct_bridges_kpis_snapshot`, `api_bridges_kpi_volume_7d`, `api_bridges_kpi_netflow_7d`, `models/bridges/marts/schema.yml` |
| P3 (KEEP) | Remove or fix the dev-tagged `int_bridges_flows_daily_v2` chain — it builds-and-fails on dev selectors (`stg_..._v2` selects `date`/`txs` absent from the source) and its semantic model is empty. | `int_bridges_flows_daily_v2`, `stg_crawlers_data__dune_bridge_flows_v2`, `semantic/authoring/bridges/semantic_models.yml` |
| P3 (KEEP) | Add a `not_null(as_of_date)` / single-row assertion on `fct_bridges_kpis_snapshot` so an upstream outage surfaces as a test failure rather than a silent all-zero snapshot. | `fct_bridges_kpis_snapshot`, `models/bridges/marts/schema.yml` |
| P4 (KEEP, low) | Document the snapshot-vs-history split (C14), the xchain-unfiltered volume convention (C13), the dual-semantic-model overlap (C16), and the Dune-only pricing / no Chainlink fallback (C17, P0-16). Remove the redundant `range_order` from the Sankey GROUP BYs (C11) and the no-op `coalesce(...,0)` (C09) per project convention. | `fct_bridges_kpis_snapshot`, `int_bridges_flows_daily`, `api_bridges_sankey_gnosis_in_ranges`, `api_bridges_sankey_gnosis_out_ranges`, `fct_bridges_netflow_weekly_by_bridge`, `semantic/authoring/bridges/semantic_models.yml` |
| DROP | Freshness alert recommendation (baseline C04) — `dune_bridge_flows` inherits source-level `18h/30h` freshness; the warn already fires at `21h` D-1 lag and data is current. Optionally add a table-specific threshold tuned to daily cadence to avoid benign morning warns (cosmetic). | `models/crawlers_data/sources.yml` |
