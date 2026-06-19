# Model review: bridges

**Convergence:** Converged in 2 rounds — all challenges resolved with live query evidence and file citations; no open disagreements remain.

---

## Scope and inventory

| Layer | Models | Purpose |
|---|---|---|
| Staging (external) | 1 | `stg_crawlers_data__dune_bridge_flows` — pass-through from Dune crawl |
| Staging v2 (dev) | 1 | `stg_crawlers_data__dune_bridge_flows_v2` — pre-aggregated daily Dune output, not in production |
| Intermediate | 2 | `int_bridges_flows_daily` (ReplacingMergeTree, transaction grain), `int_bridges_flows_daily_v2` (dev-tagged) |
| Intermediate (execution) | 1 | `int_execution_bridges_address_flows_daily` — address-level flow inference from on-chain transfers |
| Fact | 4 | KPI snapshot, Sankey edges, token netflow daily, weekly netflow by bridge |
| API mart views | 13 | KPI cards (4), Sankey (4), netflow time-series (2), volume time-series (2), cumulative weekly (1) |
| Semantic models | 16+ | All `quality_tier: candidate`; defined in `semantic/authoring/bridges/semantic_models.yml` |

Total SQL files in scope: 20. All were fully read by the inspector. Eight exploratory queries and five verification queries were run across two rounds.

---

## Business context

The bridges unit measures cross-chain capital flow for Gnosis Chain, sourced exclusively from the Dune crawl (`crawlers_data.dune_bridge_flows`). It covers four bridge families (xDai Bridge, OmniBridge, Arbitrum Bridge, Optimism Bridge, and open-ended additional protocols identified by string name from Dune data — no addresses are hardcoded in the bridges pipeline).

**Canonical definitions:**

- `volume_usd`: Gross USD value of all bridge transfers in a period regardless of direction (inflow + outflow). Sourced from Dune's `amount_usd`; cast to Float64 at staging.
- `net_usd` / `netflow_usd`: Net USD flow from Gnosis Chain's perspective: positive = net capital entering Gnosis. The sign is pre-applied at the Dune query level (Query ID 5334446): outflows (`source_chain='gnosis'`) receive `-amount_usd`; inflows (`dest_chain='gnosis'`) receive `+amount_usd`; xchain rows receive NULL. The staging model does `toFloat64(net_usd)` with no further sign logic. ClickHouse NULL-sum semantics cause xchain rows to contribute zero to netflow aggregations.
- `direction`: Categorical — `'in'` (dest_chain='gnosis'), `'out'` (source_chain='gnosis'), `'xchain'` (neither endpoint is Gnosis). Assigned in staging.
- `vol_7d` / `net_7d`: Rolling 7-day window ending at `max(date)` in `int_bridges_flows_daily`, using inclusive offsets `subtractDays(max_date, 6)` to `max_date`. The prior window is `subtractDays(max_date, 13)` to `subtractDays(max_date, 7)` — also 7 days.
- `chg_vol_7d` / `chg_net_7d`: Period-over-period change expressed as a decimal ratio: `(current - previous) / previous`. NULL when previous = 0. NOT a percentage multiplied by 100.
- `rate_7d`: Efficiency ratio = `net_7d / vol_7d`. NULL when `vol_7d = 0`.
- `distinct_chains`: `uniqExact` over the union of source and destination chains, with Gnosis itself explicitly excluded.
- `week boundary`: Monday-start ISO weeks via `toStartOfWeek(date, 1)`; current incomplete week is excluded.
- `Sankey edge encoding`: Each transfer generates two directed edge rows. For inflow: (source_chain → bridge) and (bridge → 'gnosis'). For outflow: ('gnosis' → bridge) and (bridge → dest_chain). xchain rows are excluded from all Sankey models.

**Contract context:** Bridge identification is entirely string-name-based from Dune data. Seeds (`contracts_whitelist.csv`, `tokens_whitelist.csv`) are not referenced in the bridges unit. The address-grain model (`int_execution_bridges_address_flows_daily`) identifies bridge contracts dynamically via `int_crawlers_data_labels` where `sector = 'Bridges'` or `project LIKE '%bridge%'`. USD pricing depends entirely on Dune's external price feed; the native Chainlink price replacement plan has not been applied to this unit.

---

## Implementation assessment

### High severity

**UInt64 type lie is pervasive across at least four models**
`models/bridges/intermediate/schema.yml`, `models/bridges/marts/schema.yml`

`schema.yml` declares `net_usd`, `volume_usd`, and `volume_token` in `int_bridges_flows_daily` as `data_type: UInt64`. The actual ClickHouse type is Float64 for all three, and `net_usd` carries signed negative values by design. This error propagates to the marts layer: `fct_bridges_netflow_weekly_by_bridge.netflow_usd_week` (schema: UInt64, CH: Float64; 309 of 1,016 rows = 30% are negative; min = -11,577,460 USD), `fct_bridges_token_netflow_daily_by_bridge.value` (schema: UInt64, CH: Float64; 7,491 of 19,025 rows = 39% are negative; min = -9,843,904 USD), and `api_bridges_token_netflow_daily_by_bridge.value` (also UInt64 in schema). UInt64 cannot represent negative values; any consumer relying on the schema contract for type validation will silently receive wrong type metadata and may clip or misinterpret negatives.

**bridges_kpis_snapshot semantic model `agg_time_dimension` references non-existent column `d`**
`semantic/authoring/bridges/semantic_models.yml` lines 172-242, `models/bridges/marts/fct_bridges_kpis_snapshot.sql`

The semantic model declares `defaults.agg_time_dimension: d` and `dimension expr: d`. `fct_bridges_kpis_snapshot` has 17 actual columns (confirmed via `describe_table`) — none is named `d`. The SQL produces `mx.d AS as_of_date`, renaming the column at output; `d` never appears in the view. All 14 auto-generated metrics (`cum_vol_usd_value`, `cum_net_usd_value`, `cum_txs_value`, `vol_7d_value`, `net_7d_value`, `txs_7d_value`, `vol_prev_7d_value`, `net_prev_7d_value`, `txs_prev_7d_value`, `rate_7d_value`, `rate_prev_7d_value`, `chg_vol_7d_value`, `chg_net_7d_value`, `chg_rate_7d_value`) list `d` in `allowed_dimensions`. Every MetricFlow time-series query routed through this semantic model fails at compile/query time. The `as_of_date` dimension is correctly wired in the same file and is the fix target.

**Phantom `d` column in schema.yml for five mart models**
`models/bridges/marts/schema.yml`

`fct_bridges_kpis_snapshot`, `api_bridges_sankey_gnosis_in_by_token_7d`, `api_bridges_sankey_gnosis_out_by_token_7d`, `api_bridges_sankey_gnosis_in_ranges`, and `api_bridges_sankey_gnosis_out_ranges` all declare a column named `d` in schema.yml. None of these models output a column named `d` — confirmed via `describe_table` and live view queries. These stale entries break any column-level dbt tests and MCP tools relying on the schema contract. Additionally, `fct_bridges_kpis_snapshot.as_of_date` is declared as `data_type: DateTime` when the actual CH type is `Date`.

### Medium severity

**No freshness alerting on `dune_bridge_flows` source**
`models/bridges/intermediate/int_bridges_flows_daily.sql` (sources.yml)

`sources.yml` sets `loaded_at_field: timestamp` for `dune_bridge_flows` but defines no `warn_after` or `error_after` thresholds. Other Dune sources (`dune_prices`, `dune_gno_supply`) have 36h warn / 48h error configured. Data is currently 4 days stale (max date 2026-06-07 vs 2026-06-11) with no automated alert triggered.

**`change_pct` is a decimal ratio but described as a percentage**
`models/bridges/marts/fct_bridges_kpis_snapshot.sql`, `models/bridges/marts/api_bridges_kpi_netflow_7d.sql`, `models/bridges/marts/api_bridges_kpi_volume_7d.sql`, `models/bridges/marts/schema.yml`

`chg_vol_7d` and `chg_net_7d` are `(cur - prev) / prev` — a decimal fraction (confirmed value: `chg_vol_7d ≈ 0.159` = 15.9%). The schema.yml description says "expressed as a percentage" and the API mart exposes this as `change_pct`. Consumers expecting a multiplied-by-100 value will silently misinterpret displayed figures.

**`int_bridges_flows_daily_v2` tagged `dev` but active in repo with empty semantic model**
`models/bridges/intermediate/int_bridges_flows_daily_v2.sql`, `semantic/authoring/bridges/semantic_models.yml`

The v2 intermediate model carries `tags=['dev']` and references `stg_crawlers_data__dune_bridge_flows_v2`. It is not wired into any production mart. The companion semantic model `bridges_flows_daily_v2` has no measures or dimensions defined. The model runs on any dev selector; the v2 staging source may not be provisioned in production.

**`fct_bridges_kpis_snapshot` has no uniqueness or not_null tests**
`models/bridges/marts/fct_bridges_kpis_snapshot.sql`, `models/bridges/marts/schema.yml`

The snapshot is a single-row full-rebuild table with no dbt test asserting `row_count=1` or `not_null` on `as_of_date`. If `int_bridges_flows_daily` returns zero rows during a source outage, the CROSS JOIN of the CTE sub-aggregates yields zero rows, and all downstream KPI API views return empty results silently.

**No `FINAL` on ReplacingMergeTree source reads**
`models/bridges/intermediate/int_bridges_flows_daily.sql`, `models/bridges/marts/fct_bridges_kpis_snapshot.sql`, `models/bridges/marts/fct_bridges_netflow_weekly_by_bridge.sql`

`int_bridges_flows_daily` uses `engine=ReplacingMergeTree()` with `insert_overwrite`. No downstream mart reads it with `FINAL`. Under CH Cloud, background merges may be delayed; duplicate pre-merge rows could briefly inflate row counts. Actual risk is low at 65k rows but latent for backfills.

**`coalesce(0, 0)` zero-fill in weekly netflow LEFT JOIN is fragile**
`models/bridges/marts/fct_bridges_netflow_weekly_by_bridge.sql`

`coalesce(w.netflow_usd_week, 0)` on a LEFT JOIN has no effect in ClickHouse default mode (`join_use_nulls=0`) where unmatched rows already yield 0. The pattern is fragile if `join_use_nulls` is ever enabled and inconsistent with the project convention of using pre/post hooks for join null management.

### Low severity

**`api_bridges_cum_netflow_weekly_by_bridge` missing typed date column in schema.yml**
`models/bridges/marts/api_bridges_cum_netflow_weekly_by_bridge.sql`, `models/bridges/marts/schema.yml`

The `date` column (renamed from `week`) lacks a `data_type` entry and is currently allowlisted in `check_api_tags.allow` as `no_grain_col`. The allowlist entry should be resolved by adding a typed schema entry.

**`range_order` in GROUP BY but not in SELECT on Sankey range models**
`models/bridges/marts/api_bridges_sankey_gnosis_in_ranges.sql`, `models/bridges/marts/api_bridges_sankey_gnosis_out_ranges.sql`

Both models include `r.range_order` in GROUP BY but only SELECT `r.range, e.source, e.target, sum(e.value)`. Functionally safe since `range_order` is deterministic on `range`, but adds unnecessary overhead and could confuse future editors.

---

## Business-logic assessment

### High severity

**All bridges semantic models are `quality_tier: candidate` — none promoted to `approved`**
`semantic/authoring/bridges/semantic_models.yml`

Confirmed: every semantic model in the file carries `quality_tier: candidate` (verified at line 267 for `bridges_netflow_weekly_by_bridge` specifically). MCP and API consumers that check `quality_tier` for trust signals are receiving candidate-tier data across all bridge metrics. The live breakage of `bridges_kpis_snapshot`'s `agg_time_dimension` makes the candidate status appropriate, but the structurally sound weekly netflow model could be promoted once the semantic fixes are deployed.

### Medium severity

**xchain volume is silently included in cumulative volume KPIs**
`models/bridges/marts/fct_bridges_kpis_snapshot.sql`, `models/bridges/intermediate/int_bridges_flows_daily.sql`

`fct_bridges_kpis_snapshot` aggregates `volume_usd` without a direction filter. xchain rows (neither endpoint is Gnosis Chain) carry non-NULL `amount_usd` and contribute to `cum_vol_usd` and `vol_7d`. Their `net_usd` is NULL from Dune, so they contribute zero to netflow by ClickHouse NULL-sum semantics. Whether counting non-Gnosis-endpoint volume in "Gnosis Chain bridge volume" is the intended business definition is undocumented. If xchain rows represent cross-chain legs that do not touch Gnosis, their inclusion overstates Gnosis Chain bridge activity. Currently zero xchain rows exist in the data, so there is no numeric impact today.

**`fct_bridges_kpis_snapshot` loses history on each rebuild — no trend analysis possible**
`models/bridges/marts/fct_bridges_kpis_snapshot.sql`

The model is a single-row full-rebuild table (`materialized='table'`, partitioned by `toStartOfMonth(as_of_date)`). Each run overwrites the snapshot. API views use `ORDER BY as_of_date DESC LIMIT 1`. Historical KPI trend analysis (e.g., how cumulative netflow has evolved over time) is not supported from this model.

**`int_execution_bridges_address_flows_daily` exposes `volume_usd` as NULL — semantic measure returns zero**
`models/bridges/intermediate/int_execution_bridges_address_flows_daily.sql`, `semantic/authoring/bridges/semantic_models.yml`

The address-grain model casts `volume_usd` as `CAST(NULL AS Nullable(Float64))`. The semantic model `bridges_address_flows_daily` exposes `bridges_address_flows_daily__volume_usd_value` as a sum measure. This will always return 0. Any graph explorer or MCP query using this measure silently returns zero volume for address-level bridge flows.

### Low severity

**Duplicate semantic models for `fct_bridges_token_netflow_daily_by_bridge` and `api_bridges_token_netflow_daily_by_bridge`**
`semantic/authoring/bridges/semantic_models.yml`, `models/bridges/marts/api_bridges_token_netflow_daily_by_bridge.sql`

Both `fct_bridges_token_netflow_daily_by_bridge` and `bridges_token_netflow_daily_by_bridge` semantic models expose near-identical `net_usd` aggregations at the same grain. The `api_` view adds an `All` bridge rollup row via `UNION ALL`. Having two semantic models for the same underlying fact creates ambiguity for MCP routing and metric discoverability.

**USD pricing depends entirely on external Dune price feed with no fallback**
`models/bridges/intermediate/int_bridges_flows_daily.sql`

All USD valuations use prices pre-computed by Dune analytics. The native Chainlink price replacement plan (`project_native_prices_chainlink.md`) has not been applied to this unit. A Dune pricing gap would silently produce NULL or zero USD values in bridge flow records with no coverage SLA.

---

## Data findings

Thirteen queries were run across two inspector rounds (8 in round 1, 5 in round 2).

| Finding | Value |
|---|---|
| Total rows in `int_bridges_flows_daily` | 65,492 |
| Distinct bridges tracked | 8 |
| Max date in `int_bridges_flows_daily` | 2026-06-07 (4 days stale as of 2026-06-11) |
| `fct_bridges_kpis_snapshot` row count | 1 (single-row snapshot as designed) |
| `cum_net_usd` (snapshot) | 216,382,539 USD |
| Reconciliation check | `sum(net_usd)` across all directional rows matches `cum_net_usd` to 7 significant figures |
| `chg_vol_7d` (confirmed decimal) | ~0.159 (15.9% growth) |
| Negative rows in `fct_bridges_netflow_weekly_by_bridge.netflow_usd_week` | 309 of 1,016 (30.4%); min = -11,577,460 USD |
| Negative rows in `fct_bridges_token_netflow_daily_by_bridge.value` | 7,491 of 19,025 (39.3%); min = -9,843,904 USD |
| `xchain` rows in current data | 0 (no numeric impact on KPIs today) |
| `fct_bridges_kpis_snapshot` actual columns | 17; no column named `d` confirmed |

---

## Pros / Cons

**Pros:**
- Incremental strategy and partition design on `int_bridges_flows_daily` are architecturally sound for a transaction-grain bridge flow table.
- Division-by-zero is guarded at every computation site (`rate_7d`, `chg_vol_7d`, `chg_net_7d`).
- Window arithmetic for 7-day rolling periods is correct and symmetric (inclusive `-6`/`-13` day offsets).
- `net_usd` sign semantics are consistent end-to-end: sign pre-applied at Dune query source, passed through staging unmodified, and confirmed in aggregates. xchain NULL rows contribute zero to netflow by ClickHouse semantics.
- Sankey edge encoding is correct for the two-edge-per-transfer directed graph model.
- API tag routing is unambiguous: `granularity:last_7d` vs `granularity:all_time` correctly disambiguates overlapping `api:netflow` and `api:volume` tags — distinct REST paths `/bridges/netflow/last_7d` and `/bridges/netflow/all_time`.
- KPI arithmetic reconciles: directional sum of `net_usd` matches `fct_bridges_kpis_snapshot.cum_net_usd` to 7 significant figures.
- `distinct_chains` explicitly excludes Gnosis itself, which is the correct scoping for partner-chain count.

**Cons:**
- UInt64 type lie is pervasive across at least four models — a schema contract misrepresentation that will mislead type-validation tooling silently, with 30-39% of rows carrying negative values in the affected fact columns.
- `bridges_kpis_snapshot` semantic model `agg_time_dimension` points to non-existent column `d` — all 14 auto-generated metrics are non-functional for MetricFlow time-series queries right now.
- Five schema.yml phantom `d` column entries create broken column-level test and MCP schema contract surface.
- Data is 4 days stale with no freshness alerting configured on the `dune_bridge_flows` source, unlike other Dune sources that have warn/error thresholds.
- `change_pct` is a decimal ratio (0.159) but described as a percentage in schema — a silent consumer misinterpretation risk in dashboards and MCP responses.
- `int_bridges_flows_daily_v2` with dev tag is a live distraction in any dev selector run; its companion semantic model has zero measures defined.
- `fct_bridges_kpis_snapshot` has no row_count or not_null tests — a source-outage zero-row scenario would silently empty all KPI API views.
- All bridges semantic models are `quality_tier: candidate` with none promoted to `approved`.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| Immediate | Fix `bridges_kpis_snapshot` semantic model: rename `agg_time_dimension` from `d` to `as_of_date` and remove the phantom `d` dimension entry. Unblocks all 14 derived metrics for MetricFlow time-series queries. | `semantic/authoring/bridges/semantic_models.yml` lines 174-181 |
| Immediate | Correct all `UInt64` to `Float64` `data_type` declarations in schema.yml for `net_usd`, `netflow_usd_week`, and `value` columns across intermediate and marts layers. | `models/bridges/intermediate/schema.yml`, `models/bridges/marts/schema.yml` |
| High | Remove all five phantom `d` column entries from `models/bridges/marts/schema.yml`. Replace the `fct_bridges_kpis_snapshot` entry with the correct `as_of_date` column declared as `data_type: Date` (not `DateTime`). | `models/bridges/marts/schema.yml` |
| High | Add freshness alerting to `dune_bridge_flows` in `sources.yml` — add `warn_after: 36h` and `error_after: 48h` matching the pattern for `dune_prices` and `dune_gno_supply`. | sources.yml |
| High | Clarify `change_pct`/`chg_vol_7d`/`chg_net_7d` semantics: either multiply by 100 at the fact layer and rename, or update all schema descriptions to say "decimal ratio (e.g. 0.15 = 15%)" and confirm dashboard/MCP consumers handle the conversion. | `models/bridges/marts/fct_bridges_kpis_snapshot.sql`, `models/bridges/marts/schema.yml` |
| Medium | Add a singular dbt test asserting `row_count=1` and `not_null(as_of_date)` for `fct_bridges_kpis_snapshot` to alert on source-outage zero-row scenarios. | `models/bridges/marts/fct_bridges_kpis_snapshot.sql` |
| Medium | Document and decide the xchain volume inclusion policy in `fct_bridges_kpis_snapshot` — add a direction filter comment or an explicit note confirming inclusion is intentional. | `models/bridges/marts/fct_bridges_kpis_snapshot.sql` |
| Medium | Either delete `int_bridges_flows_daily_v2` and its empty semantic model, or add a comment block with a concrete promotion checklist to make the dev-tag boundary explicit. | `models/bridges/intermediate/int_bridges_flows_daily_v2.sql`, `semantic/authoring/bridges/semantic_models.yml` |
| Low | Resolve the `no_grain_col` allowlist entry for `api_bridges_cum_netflow_weekly_by_bridge` by adding a typed `date` column schema entry, and remove `range_order` from GROUP BY in the Sankey range models. | `models/bridges/marts/api_bridges_cum_netflow_weekly_by_bridge.sql`, `models/bridges/marts/schema.yml`, `api_bridges_sankey_gnosis_*_ranges.sql` |
| Low | Evaluate promoting `bridges_netflow_weekly_by_bridge` to `quality_tier: approved` once the semantic model `agg_time_dimension` fix is deployed — it is the most structurally complete model in the unit. | `semantic/authoring/bridges/semantic_models.yml` |

---

## Open disagreements

None. The review converged in round 2.

---

## Review log

| Round | Challenges | Outcome |
|---|---|---|
| 1→2 (Inspector challenge) | Was the UInt64 type lie limited to `int_bridges_flows_daily` only? | Confirmed extended: two additional mart models (`fct_bridges_netflow_weekly_by_bridge.netflow_usd_week` and `fct_bridges_token_netflow_daily_by_bridge.value`) carry the same error with 30% and 39% negative-row rates respectively; `api_bridges_token_netflow_daily_by_bridge.value` also affected. |
| 1→2 (Inspector challenge) | Was the phantom `d` dimension in the semantic model a live breakage or just a documentation gap? | Confirmed live breakage: `describe_table` on `fct_bridges_kpis_snapshot` found no column `d` among 17 actual columns; all 14 auto-generated KPI metrics list `d` in `allowed_dimensions`, making MetricFlow time-series queries non-functional. |
| 1→2 (Context challenge) | Was `bridges_netflow_weekly_by_bridge` `quality_tier: approved` as the context report claimed? | Confirmed incorrect: re-reading `semantic/authoring/bridges/semantic_models.yml` line 267 shows `quality_tier: candidate` for all models in the file. The prior claim of `approved` was not supported by the file. |
| 1→2 (Context challenge) | Was the `net_usd` sign logic applied in the dbt staging model or pre-applied at the Dune source? | Confirmed pre-applied at Dune source: `stg_crawlers_data__dune_bridge_flows.sql` line 16 does only `toFloat64(net_usd)` with no sign transformation. The Dune query `gnosis_unifiedbridges_flows_v` (Query ID 5334446, documented in `cerebro-docs/docs/reference/dune-queries.md` lines 73505-73514) hardcodes the sign per row. An additional consequence: xchain rows carry NULL `net_usd` from Dune (not zero), so their netflow contribution is zero by ClickHouse NULL-sum semantics, not by an explicit direction filter. |
