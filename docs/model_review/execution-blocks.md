# Model review: execution/blocks

**Convergence:** converged in 1 round — both agents independently identified the partial-month exposure as the primary defect; context agent added three semantic-layer issues the inspector missed, with no contradictions.

---

## Scope and inventory

| Layer | Model | Grain | Materialization |
|---|---|---|---|
| Intermediate | `int_execution_blocks_gas_usage_daily` | date | incremental (insert_overwrite) |
| Intermediate | `int_execution_blocks_clients_version_daily` | date, client, version | incremental (insert_overwrite) |
| Fact | `fct_execution_blocks_gas_usage_monthly` | month | incremental (insert_overwrite) |
| Fact | `fct_execution_blocks_clients_daily` | date, client | view |
| API/mart | `api_execution_blocks_gas_usage_pct_daily` | date | view |
| API/mart | `api_execution_blocks_gas_usage_pct_monthly` | month | view |
| API/mart | `api_execution_blocks_clients_cnt_daily` | date, client | view |
| API/mart | `api_execution_blocks_clients_pct_daily` | date, client | view |

Two intermediates feed four api_ mart views and two fct views. Eight semantic models at `quality_tier: candidate` are registered in `semantic/authoring/execution/blocks/semantic_models.yml`. All models originate from the single `execution.blocks` source table.

---

## Business context

This unit answers two infrastructure-health questions for Gnosis Chain:

1. **Client diversity** — which execution clients (Nethermind, Erigon, Besu, Geth, etc.) and versions are producing blocks, identified by parsing the hex-encoded `extra_data` block-header field. The public API exposes these as `blocks_per_clients_count` and `blocks_per_clients_pct` endpoints.

2. **Gas utilization** — the ratio `SUM(gas_used) / SUM(gas_limit)` at daily and monthly resolution, measuring block-space demand pressure. Exposed as `blocks_gas_usage_pct` daily and monthly API endpoints.

**Canonical definitions (as implemented):**

- `gas_used_fraq` — `SUM(gas_used) / NULLIF(SUM(gas_limit), 0)` over all deduplicated blocks on a given day. Range [0,1].
- `gas_used_fraq (monthly)` — same ratio rolled up from daily aggregates in `fct_execution_blocks_gas_usage_monthly`.
- `execution client` — `decoded_extra_data[1]` from `decode_hex_tokens(extra_data)`. Convention-based, not standardised.
- `Unknown client` — any block whose decoded first token is one of `'choose'`, `'mysticryuujin'`, `'sanae.io'`, or empty string.
- `client market share (fraq)` — `cnt / SUM(cnt) OVER (PARTITION BY date)` in `fct_execution_blocks_clients_daily`.
- `deduplication` — `ROW_NUMBER() OVER (PARTITION BY block_number ORDER BY insert_version DESC)`, keeping the highest `insert_version` row.

**Contract context:** No smart-contract addresses, seed lookups, or ABI decoding. Source is declared authoritative (`meta.authoritative: true`) with freshness SLAs of warn at 26 h and error at 48 h. Chain ID 100 (Gnosis Chain) is implicit. The unit title claims "base fee" scope, but `base_fee_per_gas` is present in the source and exposed by no model.

Note: the ESG energy-consumption pipeline uses `int_p2p_discv4_clients_daily` (p2p crawler data), not this unit — client distributions from the two sources may disagree.

---

## Implementation assessment

### HIGH

**Incomplete current month exposed as final by monthly gas-usage models**
`models/execution/blocks/marts/fct_execution_blocks_gas_usage_monthly.sql` and `models/execution/blocks/marts/api_execution_blocks_gas_usage_pct_monthly.sql` have no guard filtering the in-progress month, unlike the daily models which correctly apply `WHERE date < today()`. Warehouse confirmation: June 2026 shows `gas_used_fraq = 46.08%` from 8 days of data (2026-06-01 to 2026-06-08) vs 38.9% for the full May 2026 month. The latest monthly row mutates silently every day. Fix: add `WHERE month < toStartOfMonth(today())` or expose an `is_complete_month` boolean flag.

### MEDIUM

**No grain uniqueness test at fct/monthly layer**
`int_execution_blocks_gas_usage_daily` and `int_execution_blocks_clients_version_daily` carry `dbt_utils.unique_combination_of_columns`, but `fct_execution_blocks_gas_usage_monthly` (month grain) and `fct_execution_blocks_clients_daily` (date, client grain) have no uniqueness or not_null tests. A `GROUP BY` double-count at either layer would be undetected. Affected: `models/execution/blocks/marts/fct_execution_blocks_gas_usage_monthly.sql`, `models/execution/blocks/marts/fct_execution_blocks_clients_daily.sql`, `models/execution/blocks/marts/schema.yml`.

**`decode_hex_tokens` used instead of safer `decode_hex_tokens2`**
`models/execution/blocks/intermediate/int_execution_blocks_clients_version_daily.sql` calls `decode_hex_tokens`, which passes strings directly to `unhex()` without guarding against odd-length or non-hex values. The `decode_hex_tokens2` variant in the same macro file adds both guards. Malformed proposer `extra_data` may yield garbled token arrays rather than empty arrays, silently mislabeling client rows.

**Schema.yml declares `DateTime64` but ClickHouse type is `DateTime`**
`describe_table` confirms `int_execution_blocks_clients_version_daily.date` is typed `DateTime` in ClickHouse, while `models/execution/blocks/intermediate/schema.yml` declares `data_type: DateTime64`. This misleads MCP/API metadata and could trigger Elementary `schema_changes` tests when the type annotation is corrected.

### LOW

**`api_execution_blocks_gas_usage_pct_monthly` missing Elementary freshness/volume tests**
All other `api_` marts in this unit carry `elementary.volume_anomalies` and `elementary.freshness_anomalies`. This mart carries only `elementary.schema_changes`. A stale or zero-row monthly view would be undetected, compounding the high-severity partial-month issue. Affected: `models/execution/blocks/marts/schema.yml`.

**Unknown-client bucket (8.6% of rows) is unmonitored**
1,126 of 13,032 `(date, client, version)` rows are `client = Unknown`. The hardcoded `multiIf` allow-list (`'choose'`, `'mysticryuujin'`, `'sanae.io'`, empty string) in `models/execution/blocks/intermediate/int_execution_blocks_clients_version_daily.sql` has no catch-all and no Elementary monitor. A spike in unclassified graffiti is invisible.

**Source pipeline is 3 days behind today**
Both intermediates have `max(date) = 2026-06-08` (today - 3). The daily api_ views apply `WHERE date < today()`, so consumers see data through T-3. This is a pipeline-lag issue, not a dbt logic error, but it appears to conflict with the declared 26 h freshness SLA.

---

## Business-logic assessment

### HIGH

**Monthly gas-utilization figure is materially wrong for the current month**
The business question is "how efficiently is block space consumed per month?" Serving a partial-month ratio as a completed monthly figure is a definition error. Any external consumer or report reading the latest row from `models/execution/blocks/marts/api_execution_blocks_gas_usage_pct_monthly.sql` receives a value that is simultaneously wrong and silently changing every day. This is the single most important trust issue for the unit.

**Semantic `fraq_value` metric sums a pre-computed ratio column**
`execution_blocks_clients_daily.fraq_value` in `semantic/authoring/execution/blocks/semantic_models.yml` declares `agg: sum` over the pre-computed `cnt / SUM(cnt) OVER (PARTITION BY date)` fraction column. MetricFlow summing ratios across client rows or time windows yields a sum-of-ratios, not a ratio-of-sums — semantically meaningless. Any MCP query aggregating this measure returns incorrect numbers. The fix is a derived metric computing `sum(numerator) / sum(denominator)`.

### MEDIUM

**Monthly semantic model mislabeled as `grain: day`**
`execution_blocks_gas_usage_pct_monthly` in `semantic/authoring/execution/blocks/semantic_models.yml` declares `agg_time_dimension: date` with `grain: day` and lists `day` in `supported_time_grains`, but the mart does `SELECT month AS date` at monthly resolution. MetricFlow time-grain queries against this model would produce inconsistent results. Also noted via the `no_grain_col` exemption in `check_api_tags.allow`.

**Unit title over-states scope: `base_fee` never implemented**
The unit is titled "Block-level metrics: production, gas, base fee", but `base_fee_per_gas` (UInt64, wei) is documented in the source and surfaced by no model. Gnosis Chain runs EIP-1559; base-fee trends are directly relevant to network health monitoring. Either build the metric or correct the scope claim.

**Client identification is heuristic and convention-dependent**
`decoded_extra_data[1]` as client name and `[2]` or `[3]` as version is an informal convention, not an EIP standard. Pre-Merge (PoA) vs post-Merge `extra_data` conventions differ. The `Unknown` bucket has no monitoring. Numbers are directionally useful but should not be treated as authoritative client market share by external consumers without these caveats in documentation.

### LOW

**Candidate quality tier exposed to public API without signal**
All eight semantic models are `quality_tier: candidate` (unreviewed/unpromoted) yet the `api_` models serve the public analytics API. Consumers receive no indication these are provisional. Affected: `semantic/authoring/execution/blocks/semantic_models.yml`.

**`fct_execution_blocks_gas_usage_monthly` tagged `transactions` not `blocks`**
Inspector flagged a likely copy-paste tag error. Low business impact but affects tag-based model selection and `check_api_tags.py` CI consistency. Affected: `models/execution/blocks/marts/fct_execution_blocks_gas_usage_monthly.sql`.

---

## Data findings

Seven warehouse queries were executed during the review:

- Row counts and date ranges on both intermediates confirmed: `max(date) = 2026-06-08` in both tables (today - 3).
- `int_execution_blocks_clients_version_daily`: 13,032 distinct `(date, client, version)` rows; grain uniqueness confirmed at the intermediate layer.
- `gas_used_fraq` stats: `min = 1.2e-7` (earliest Gnosis Chain blocks, 2018-10-08), consistent with minimal early activity — no data quality issue confirmed.
- Unknown client: 1,126 / 13,032 rows (8.6%) across all time.
- `fct_execution_blocks_gas_usage_monthly`, June 2026 row: `gas_used_sum = 1.04T`, `gas_limit_sum = 2.26T`, `used_pct = 46.08%` from only 2026-06-01 to 2026-06-08 (8 days). May 2026 full-month = 38.9%. Partial-month confirmed.
- Partial-month value is passed unchanged to `api_execution_blocks_gas_usage_pct_monthly`.

---

## Pros / Cons

**Pros**

- Incremental strategy (`insert_overwrite` / partition-replace) and `dedup_source` macro usage follow project convention correctly throughout.
- Daily-grain models correctly exclude the in-progress day via `WHERE date < today()`, preventing partial-day distortion.
- Block deduplication by `block_number` on highest `insert_version` guards against duplicate cryo ingestion.
- Source is declared authoritative with explicit freshness SLAs (warn 26 h / error 48 h).
- Most `api_` marts carry Elementary `volume_anomalies` and `freshness_anomalies`.
- `gas_used_fraq` is correctly `NULLIF`-guarded against zero `gas_limit` denominators.
- Clear two-dimensional scope (client diversity + gas utilization) with full intermediate → fct → api → semantic lineage.
- Full read coverage in review: all 8 SQL files, both schema.yml files, all three decode macro variants, and the `check_api_tags.py` CI guard.

**Cons**

- Monthly gas-utilization api endpoint serves an incomplete current month as if final; the latest value silently changes every day.
- Semantic `fraq_value` metric sums a pre-computed ratio column — MetricFlow aggregation produces meaningless sum-of-ratios.
- Monthly semantic model declares `grain: day` on monthly data, with `month` aliased to `date` — confuses MCP/API consumers.
- Unit title claims base-fee scope but `base_fee_per_gas` is never implemented despite being in the source.
- Client identification is heuristic hex parsing with a hardcoded Unknown allow-list; 8.6% of rows are Unknown and unmonitored.
- Schema.yml type discrepancy (`DateTime64` vs actual `DateTime`) yields misleading API metadata.
- No grain uniqueness test at the fct/monthly layer — a `GROUP BY` double-count would be undetected.
- All metrics are `candidate` quality tier, yet served to the public API with no consumer-facing signal.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| 1 — High | Add `WHERE month < toStartOfMonth(today())` to `fct_execution_blocks_gas_usage_monthly`, or expose an `is_complete_month` boolean; propagate to the api_ mart view | `fct_execution_blocks_gas_usage_monthly.sql`, `api_execution_blocks_gas_usage_pct_monthly.sql` |
| 2 — High | Convert `fraq_value` (and any other ratio measures) in the semantic models from `agg: sum` over a pre-computed fraction to a derived metric computing `sum(numerator) / sum(denominator)` | `semantic/authoring/execution/blocks/semantic_models.yml`, `fct_execution_blocks_clients_daily.sql` |
| 3 — Medium | Fix the monthly semantic model: set `grain: month`, remove `day` from `supported_time_grains`, and reconcile the `month AS date` alias | `semantic/authoring/execution/blocks/semantic_models.yml`, `api_execution_blocks_gas_usage_pct_monthly.sql` |
| 4 — Medium | Add `dbt_utils.unique_combination_of_columns` and `not_null` tests on `fct_execution_blocks_gas_usage_monthly` (month) and `fct_execution_blocks_clients_daily` (date, client) | `schema.yml` (marts) |
| 5 — Medium | Switch `int_execution_blocks_clients_version_daily` to `decode_hex_tokens2` (or add odd-length / non-hex validation), and confirm whether malformed `extra_data` values exist in the source | `int_execution_blocks_clients_version_daily.sql` |
| 6 — Medium | Correct `schema.yml` `data_type` from `DateTime64` to `DateTime` for `int_execution_blocks_clients_version_daily.date` | `models/execution/blocks/intermediate/schema.yml` |
| 7 — Medium | Either build `base_fee_per_gas` metrics (and/or block production count) or correct the unit title to remove the "base fee" claim | `int_execution_blocks_gas_usage_daily.sql`, unit title |
| 8 — Low | Add `elementary.volume_anomalies` and `elementary.freshness_anomalies` to `api_execution_blocks_gas_usage_pct_monthly` to match other api_ marts | `models/execution/blocks/marts/schema.yml` |
| 9 — Low | Add monitoring on the Unknown-client bucket; establish a maintenance process to review the hardcoded graffiti allow-list against recent `extra_data` values | `int_execution_blocks_clients_version_daily.sql` |
| 10 — Low | Confirm the 3-day source lag is within the 26 h/48 h freshness SLA; correct the `transactions` tag on `fct_execution_blocks_gas_usage_monthly`; confirm or promote candidate quality tier before treating api_ endpoints as production contracts | `fct_execution_blocks_gas_usage_monthly.sql`, `semantic_models.yml` |

---

## Open disagreements

None. The review converged in round 1.

---

## Review log

| Round | Agent | Challenge issued | Outcome |
|---|---|---|---|
| 1 | Inspector | Identified partial-month exposure with warehouse confirmation (June 2026 = 46% from 8 days) | Confirmed by context agent independently |
| 1 | Inspector | Flagged `decode_hex_tokens` vs `decode_hex_tokens2` safety gap | Not challenged; context agent had no contradicting finding |
| 1 | Inspector | Flagged `DateTime64` vs `DateTime` schema.yml discrepancy | Not challenged |
| 1 | Context | Identified sum-of-ratios defect in semantic `fraq_value` — missed by inspector | Not challenged; accepted as additive finding |
| 1 | Context | Identified monthly semantic model `grain: day` contradiction | Not challenged |
| 1 | Context | Identified base_fee_per_gas unimplemented despite unit title claim | Not challenged; inspector had not read semantic/unit-title scope |
| 1 | Context | Confirmed ESG pipeline does NOT consume this unit (uses p2p discv4 data instead) | Resolves open question; no contradiction |
