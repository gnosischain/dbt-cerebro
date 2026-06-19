# Model review: probelab

**Convergence:** converged in 1 round — both agents reached full agreement with no factual disagreements; all material findings are independently corroborated with warehouse evidence.

---

## Scope and inventory

The `probelab` sector is a thin, read-through layer over pre-aggregated external data delivered by the ProbeLab independent network-measurement crawler. It tracks Gnosis Chain P2P node infrastructure health across five dimensions.

| Layer | Count | Notes |
|---|---|---|
| Sources | 12 declared | 8 have no downstream model |
| Staging | 4 views | Pure pass-through SELECTs; no computation |
| Marts (api_*) | 5 views | Minimal projection/aggregation; no joins |
| Incremental models | 0 | N/A |

All models reside under `models/probelab/`. There are no joins, no ReplacingMergeTree dedup concerns, and no partition design decisions. The unit is entirely non-PII.

---

## Business context

**Intended purpose.** The five mart models answer infrastructure-decentralization questions for the Gnosis Chain analytics platform: total nodes per client type (daily), cloud-provider distribution, geographic distribution, semantic-version distribution, and QUIC protocol-support adoption. Primary consumer is the Network dashboard sector. A documented secondary use is the ESG module's energy attribution (cloud-provider PUE weighting), though no direct `ref()` or `source()` dependency on probelab models appears in ESG SQL — that link may be aspirational or handled outside dbt.

**Canonical definitions.**

- `agent_version_type`: categorical label for the P2P agent/crawler software. Described as both "client software name" and "version type" in docs — the ambiguity is a material open question (see recommendations).
- `value` in `api_probelab_clients_daily`: `any_value(toInt32(floor(__total)))` — the ProbeLab crawl denominator (total peers visited for that client type), not a sum of version-bucket counts.
- `value` in cloud/country/version marts: `toInt32(floor(__count))` — nodes matching the specific (client, category) combination.
- `value` in the QUIC mart: `__count` (UInt64, no floor cast) — nodes matching the (client, quic_support) pair.
- `date`: `toStartOfDay(max_crawl_created_at)` — the last crawl timestamp in the ProbeLab rollup window, truncated to day. Not a wall-clock ingestion date.
- `authoritative: false` declared consistently throughout; ProbeLab is third-party measurement data.

**Contract context.** ProbeLab delivers pre-aggregated Parquet files to S3 (`prod-use1-gnosis`) ingested via the `probelab-agent-semvers-ingestor` click-runner service (Parquet mode, cron 03:00 UTC). All 12 source tables carry `freshness: null`, exempting them from dbt freshness SLA enforcement.

---

## Implementation assessment

### Critical

**All five `api_*` marts are missing `api:` and `granularity:` tags — silently bypass the CI guard and endpoint registry.**
`check_api_tags.py` validates only models that already carry an `api:` tag; because the five probelab marts carry only `['production','probelab','clients','tier1']`, they silently pass the guard rather than failing CI. The result is that none of the five models are registered in cerebro-api routing or the MCP build registry.
Affected: `models/probelab/marts/api_probelab_clients_daily.sql`, `api_probelab_clients_version_daily.sql`, `api_probelab_clients_cloud_daily.sql`, `api_probelab_clients_country_daily.sql`, `api_probelab_clients_quic_daily.sql`.

### High

**`api_probelab_clients_daily`: `any_value(__total)` is not semantically equivalent to total-client count and breaks semantic-layer `sum(value)` cross-mart.**
`__total` is the ProbeLab crawl denominator (all peers visited), not the sum of per-version `__count` values. Warehouse evidence: on 2026-06-10, lighthouse shows `any_value(__total)=286` vs `sum(__count across versions)=282` — a 1.4% discrepancy. More importantly, `value` in `clients_daily` is therefore semantically incompatible with `value` in the four other marts (which use `__count`). Any semantic-layer rollup that sums `value` across marts will silently mix incompatible denominators.
Affected: `models/probelab/marts/api_probelab_clients_daily.sql`.

**QUIC mart named `_daily` but sources a 7-day rolling window table; date derivation suppresses historical crawl rows.**
`api_probelab_clients_quic_daily.sql` reads from `probelab_quic_support_over_7d` — a rolling 7-day average with one row per (crawl, quic_support, client). The mart uses `toStartOfDay(max_crawl_created_at)` as the date, collapsing all historical window rows under a single date equal to the most recent crawl. Prior crawl-day observations are suppressed. The `_daily` suffix implies a point-in-time daily series; consumers evaluating the QUIC adoption KPI (> 30%) will receive a smoothed 7-day average mislabeled as a daily observation.
Affected: `models/probelab/marts/api_probelab_clients_quic_daily.sql`.

### Medium

**`probelab_sources.yml` declares `probelab_quic_support_over_7d.__count` as `Nullable(Float64)` but the warehouse type is `Nullable(UInt32)`.**
The mart passes `__count` through without a cast. A `schema_changes` elementary test or a strict-typed downstream consumer will surface a type mismatch.
Affected: `models/probelab/probelab_sources.yml`, `models/probelab/staging/stg_crawlers_data__probelab_quic_support_over_7d.sql`.

**`staging/schema.yml` declares `__count` and `__total` as non-nullable UInt64/Float64 for the three `_avg_1d` staging models, but the warehouse delivers `Nullable(Float64)`.**
The wrong baseline causes elementary `schema_changes` tests to emit false negatives. The cloud mart applies `toInt32(floor(...))` to a nullable column, which silently returns NULL or 0 on null input depending on ClickHouse settings.
Affected: `models/probelab/staging/schema.yml`, `models/probelab/marts/api_probelab_clients_cloud_daily.sql`.

**Unused `crawl_created_at` column selected in QUIC staging with a `not_null` test but never consumed by the mart.**
`stg_crawlers_data__probelab_quic_support_over_7d.sql` selects `crawl_created_at`; `staging/schema.yml` defines a `not_null` test on it. `api_probelab_clients_quic_daily.sql` never references the column. The test runs at every `dbt test` invocation with no production value.
Affected: `models/probelab/staging/stg_crawlers_data__probelab_quic_support_over_7d.sql`, `models/probelab/marts/api_probelab_clients_quic_daily.sql`.

### Low

**Eight source tables in `probelab_sources.yml` have no downstream model — dead catalog weight.**
`probelab_agent_semvers_over_7d`, `probelab_agent_types_avg_1d`, `probelab_agent_types_over_7d`, `probelab_cloud_provider_over_7d`, `probelab_countries_over_7d`, `probelab_discv5_stale_records`, `probelab_is_cloud_avg_1d`, `probelab_is_cloud_over_7d`. cerebro-docs identifies `discv5_stale_records` and `is_cloud_*` as desired future metrics — these appear to be planned, not archived.
Affected: `models/probelab/probelab_sources.yml`.

**No uniqueness or primary-key tests on any mart or staging model.**
Both `schema.yml` files define only `not_null` and elementary anomaly tests. Grain contracts (date+client unique in `clients_daily`; date+client+version unique in `version_daily`) are correct per warehouse spot-check but are undocumented and untested.
Affected: `models/probelab/marts/schema.yml`, `models/probelab/staging/schema.yml`.

**Marts have no `expose_to_mcp` or `privacy_tier` meta blocks.**
Low risk for non-PII network measurement data, but diverges from project convention for API-tier models.
Affected: `models/probelab/marts/schema.yml`.

---

## Business-logic assessment

### High

**`value` column has three incompatible semantic definitions across the five marts — semantic-layer `sum(value)` is not uniformly meaningful.**
`clients_daily` uses `any_value(__total)` (ProbeLab crawl denominator); cloud/country/version marts use `toInt32(floor(__count))` (per-category node count); the QUIC mart uses raw `__count` (UInt64, no floor). The semantic registry exposes all five with `agg=sum` and no disambiguation. Cross-mart comparison or roll-up via the MCP semantic layer will silently produce inconsistent numbers.

**QUIC mart temporal semantics misrepresent the source as a daily series.**
The infrastructure decentralization KPI (QUIC adoption > 30%) cannot be correctly evaluated against a true daily trend using `api_probelab_clients_quic_daily` — it reflects a 7-day rolling average labeled as a point-in-time observation.

### Medium

**All freshness values null — no SLA enforcement on external third-party ingestion.**
Other `crawlers_data` sources carry `warn_after: 36h` / `error_after: 72h`. ProbeLab data could lag by days without any dbt alert. The decentralization KPIs are used for infrastructure monitoring; silent staleness is a material risk.
Affected: `models/probelab/probelab_sources.yml`.

**cerebro-docs Key Models Reference lists three model names that do not exist.**
Docs name `api_probelab_cloud_providers_daily`, `api_probelab_quic_support_daily`, and `api_probelab_agent_versions_daily`. Actual models are `api_probelab_clients_cloud_daily`, `api_probelab_clients_quic_daily`, and `api_probelab_clients_version_daily`. Combined with the missing `api:` tags (invisible to endpoint registry), discoverability is doubly broken.

**`agent_version_type` semantic definition is ambiguous.**
Schema descriptions say "categorizes the type of agent version"; cerebro-docs describes it as both "client name" and "version type". If this column encodes version tiers (stable/beta/dev) rather than client software names (Lighthouse/Teku/Nimbus), the decentralization KPI interpretation changes materially.
Affected: `models/probelab/staging/schema.yml`, `models/probelab/marts/schema.yml`.

### Low

**All five semantic metrics carry `quality_tier: candidate` — auto-generated, not reviewed, yet exposed via MCP agent queries.**
Given the `value` definition inconsistency across marts, candidate-tier metrics with `agg=sum` can silently return misleading numbers to AI-agent consumers without any quality gate.
Affected: `/semantic/authoring/probelab/semantic_models.yml`.

---

## Data findings

Six warehouse queries were run across two source tables.

| Query | Result |
|---|---|
| `describe_table(probelab_agent_semvers_avg_1d)` | `__count`: `Nullable(Float64)`, contradicting staging schema declaration of `UInt64` |
| `describe_table(probelab_quic_support_over_7d)` | `__count`: `Nullable(UInt32)`, contradicting sources.yml declaration of `Nullable(Float64)`; `crawl_created_at` column confirmed present |
| Freshness/row-count on `agent_semvers_avg_1d` | max `2026-06-10 22:00 UTC`, 11,409 rows, 449 distinct days back to 2025-03-12 — data is current |
| Freshness/row-count on `quic_support_over_7d` | max `2026-06-10 22:00 UTC`, 5,966 rows, 434 distinct days — data is current |
| Grain check date+client+version | 0 duplicates — grain is clean at the finest level |
| `any_value(__total)` vs `sum(__count)` spot-check (lighthouse, 2026-06-10) | `any_value(__total) = 286`, `sum(__count) = 282` — confirmed 1.4% discrepancy |

The `__pct` column in staging is in the [0,1] range (confirmed min=0.0027, max=1.0); schema description is inconsistent but no mart exposes it directly so consumer risk is low.

---

## Pros / Cons

**Pros**

- Extremely thin layer (4 staging views + 5 mart views, no computation) minimizes transformation risk.
- Source data is current: `max_crawl_created_at = 2026-06-10`, within 36-hour freshness norms.
- 1:1 semantic model coverage across all five marts with correct dimensional structure.
- Non-PII network measurement data — no privacy risk at any tier.
- `authoritative: false` declared consistently; consumers are correctly warned this is third-party data.
- Elementary anomaly tests present on staging and mart models.
- Date range spans 449 distinct days (2025-03-12 to 2026-06-10), providing solid trend depth for KPI monitoring.

**Cons**

- All five `api_*` marts silently bypass the CI tag guard — invisible to the canonical endpoint registry and MCP build registry.
- The `value` column has three incompatible semantic definitions across the five marts; semantic-layer `sum(value)` is not uniformly meaningful.
- The QUIC mart is named `_daily` but sources a 7-day rolling average, misrepresenting temporal granularity to all consumers.
- Stale cerebro-docs references three model names that do not exist.
- Eight source tables declared in `probelab_sources.yml` have no downstream models — catalog pollution.
- All freshness values are null — no SLA enforcement on an external third-party ingestion that feeds infrastructure KPIs.
- All five semantic metrics carry `quality_tier: candidate` — none reviewed or promoted, yet exposed via MCP agent queries.
- No uniqueness or primary-key tests on any model.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 | Add `api:probelab` and `granularity:daily` tags to all five mart configs to register them in the canonical endpoint registry and activate the CI tag guard. | All five marts |
| P0 | Resolve the `value` semantic definition inconsistency: decide whether `clients_daily.value` should be `__total` (crawl denominator) or `sum(__count)` (version-attributable nodes), document in `schema.yml`, and align the semantic model measure description. If both numbers are useful, expose them as separate measures. | `api_probelab_clients_daily.sql`, `semantic_models.yml` |
| P1 | Rename `api_probelab_clients_quic_daily` to `api_probelab_clients_quic_7d_avg` (or equivalent) to surface the 7-day rolling window semantics, or change the mart to group on `toStartOfDay(crawl_created_at)` if a true daily series is the intent. | `api_probelab_clients_quic_daily.sql` |
| P1 | Add freshness thresholds (`warn_after: 48h`, `error_after: 96h`) to probelab source tables to enforce staleness alerting for infrastructure KPI monitoring. | `probelab_sources.yml` |
| P1 | Fix the three stale model name references in cerebro-docs Key Models Reference to use the actual names (`api_probelab_clients_cloud_daily`, `api_probelab_clients_quic_daily`, `api_probelab_clients_version_daily`). | cerebro-docs |
| P2 | Correct type declarations in `probelab_sources.yml` (`__count: Nullable(UInt32)` for quic table) and `staging/schema.yml` (`__count: Nullable(Float64)`, `__total: Nullable(Float64)` for avg_1d tables) to match actual warehouse types. | `probelab_sources.yml`, `staging/schema.yml` |
| P2 | Add `unique_combination_of_columns` tests to at least `clients_daily` (date+client) and `version_daily` (date+client+version) to enforce grain contracts. | `marts/schema.yml`, `staging/schema.yml` |
| P2 | Clarify `agent_version_type` in `schema.yml`: add concrete examples (e.g., "Lighthouse", "Teku", "Nimbus") and specify whether it is a client software identifier or a version classification tier. Update cerebro-docs to remove conflicting language. | `staging/schema.yml`, `marts/schema.yml` |
| P3 | Add `meta.expose_to_mcp` and `privacy_tier: public` to mart `schema.yml`, or set defaults in `dbt_project.yml`, to align with project convention for API-tier models. | `marts/schema.yml` |
| P3 | Audit the eight unmodeled source tables: add TODO comments with ticket references for tables planned as future marts (`discv5_stale_records`, `is_cloud_avg_1d` per cerebro-docs); remove from sources file any truly archived tables to reduce catalog pollution. | `probelab_sources.yml` |

---

## Open disagreements

None. Both agents converged in round 1 with full agreement on all material findings.

---

## Review log

| Round | Agent | Challenge | Resolution |
|---|---|---|---|
| 1 | Inspector | Primary inspection pass — no challenges issued to context agent | N/A — no prior report to challenge |
| 1 | Context | Context report filed — no challenges issued to inspector | N/A — no prior report to challenge |
| Verdict | Orchestrator | Convergence confirmed; no challenges required | Converged |
