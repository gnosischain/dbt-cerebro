# Model review (revisit 2026-06-21): probelab

Baseline `docs/model_review/probelab.md` (dated `2026-06-11`); `16` cases re-verified over `3` rounds against code + live warehouse. Headline: `1` RESOLVED (`PROBELAB-C16`, benign `__pct` doc), `1` CHANGED (`PROBELAB-C05`, NULL/0 hazard dormant while schema mismatch stands), `14` STILL CONFIRMED including the critical missing-`api:`-tag endpoint blackout — `0` NEW, `0` unverifiable, no incident attributions.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| PROBELAB-C01 | - | 5 api_* marts lack `api:`/`granularity:` tags -> silently pass `check_api_tags.py`, absent from cerebro-api routing | critical | CONFIRMED | critical | high | none | 3 |
| PROBELAB-C02 | - | `clients_daily` value=`any_value(floor(__total))` != sum of per-version `__count` -> breaks cross-mart rollup | high | CONFIRMED | high | high | none | 3 |
| PROBELAB-C03 | - | QUIC mart named `_daily` reads `over_7d` rolling window; `toStartOfDay(max_crawl_created_at)` collapses crawl-days | high | CONFIRMED | high | high | none | 3 |
| PROBELAB-C04 | - | `__count` warehouse `Nullable(UInt32)` vs sources.yml `Nullable(Float64)`, uncast passthrough | medium | CONFIRMED | medium | high | none | 3 |
| PROBELAB-C05 | - | staging schema.yml declares non-nullable `UInt64`/`Float64`; warehouse delivers `Nullable(Float64)`; `floor(nullable)` NULL/0 risk | medium | CHANGED | medium | high | none | 3 |
| PROBELAB-C06 | - | QUIC staging selects + `not_null`-tests `crawl_created_at` but no downstream model uses it | medium | CONFIRMED | low | high | none | 3 |
| PROBELAB-C07 | - | `8/12` declared probelab sources have zero downstream ref | low | CONFIRMED | low | high | none | 3 |
| PROBELAB-C08 | - | No uniqueness/PK test on any probelab mart/staging model | low | CONFIRMED | low | high | none | 3 |
| PROBELAB-C09 | - | Marts have no `expose_to_mcp`/`privacy_tier` meta; diverges from API-tier convention | low | CONFIRMED | low | high | none | 3 |
| PROBELAB-C10 | - | `value` column has 3 incompatible semantics across 5 marts, all exposed `agg=sum` | high | CONFIRMED | high | high | none | 3 |
| PROBELAB-C11 | - | QUIC adoption KPI (>30%) cannot be read as a true daily trend (7DMA mislabeled `_daily`) | high | CONFIRMED | high | high | none | 3 |
| PROBELAB-C12 | - | All 12 probelab source freshness values `null` -> no SLA on third-party ingestion | medium | CONFIRMED | medium | high | none | 3 |
| PROBELAB-C13 | - | cerebro-docs Key Models Reference lists 3 nonexistent model names + phantom columns | medium | CONFIRMED | medium | high | none | 3 |
| PROBELAB-C14 | - | `agent_version_type` mis-described as version tiers ("stable or beta") but holds client software names | medium | CONFIRMED | medium | high | none | 3 |
| PROBELAB-C15 | - | All 5 semantic metrics `quality_tier: candidate` yet MCP-exposed `agg=sum` with no gate | low | CONFIRMED | low | high | none | 3 |
| PROBELAB-C16 | - | `__pct` description "expressed as a decimal" allegedly inconsistent with `[0,1]` range | low | RESOLVED | resolved | high | none | 3 |

## Delta vs baseline

### RESOLVED (1)
- `PROBELAB-C16` — `__pct` is in `[0,1]` (semvers `[0.0034, 1.0]`, cloud `[0.0069, 0.672]`, countries `[0.0144, 0.559]`). A `[0,1]` fraction IS a decimal, so the schema description "expressed as a decimal" (`models/probelab/staging/schema.yml:43`) is loosely-worded-but-correct, not inconsistent; no mart selects `__pct`, so consumer risk is nil. The "inconsistent description" premise does not hold — non-issue. No incident.

### CHANGED (1)
- `PROBELAB-C05` — partial-dormant. The schema-mismatch half STANDS: `models/probelab/staging/schema.yml` declares `__count` as `UInt64` (lines `36`/`79`/`127`) while the warehouse delivers `Nullable(Float64)` for all three `*_avg_1d` tables. The "silently returns NULL/0 via `toInt32(floor(nullable __count))`" half is DORMANT: `0` nulls across all three sources over 30d (semvers `0/536`, cloud `0/1773`, countries `0/2057`). One half standing + one half dormant -> CHANGED (not CONFIRMED, not RESOLVED). No incident.

### STILL CONFIRMED (14)
- `PROBELAB-C01` (critical) — all 5 marts carry `tags=['production','probelab','clients','tier1']` only; `429` repo models DO carry `api:` tags; `check_api_tags.py:55-57` (`if not api: continue`) skips untagged models; cerebro-api repo has zero `probelab` refs. The `api:` tag is the discriminator; the 5 marts are never registered as endpoints. No incident.
- `PROBELAB-C02` (high) — `clients_daily` value=`any_value(toInt32(floor(__total)))` (`api_probelab_clients_daily.sql:12`); on `2026-06-20` lighthouse `floor(__total)=284` vs `sum(floor(__count))=281` (delta `+3`, n=10 versions), teku `+1`. Over 14d every multi-version delta lies in `[0, n_versions-1]`, never negative — the algebraic identity `sum(floor(x_i)) <= floor(sum(x_i))` -> pure per-version floor rounding loss. No incident.
- `PROBELAB-C03` (high) — source `probelab_quic_support_over_7d`, date=`toStartOfDay(max_crawl_created_at)` (`api_probelab_clients_quic_daily.sql:10,15`). Over 30d, `30` crawl-days collapse to `23` mart dates; each daily label folds in a 7-day window with up to `6` days of date-label drift. No incident.
- `PROBELAB-C04` (medium) — warehouse `__count`=`Nullable(UInt32)`, `probelab_sources.yml:385`=`Nullable(Float64)`, `marts/schema.yml:242`=`UInt64`, mart passes `__count` uncast (`api_probelab_clients_quic_daily.sql:13`). No per-column type test on `value` (only model-level `elementary.schema_changes` at `marts/schema.yml:290`) -> latent-on-promotion, not an active false-negative. No incident.
- `PROBELAB-C06` (medium -> low) — QUIC staging selects `crawl_created_at` (line `12`), `not_null` test at `staging/schema.yml:166-172`, QUIC mart never references it. Test passes today (`138` distinct non-null crawl timestamps / 30d) — pure green overhead, downgraded to low. No incident.
- `PROBELAB-C07` (low) — exactly 4 of 12 declared sources referenced; `8/8` orphans confirmed zero downstream ref; added together `2025-08-05` (commit `d11805d3`) as a declared-ahead-of-build batch. No incident.
- `PROBELAB-C08` (low) — no `unique`/`unique_combination_of_columns` test in either schema.yml. Grains hold exactly over 30d on all 5 marts: clients `168=168`, version `536=536`, cloud `1773=1773`, country `2057=2057`, quic `364=364`. Coverage gap, not a dup bug. No incident.
- `PROBELAB-C09` (low) — `marts/schema.yml` meta carries only `owner`/`authoritative`; no probelab block or project default in `dbt_project.yml`. cerebro-mcp `loaders/manifest.py:243` hides a model only if `meta.expose_to_mcp IS False` (opt-out), so absence keeps the model visible; exposure flows via the semantic registry. Cosmetic divergence. No incident.
- `PROBELAB-C10` (high) — 3 distinct value exprs persist: `any_value(toInt32(floor(__total)))` (clients), `toInt32(floor(__count))` (cloud/country/version), raw `__count` (quic). `describe_table` confirms even types diverge: quic `value`=`Nullable(UInt32)` vs version `value`=`Nullable(Int32)`. All 5 exposed `agg=sum` under one `value` name; cross-mart sum latent-by-design. No incident.
- `PROBELAB-C11` (high) — only QUIC source is `probelab_quic_support_over_7d` (`sources.yml:373`); no `_avg_1d`/daily QUIC table exists, so no correct daily series is derivable. Lighthouse QUIC-enabled share `~99.6%` (`272/273` on `2026-06-20`) — threshold-flip risk theoretical at saturation, but structural 7DMA-labeled-`_daily` defect stands. No incident.
- `PROBELAB-C12` (medium) — all 12 sources `freshness: null` in `probelab_sources.yml` (forced by `Nullable(DateTime64)` loaded_at); live lag `~70-71h` vs would-be `warn_after: 36h`/`error_after: 72h`. Mart `elementary.freshness_anomalies` watches `date`=`toStartOfDay(max_crawl_created_at)` but is ML-based, not a hard threshold SLA. No incident.
- `PROBELAB-C13` (medium) — `cerebro-docs/docs/models/probelab.md:48-50` list 3 phantom models (`api_probelab_cloud_providers_daily`, `api_probelab_quic_support_daily`, `api_probelab_agent_versions_daily`) + phantom columns; `SELECT count() FROM dbt.api_probelab_cloud_providers_daily` returns ClickHouse Code `60` UNKNOWN_TABLE — copy-paste examples are non-executable. No incident.
- `PROBELAB-C14` (medium) — `agent_version_type` distinct values are client software names (`lighthouse`, `teku`, `nimbus`, `lodestar`, `erigon`, `unknown`); `staging/schema.yml:9` wrongly says "such as stable or beta". Wrong framing confined to dbt schema.yml (lines `9`/`57`/`101`/`149` + marts), NOT in the agent-facing semantic layer (no dimension descriptions in `semantic_models.yml`). No incident.
- `PROBELAB-C15` (low) — all 5 metrics `quality_tier: candidate`, `agg=sum`. cerebro-mcp `index.py:187-188` only gives `+20` ranking to `approved` (candidate de-ranked, not excluded); `semantic.py:1779` permits running a candidate; `quality_tier` IS in the details payload (`semantic.py:1388`) so an agent could self-gate. Advisory-only, partially self-discoverable. No incident.

### NEW (0)
- None.

### UNVERIFIABLE / UNRESOLVED (0)
- None. All 16 cases reached terminal status with self-consistent evidence over >=3 rounds.

## Evidence appendix

### PROBELAB-C01 (api: tag absence)
- Code: all 5 marts `tags=['production','probelab','clients','tier1']` (`api_probelab_clients_*.sql:5`); no `api:`/`granularity:` tag in `models/probelab/`; no probelab block in `dbt_project.yml`.
- `grep -rl "'api:" models/ | wc -l` => `429` models carry `api:` tags. Valid registered peer `api_consensus_zero_blob_commitments_daily` carries `'api:blocks_and_blobs'`,`'granularity:daily'`.
- `check_api_tags.py:55-57`: `api=[t for t in tags if t.startswith('api:')]; if not api: continue`.
- cerebro-api repo: zero refs to `probelab` (and zero to `mixpanel_ga`) — routing is manifest/tag-driven. cerebro-mcp reaches probelab only via semantic registry `index.py:88`.

### PROBELAB-C02 / C10 (value semantics) — shared mart SQL read
- `api_probelab_clients_daily.sql:12` -> `any_value(toInt32(floor(__total)))`; cloud/country/version `:13` -> `toInt32(floor(__count))`; quic `:13` -> raw `__count`.
- SQL: `SELECT toDate(max_crawl_created_at) d, agent_version_type, toInt32(floor(any(__total))) tf, sum(toInt32(floor(__count))) scf, tf-scf delta, count() FROM dbt.stg_crawlers_data__probelab_agent_semvers_avg_1d WHERE toDate(max_crawl_created_at)>=today()-14 GROUP BY d,agent_version_type HAVING count()>1`.
- Returned: `2026-06-20` lighthouse `delta=+3` (10 ver), teku `+1` (4-5 ver), lodestar `0` (3 ver); `2026-06-19` `+3`/`0`/`0`; all deltas in `[0, n_versions-1]`, zero negatives over 14d.
- C10 types: `describe_table` -> `api_probelab_clients_quic_daily.value=Nullable(UInt32)` vs `api_probelab_clients_version_daily.value=Nullable(Int32)`. `semantic_models.yml`: all 5 measures `agg: sum` on `value` (lines `19`/`47`/`72`/`100`/`128`).

### PROBELAB-C03 / C11 (QUIC temporal) — shared source
- SQL: `SELECT count(), uniqExact(toStartOfDay(max_crawl_created_at)), uniqExact(toDate(crawl_created_at)) FROM dbt.stg_crawlers_data__probelab_quic_support_over_7d WHERE toDate(max_crawl_created_at)>=today()-30` => `364` rows, `23` mart dates vs `30` crawl-days vs `138` crawl timestamps. Per-row window span up to `6` days (lighthouse row labeled `2026-06-20` covers crawls `2026-06-16..2026-06-20`).
- Baseline-era full counts: source `458` distinct `crawl_created_at` days vs `441` distinct `max_crawl_created_at` days; mart `6074` rows / `6074` distinct `(date,client,quic)` (clean 1:1, suppressed not duplicated).
- C11: only QUIC source declared is `probelab_quic_support_over_7d` (`sources.yml:373`). Lighthouse `2026-06-20`: `no_quic=1`, `quic4=224`, `quic4+6=47`, `quic6=1` (total `273`) -> enabled share `~99.6%`.

### PROBELAB-C04 (quic __count type)
- `describe_table crawlers_data.probelab_quic_support_over_7d` -> `__count = Nullable(UInt32)`. `sources.yml:385` declares `Nullable(Float64)`; `marts/schema.yml:242` declares `UInt64`; quic mart `:13` passes `__count AS value` uncast. Tests on `value`: `column_anomalies` (`marts/schema.yml:244-255`) + model-level `elementary.schema_changes` (`:290`) — no per-column type contract.

### PROBELAB-C05 (avg_1d nullability)
- `describe_table` x3 `*_avg_1d` -> `__count=Nullable(Float64)`, `__total=Float64`. `staging/schema.yml` declares `__count: UInt64` (lines `36`/`79`/`127`).
- SQL: `SELECT countIf(__count IS NULL), count() ... WHERE toDate(max_crawl_created_at)>=today()-30` -> semvers `0/536`, cloud `0/1773`, countries `0/2057`.

### PROBELAB-C06 (unused not_null)
- QUIC staging `:12` selects `crawl_created_at`; `staging/schema.yml:166-172` `not_null`; QUIC mart references only `max_crawl_created_at`/`agent_version_type`/`quic_support`/`__count`. Grep `crawl_created_at` across `models/`+`semantic/` (excl. `min_`/`max_`): only the quic staging SQL + schema. `138` non-null crawl timestamps / 30d (test green).

### PROBELAB-C07 (orphan sources)
- `grep -rl` per table excl. `probelab_sources.yml` => `0` downstream refs for all 8 (`agent_semvers_over_7d`, `agent_types_avg_1d`, `agent_types_over_7d`, `cloud_provider_over_7d`, `countries_over_7d`, `discv5_stale_records`, `is_cloud_avg_1d`, `is_cloud_over_7d`). `git log -S` -> batch added `2025-08-05` (`d11805d3`); 4 live sources added `2025-04-29` (`c9bacd06`).

### PROBELAB-C08 (no uniqueness test)
- SQL (UNION ALL, 30d): `count()==uniqExact(grain)` on all 5 — clients `(date,client) 168=168`, version `(date,client,version) 536=536`, cloud `(date,client,cloud) 1773=1773`, country `(date,client,country) 2057=2057`, quic `(date,client,quic) 364=364`. No `unique`/`unique_combination_of_columns` in either schema.yml.

### PROBELAB-C09 (meta gap)
- `marts/schema.yml` meta = `{owner, authoritative:false}` only; no probelab block in `dbt_project.yml`. cerebro-mcp `loaders/manifest.py:243` hides only when `meta.expose_to_mcp IS False`. Peer `api_mixpanel_ga_users_daily.sql:5` sets `meta={'expose_to_mcp':False,'privacy_tier':'internal'}`.

### PROBELAB-C12 (freshness null)
- `probelab_sources.yml`: 12 `freshness: null` lines (tables at lines `143-374`) overriding source-level `warn_after: 36h`/`error_after: 72h` (`:8`); intentional comment `:141-142`; loaded_at = `Nullable(DateTime64(6))`.
- SQL: `SELECT dateDiff('hour', max(max_crawl_created_at), now())` -> `~70-71h` (max crawl `2026-06-20`). Mart `freshness_anomalies` keyed on `date` (`marts/schema.yml:66-73`).

### PROBELAB-C13 (phantom doc names)
- `cerebro-docs/docs/models/probelab.md:48-50` + example queries `:58,67,76,91` name 3 nonexistent models with phantom cols (`dt`, `node_count`, `pct_of_total`, `quic_nodes`, `agent_name`). Real marts: `api_probelab_clients_cloud/quic/version_daily` (cols `date`/`client`/dimension/`value`). `SELECT count() FROM dbt.api_probelab_cloud_providers_daily` -> Code `60` UNKNOWN_TABLE. Auto-gen table (`:23-27`) + `dashboard/sectors.md:193-197` use correct names.

### PROBELAB-C14 (agent_version_type mislabel)
- SQL: `SELECT DISTINCT agent_version_type FROM crawlers_data.probelab_agent_semvers_avg_1d WHERE toDate(max_crawl_created_at)>=today()-14` -> `{erigon, lighthouse, lodestar, nimbus, teku, unknown}`. `staging/schema.yml:9` "Categorizes the type of agent version, such as stable or beta". `semantic_models.yml` carries no dimension descriptions (only auto-gen metric blurbs).

### PROBELAB-C15 (candidate tier)
- `semantic_models.yml`: all 5 models + 5 metrics `quality_tier: candidate`; measures `agg: sum`. cerebro-mcp `index.py:187-188` `+20` only for `approved`; `semantic.py:1779` allows candidate run; `semantic.py:1388` emits `['quality_tier', metric.get('quality_tier','')]`.

### PROBELAB-C16 (pct decimal)
- SQL: `SELECT min(__pct), max(__pct)` over 3 `*_avg_1d` (30d) -> semvers `[0.0034, 1.0]`, cloud `[0.0069, 0.672]`, countries `[0.0144, 0.559]`. `staging/schema.yml:43` "expressed as a decimal". Grep of 5 mart SQL: none reference `__pct`.

## Review log (>=3 rounds per case)

- **PROBELAB-C01**: R1 CONFIRMED (tags/CI guard read) -> challenge: prove absence from cerebro-api routing -> R2 CONFIRMED (zero probelab refs in cerebro-api; MCP via semantic only) -> challenge: side-by-side vs a registered peer -> R3 CONFIRMED (suggested peer mixpanel was itself untagged; valid peer is `api_consensus_zero_blob_commitments_daily`; `429` tagged models). Settled critical.
- **PROBELAB-C02**: R1 CONFIRMED (`286` vs `282` -> `284` vs `281`) -> challenge: per-client signed deltas across clients/days -> R2 CONFIRMED (`+3`/`+1`/`0` by version count) -> challenge: prove directionality guaranteed -> R3 CONFIRMED (all deltas `[0,n-1]`, zero negatives, floor identity). Settled high.
- **PROBELAB-C03**: R1 CONFIRMED (`458`->`441`, nuance: not one date) -> challenge: rows_in vs rows_out for a re-stamped day -> R2 CONFIRMED (`16->16`, suppressed not duplicated) -> challenge: express drift in days -> R3 CONFIRMED (up to `6` days drift, `30`->`23` over 30d). Settled high.
- **PROBELAB-C04**: R1 CONFIRMED (UInt32/Float64/UInt64 mismatch) -> challenge: is mismatch end-to-end -> R2 CONFIRMED (3-way along source->mart, uncast) -> challenge: is schema_changes attached per-column -> R3 CONFIRMED (only model-level test; latent-on-promotion). Settled medium.
- **PROBELAB-C05**: R1 CONFIRMED (Nullable(Float64) vs UInt64) -> challenge: live NULL incidence -> R2 CHANGED (`0` nulls / 30d -> NULL/0 half dormant) -> challenge: lock sole surviving half -> R3 CHANGED (schema mismatch stands at lines `36`/`79`/`127`; NULL/0 dormant `0/4366`). Settled CHANGED/medium.
- **PROBELAB-C06**: R1 CONFIRMED (medium baseline) -> challenge: exhaustive scope of `crawl_created_at` -> R2 CONFIRMED, downgraded low (only staging refs) -> challenge: is test green today -> R3 CONFIRMED (green no-op, `0` nulls). Settled low.
- **PROBELAB-C07**: R1 CONFIRMED (`8/12`) -> challenge: dead vs declared-ahead -> R2 CONFIRMED (batch `2025-08-05`) -> challenge: are orphan tables populated -> R3 CONFIRMED (ingested-but-unmodeled, low). Settled low.
- **PROBELAB-C08**: R1 CONFIRMED (no unique test; 2 marts spot-check) -> challenge: grain over full 30d -> R2 CONFIRMED (`536=536`/`168=168`) -> challenge: extend to all 5 -> R3 CONFIRMED (5/5 zero violations). Settled low.
- **PROBELAB-C09**: R1 CONFIRMED (no meta, no default) -> challenge: which layer gates MCP -> R2 CONFIRMED (registry path; cosmetic) -> challenge: does builder read model meta -> R3 CONFIRMED (loader opt-out only at `:243`). Settled low.
- **PROBELAB-C10**: R1 CONFIRMED (3 exprs, agg=sum) -> challenge: is cross-mart sum reachable -> R2 CONFIRMED (siloed simple metrics; latent-by-design) -> challenge: do value types diverge -> R3 CONFIRMED (`Nullable(UInt32)` vs `Nullable(Int32)`). Settled high.
- **PROBELAB-C11**: R1 CONFIRMED (over_7d labeled _daily) -> challenge: alternative daily series exists -> R2 CONFIRMED (no non-windowed QUIC source) -> challenge: KPI threshold-flip materiality -> R3 CONFIRMED (`~99.6%` saturation; structural defect stands). Settled high.
- **PROBELAB-C12**: R1 CONFIRMED (freshness null, 3-day lag) -> challenge: is dbt freshness feasible on Nullable loaded_at -> R2 CONFIRMED (forced workaround, lag `70h`) -> challenge: what monitoring exists -> R3 CONFIRMED (ML anomaly on `date`, no threshold SLA, lag `70h`). Settled medium.
- **PROBELAB-C13**: R1 CONFIRMED (3 phantom names) -> challenge: doc-section scope -> R2 CONFIRMED (Key Models Reference only; phantom cols absent from marts) -> challenge: prove non-executable -> R3 CONFIRMED (Code `60` UNKNOWN_TABLE). Settled medium.
- **PROBELAB-C14**: R1 CONFIRMED (client names not tiers) -> challenge: full doc-side scope -> R2 CONFIRMED (wrong framing pervasive in schema.yml) -> challenge: does it reach the semantic layer -> R3 CONFIRMED (confined to schema.yml; no semantic dimension descriptions). Settled medium.
- **PROBELAB-C15**: R1 CONFIRMED (all candidate, agg=sum) -> challenge: is the tier an enforced gate -> R2 CONFIRMED (advisory; ranking-only) -> challenge: is tier visible at query time -> R3 CONFIRMED (in details payload; self-discoverable, no hard gate). Settled low.
- **PROBELAB-C16**: R1 CONFIRMED (range `[0,1]`) -> challenge: is "decimal" actually wrong -> R2 RESOLVED (loosely-worded-but-correct; no mart exposes `__pct`) -> R3 RESOLVED (re-measured `[0,1]`, no further challenge). Settled RESOLVED.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P0 (ESCALATE) | Add `api:probelab` + `granularity:daily` tags to all 5 marts so `check_api_tags.py` validates them and cerebro-api registers the endpoints (currently invisible). | `models/probelab/marts/api_probelab_clients_{daily,version_daily,cloud_daily,country_daily,quic_daily}.sql` |
| P1 (KEEP) | Unify the `value` semantics across marts (pick one of total-denominator vs summed-`__count`; floor consistently; cast QUIC `__count`), or rename/disambiguate so a `sum(value)` rollup is well-defined; document each metric's denominator. | all 5 marts + `semantic/authoring/probelab/semantic_models.yml` |
| P1 (KEEP) | Rename/redefine the QUIC mart off the 7-day rolling source or surface the windowing in name/description; the `_daily` label misrepresents a 7DMA (up to `6` days date drift) and no true daily QUIC source exists. | `models/probelab/marts/api_probelab_clients_quic_daily.sql`, `probelab_sources.yml` |
| P2 (KEEP) | Add a threshold freshness SLA (custom/elementary test on `max_crawl_created_at`, e.g. warn `36h`/error `72h`) since `freshness:` is forced null by Nullable loaded_at; live lag is `~70h` unalerted. | `models/probelab/probelab_sources.yml` |
| P2 (KEEP) | Fix cerebro-docs Key Models Reference: replace 3 phantom model names + phantom columns (queries error Code `60`) with the real `api_probelab_clients_*` models/columns. | `cerebro-docs/docs/models/probelab.md:48-91` |
| P2 (KEEP) | Correct `agent_version_type` descriptions from "version type / stable or beta" to "client software name" (values are lighthouse/teku/nimbus/lodestar/erigon/unknown). | `models/probelab/staging/schema.yml:9,57,101,149`, `models/probelab/marts/schema.yml` |
| P2 (KEEP) | Align declared types to warehouse reality: `__count`=`Nullable(UInt32)` (quic) and `Nullable(Float64)` (avg_1d) — fixes schema_changes false-baselines and the latent type contract. | `probelab_sources.yml:385`, `staging/schema.yml:36,79,127`, `marts/schema.yml:242` |
| P3 (KEEP) | Add `unique_combination_of_columns` tests on the verified grains of all 5 marts (correct today, untested). | `models/probelab/marts/schema.yml` |
| P3 (KEEP) | Drop the unused `not_null` test on `crawl_created_at` (green no-op, no downstream consumer) or wire the column into a consumer. | `models/probelab/staging/schema.yml:166-172` |
| P3 (KEEP) | Prune or model the `8/12` orphaned source declarations (declared-ahead-of-build since `2025-08-05`). | `models/probelab/probelab_sources.yml` |
| P3 (KEEP) | Add `expose_to_mcp`/`privacy_tier` meta and promote semantic metrics past `quality_tier: candidate` after the value-semantics fix (currently MCP-exposed `agg=sum` ungated). | `models/probelab/marts/schema.yml`, `semantic/authoring/probelab/semantic_models.yml` |
| DROP | `PROBELAB-C16` `__pct` description — RESOLVED as a non-issue; no action needed. | `models/probelab/staging/schema.yml:43` |
