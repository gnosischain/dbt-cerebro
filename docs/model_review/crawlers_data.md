# Model review: crawlers_data

**Convergence:** converged in 2 rounds — all three blocking unknowns (FINAL feasibility on dune_labels, dune_prices ingestion timestamp availability, and labels_dex non-DEX row scope) resolved with direct warehouse evidence; both agents consistent.

---

## Scope and inventory

The `crawlers_data` sector is a foundational reference-data layer. It ingests third-party datasets (Dune Analytics, CoW Protocol API, aboutcircles bot-analytics API) and exposes cleaned, normalised models consumed widely across the platform. It produces no KPIs of its own.

| Layer | Models | Purpose |
|---|---|---|
| Staging (views) | `stg_crawlers_data__dune_labels`, `stg_crawlers_data__dune_prices`, `stg_crawlers_data__dune_bridge_flows`, `stg_crawlers_data__dune_bridge_flows_v2` (dev), `stg_crawlers_data__dune_gno_supply`, `stg_crawlers_data__cow_api_trade_fees`, `stg_crawlers_data__circles_blacklisted` | Clean and gate source tables |
| Intermediate (tables) | `int_crawlers_data_labels`, `int_crawlers_data_labels_dex` | Deduplication, sector assignment, performance slice |
| Fact (table) | `fct_crawlers_data_distinct_projects_sectors` | ReplacingMergeTree aggregate |
| Marts (views) | `api_crawlers_data_distinct_projects_sectors_totals`, `api_crawlers_data_gno_supply_daily` | API endpoints |

Total: 12 SQL model files across 3 layers. Sources: `dune_labels`, `dune_prices`, `dune_bridge_flows`, `dune_gno_supply`, `cow_api_trade_fees`, `circles_blacklisted` — all in the `crawlers_data` database.

---

## Business context

The unit answers five distinct questions:

1. **Address labelling** — "What project/sector owns this on-chain address?" via `int_crawlers_data_labels` and `int_crawlers_data_labels_dex`. Consumed by execution/transactions, execution/tokens, execution/live-trades, execution/ubo, execution/pools, and the MCP `resolve_address` tool. Source: ~5.45M rows from Dune Analytics.

2. **Token prices** — "What was the USD price of token X on date D?" via `stg_crawlers_data__dune_prices`, feeding `int_execution_token_prices_daily` as a lower-priority fallback behind native Chainlink prices. Now demoted to fallback for pre-2021 history and tokens without a Chainlink oracle (GBPe, BRLA, BRZ, COW, SAFE).

3. **GNO supply** — "How much GNO is in circulation/staked/locked on date D?" via `api_crawlers_data_gno_supply_daily` (tier1, granularity:daily).

4. **Taxonomy coverage** — "How many distinct projects and sectors are labelled?" via `api_crawlers_data_distinct_projects_sectors_totals` (tier0, granularity:total), a single-row KPI used in dashboard summary cards.

5. **Circles sybil filtering** — "Is this Circles avatar a known bot?" via `stg_crawlers_data__circles_blacklisted`, consumed by three gnosis_app WAU/WEAU marts and two Circles ranking models. The table is fully replaced on every daily run — no point-in-time history.

**Canonical definitions of note:**

- *Label normalisation*: six-stage regex pipeline (s1–s7) strips version suffixes, address-hex tails, and contract-type suffixes from raw Dune labels, then maps to a canonical project name via a two-level `multiIf` ruleset. Fallback to 'ERC20' if cleaned name is empty or still contains a 0x address.
- *Sector taxonomy*: 18 buckets assigned in `int_crawlers_data_labels` by regex match on the canonical project name (EOAs, ERC20 Tokens, DEX, Lending & Yield, Bridges, Payments, etc.).
- *Address dedup rule*: `row_number() OVER (PARTITION BY address ORDER BY lower(project) = 'gpay' DESC, project ASC)` — intent is Gnosis Pay priority, but the guard string is misaligned with the canonical project name (see Implementation assessment below).
- *`int_crawlers_data_labels_dex`*: documented in `schema.yml` as "DEX-only slice restricted to sector = DEX", but the actual SQL filter is `WHERE sector NOT IN ('EOAs', 'ERC20 Tokens', 'Wallets & AA', 'Payments')` — 14 of 18 sector buckets pass through.

---

## Implementation assessment

### High severity

**`int_crawlers_data_labels` — partition design approaching CH Cloud 100-partition hard block**
`models/crawlers_data/intermediate/int_crawlers_data_labels.sql`

`PARTITION BY toStartOfMonth(introduced_at)`. The source `dune_labels` spans 94 distinct months (2018-08 to 2026-04), confirmed by warehouse query on 5,455,162 rows. CH Cloud blocks a full-table INSERT at >100 partitions (code 252) and rejects attempts to raise `max_partitions_per_insert_block` (code 452). At current growth, a full `dbt run --full-refresh` with approximately 6 more months of data will hard-fail. Project convention (per `feedback_ch_cloud_partition_cap.md`) requires `toStartOfYear` for wide-history tables. This is the only time-critical fix in the unit — it must land before the next full rebuild.

**`stg_crawlers_data__dune_bridge_flows_v2` — references non-existent columns from transaction-level source**
`models/crawlers_data/staging/stg_crawlers_data__dune_bridge_flows_v2.sql`

The v2 staging model reads `source('crawlers_data', 'dune_bridge_flows')` and references columns `date` and `txs`. The source is transaction-level with a `timestamp` column, not a pre-aggregated daily schema. The model will fail at runtime. It is tagged `dev` with one dev-only downstream consumer (`int_bridges_flows_daily_v2`) and no production consumer, limiting blast radius. The model should either be removed or held off the main branch until the upstream Dune query is updated to the pre-aggregated schema it expects.

### Medium severity

**`stg_crawlers_data__dune_prices` — `anyLast(price)` deduplication is non-deterministic and cannot be upgraded**
`models/crawlers_data/staging/stg_crawlers_data__dune_prices.sql`

Source `dune_prices` has 2,577 duplicate `(block_date, symbol)` grain pairs across 38,976 total rows. `anyLast()` selects the last-inserted row with no ordering guarantee. `DESCRIBE TABLE crawlers_data.dune_prices` confirms exactly three columns (`block_date Date`, `symbol LowCardinality(String)`, `price Float64`) — no ingestion timestamp exists, so upgrading to `argMax()` requires a source ETL schema change. The deterministic interim mitigation is replacing `anyLast(price)` with `max(price)` (or `min(price)`) with a SQL comment documenting the explicit arbitrary choice.

**`sources.yml` — `dune_labels` freshness thresholds misconfigured for weekly refresh cadence**
`models/crawlers_data/sources.yml`

`sources.yml` inherits the source-level default of `warn_after 18h / error_after 30h` for `dune_labels`. `cerebro-docs/docs/models/crawlers.md` (Data Freshness table) explicitly documents: "Dune labels | Weekly | 1-7 days". A weekly-refreshed source permanently exceeds the 30h error threshold on every non-refresh day, generating persistent false alerts. The table-level override should be `warn_after 7d / error_after 8d`.

**`int_crawlers_data_labels` — dead-letter dedup priority guard**
`models/crawlers_data/intermediate/int_crawlers_data_labels.sql`

The `row_number()` `ORDER BY` is `lower(project) = 'gpay' DESC, project ASC`. The stg pipeline's explicit canon rule (line 144 of `stg_crawlers_data__dune_labels.sql`) maps all Gnosis Pay label variants to the string `'Gnosis Pay'` (title case). `lower('Gnosis Pay') = 'gnosis pay'`, not `'gpay'`. The guard only fires for addresses whose raw Dune label passed through stg without matching the canon rule — an edge case. For the common canonical case (`'Gnosis Pay'` vs. another project), dedup silently falls through to alphabetical order. `'Gnosis Pay'` alphabetically precedes `'Safe'` and `'Uniswap'`, so those collisions resolve correctly by accident. Any project name alphabetically preceding `'Gnosis Pay'` (Aave, Angle, Balancer, CowSwap) colliding on the same address would silently win. Fix: change the guard to `lower(project) = 'gnosis pay'`.

**`int_crawlers_data_labels_dex` — misnamed and misdescribed; 72.4% of rows are non-DEX sectors**
`models/crawlers_data/intermediate/int_crawlers_data_labels_dex.sql`

`WHERE sector NOT IN ('EOAs', 'ERC20 Tokens', 'Wallets & AA', 'Payments')` retains 14 of 18 sector buckets. Warehouse query confirmed: 11,844 total rows; only 3,269 (27.6%) are `sector='DEX'`. The remaining 8,575 rows span Infrastructure & DevTools (3,505), Others (3,023), NFTs & Marketplaces (1,208), Bridges (98), and 10 additional sectors. The `schema.yml` description reads "DEX-only slice...restricted to sector = DEX" — factually incorrect.

The WHERE clause is correct for its one confirmed consumer (`api_execution_live_trades`), which joins on `tx.to_address` to populate an `aggregator` column — a use case that legitimately needs any non-noise protocol label, not DEX-only. Only the model name and description require correction. Recommended rename: `int_crawlers_data_labels_attributed` or `int_crawlers_data_labels_non_noise`.

**`stg_crawlers_data__dune_labels` `schema.yml` — 13 CTE-internal columns documented as output columns**
`models/crawlers_data/staging/stg_crawlers_data__dune_labels.sql`

The staging view's final `SELECT` outputs four columns: `address`, `project`, `project_raw`, `introduced_at`. The `schema.yml` additionally documents 13 intermediate CTE columns (`agg`, `label_raw`, `s1`–`s7`, `looks_like_token_tail`, `wl_symbol`, `project_canon`, `label`) that do not exist in the output. The phantom `label` column is particularly confusing. Any schema-drift tooling will alert indefinitely. Trim `schema.yml` to match actual output columns.

**`fct_crawlers_data_distinct_projects_sectors` — ReplacingMergeTree without version column**
`models/crawlers_data/marts/fct_crawlers_data_distinct_projects_sectors.sql`

Materialised as `ReplacingMergeTree()` with `order_by=(project, sector)`, no version column. CH deduplicates only on asynchronous background merges. Queries without `FINAL` may see duplicate `(project, sector)` pairs. The downstream `api_crawlers_data_distinct_projects_sectors_totals` runs `countDistinct()` against this table — a tier0 KPI endpoint that may return inflated counts until a background merge fires. No unique test covers the `(project, sector)` grain in `schema.yml`.

### Low severity

**`int_crawlers_data_labels` and `int_crawlers_data_labels_dex` — `unique_key` config is cosmetic**
In dbt-clickhouse, `unique_key` sets `ORDER BY` but does not enforce row-level uniqueness. The SQL dedup via `row_number()` is correct; the config is misleading but harmless. A comment would reduce future confusion.

**`api_crawlers_data_distinct_projects_sectors_totals` — `as_of_date` description is wrong; opaque column names**
`models/crawlers_data/marts/api_crawlers_data_distinct_projects_sectors_totals.sql`

`schema.yml` states `as_of_date` = "max date in the underlying data". Actual SQL uses `today()` (wall-clock at query time). `value1` (distinct project count) and `value2` (distinct sector count) are semantically opaque and cast to `Float64`. Rename to `project_count` / `sector_count`. The semantic model wraps these with `sum()` aggregation — mathematically wrong if the view ever returns more than one row; change to `max()`.

**`stg_crawlers_data__dune_gno_supply` — raw label passthrough, no normalisation**
`models/crawlers_data/staging/stg_crawlers_data__dune_gno_supply.sql`

Trivial passthrough with no `lower()` on `label`, no type casts, no `accepted_values` test. Any upstream Dune query renaming a category (e.g. `'Circulating'` vs `'circulating'`) silently changes API output. Adding `lower(label)` in staging and an `accepted_values` test would provide a safety net.

**`int_crawlers_data_labels_dex` — `introduced_at` stripped from output**
Downstream models cannot apply a label-age filter. Not a blocker for the current live-trades use case but limits future utility.

---

## Business-logic assessment

### High severity

**`int_crawlers_data_labels_dex` — 98 bridge contract addresses served as aggregators in `api_execution_live_trades`**
`models/crawlers_data/intermediate/int_crawlers_data_labels_dex.sql`, `models/execution/live/marts/api_execution_live_trades.sql`

Confirmed by warehouse query: 98 addresses with `sector='Bridges'` (Hop Protocol, Stargate, LI.FI, Bungee, etc.) pass through `int_crawlers_data_labels_dex` and populate the `aggregator` column in `api_execution_live_trades`. Whether this is a bug or intentional depends on the business definition of "aggregator" — if the intent is "any non-noise protocol a trade was routed through", these 98 rows are correct; if the intent is strictly DEX aggregators, they are a misclassification. The model name implies DEX-only, suggesting the former interpretation has not been formally documented. A documented business rule and a corrected model name resolve this ambiguity.

### Medium severity

**`int_crawlers_data_labels` — Gnosis Pay dedup priority is latently unreliable**
`models/crawlers_data/intermediate/int_crawlers_data_labels.sql`

The intended rule (Gnosis Pay addresses always resolve to 'Gnosis Pay' when multiple labels compete) is not reliably enforced by the `lower(project) = 'gpay'` guard (see Implementation section). For today's known collisions ('Gnosis Pay' vs. 'Safe', 'Uniswap'), alphabetical sorting happens to produce the correct result. Any project name alphabetically preceding 'Gnosis Pay' (Aave, Angle, Balancer, CowSwap) colliding on the same address would silently override Gnosis Pay attribution. The `resolve_address` MCP tool and all downstream execution models inherit this latent risk.

**`fct_crawlers_data_distinct_projects_sectors` — may double-count (project, sector) pairs before background merge, inflating the tier0 KPI**
`models/crawlers_data/marts/fct_crawlers_data_distinct_projects_sectors.sql`, `models/crawlers_data/marts/api_crawlers_data_distinct_projects_sectors_totals.sql`

ReplacingMergeTree without a version column means duplicates are only removed asynchronously. `api_crawlers_data_distinct_projects_sectors_totals` runs `countDistinct(project)` and `countDistinct(sector)` against this table. If unmerged duplicate parts exist, both dashboard KPIs inflate. The view should read `fct_crawlers_data_distinct_projects_sectors FINAL` or be rewritten to query `int_crawlers_data_labels` directly.

### Low severity

**`stg_crawlers_data__dune_prices` — fallback status not visible in the crawlers_data unit**

Demotion to fallback (behind native Chainlink) is documented only in `int_execution_token_prices_daily` and `docs/native_token_prices_build_plan.md`. No deprecation tag or comment in the staging model. Planned decommission is not reflected in any schema annotation.

**`api_crawlers_data_gno_supply_daily` — GNO supply label categories undocumented and unnormalised**

Three distinct label values confirmed by warehouse query, but their names are undocumented in dbt. Any upstream Dune query renaming a category silently changes API output with no dbt-layer validation.

---

## Data findings

Seven warehouse queries confirmed key numbers:

| Query | Result |
|---|---|
| `dune_labels` total rows | 5,455,162 |
| `dune_labels` distinct lowercase addresses | 5,441,739 (13,423 duplicates) |
| `dune_labels` distinct months (`introduced_at`) | 94 (2018-08 to 2026-04) |
| `dune_labels FINAL` | Exception code 181 — SharedMergeTree does not support FINAL |
| `dune_prices` total rows | 38,976 across 23 symbols |
| `dune_prices` duplicate `(block_date, symbol)` pairs | 2,577 |
| `dune_prices` columns (`DESCRIBE TABLE`) | 3 columns: `block_date`, `symbol`, `price` — no ingestion timestamp |
| `dune_gno_supply` | 8,791 rows, 3 distinct labels, 0 null/zero supply, 0 duplicate grain |
| `cow_api_trade_fees` after FINAL | 2,409,847 rows, 0 duplicate `order_uid` |
| `int_crawlers_data_labels` rows with `sector='Bridges'` | 98 |
| `int_crawlers_data_labels_dex` total rows | 11,844 (3,269 DEX, 8,575 non-DEX) |
| All sources max `block_date` / `loaded_at` | 2026-06-10 (D-1, within freshness thresholds) |

`stg_crawlers_data__dune_bridge_flows_v2` was not queried — tagged dev, confirmed broken schema against source.

---

## Pros / Cons

**Pros**

- Label pipeline correctly handles 5.45M source rows with a six-stage normalisation chain that is well-structured and auditable.
- Row-level dedup in `int_crawlers_data_labels` is implemented correctly in SQL (`row_number()` partitioned by address, producing a clean-grain output table).
- Source freshness monitoring is configured for all five source tables with appropriate `loaded_at_field` mappings.
- `dune_labels` uses SharedMergeTree (CH Cloud native engine) — FINAL is architecturally unsupported, so the "missing FINAL" risk from the initial assessment does not apply; the 13,423 duplicate addresses are confirmed source-data duplicates handled correctly in the int layer.
- `stg_crawlers_data__cow_api_trade_fees` correctly applies FINAL and delivers a unique-grain output (0 duplicate `order_uid` confirmed).
- `dune_gno_supply` source is clean: zero nulls, zero zeros, zero duplicate grain, fresh as of D-1.
- `api:` tag and `granularity:` tag compliance is clean across all mart models.
- `dev` tag on `stg_crawlers_data__dune_bridge_flows_v2` correctly limits blast radius — no production consumer is exposed to the broken schema.

**Cons**

- `int_crawlers_data_labels` partitions by month on a 94-month span — six months away from the CH Cloud 100-partition hard block on full rebuild.
- `stg_crawlers_data__dune_bridge_flows_v2` references columns that do not exist in the source and will fail at runtime.
- `anyLast(price)` dedup for 2,577 duplicate price pairs is non-deterministic; no ingestion timestamp column exists to upgrade it.
- Dedup tie-break `lower(project) = 'gpay'` never fires for canonical 'Gnosis Pay' addresses — guard is a dead letter.
- `dune_labels` freshness thresholds (18h warn / 30h error) are misconfigured for a weekly refresh cadence — permanent false-alerting in production.
- `int_crawlers_data_labels_dex` contains 8,575 non-DEX rows (72.4% of the table) but is named and documented as a DEX-only slice — misleading to every downstream engineer.
- `api_crawlers_data_distinct_projects_sectors_totals` `schema.yml` incorrectly states `as_of_date` = "max date in underlying data"; actual code uses `today()`.
- `fct_crawlers_data_distinct_projects_sectors` uses ReplacingMergeTree with no version column and has no unique test on its `(project, sector)` grain.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| IMMEDIATE (pre-next-full-rebuild) | Change `PARTITION BY toStartOfMonth(introduced_at)` to `toStartOfYear(introduced_at)` in `int_crawlers_data_labels`. At 94 months today, the next full rebuild is ~6 months from a hard CH Cloud code 252 failure. | `models/crawlers_data/intermediate/int_crawlers_data_labels.sql` |
| HIGH | Rename `int_crawlers_data_labels_dex` to `int_crawlers_data_labels_attributed` (or `_non_noise`) and rewrite `schema.yml` description to "Non-noise address-to-project lookup — excludes EOAs, ERC20 Tokens, Wallets & AA, and Payments; used to attribute the contract a trade was routed through." Do not change the WHERE clause — it is correct for its consumer. | `models/crawlers_data/intermediate/int_crawlers_data_labels_dex.sql` |
| HIGH | Fix the dedup tie-break guard from `lower(project) = 'gpay'` to `lower(project) = 'gnosis pay'`. The current guard is dead code for the canonical project string produced by the stg pipeline. | `models/crawlers_data/intermediate/int_crawlers_data_labels.sql` |
| HIGH | Add a table-level freshness override for `dune_labels` in `sources.yml`: `warn_after: 7d / error_after: 8d`. The current inherited 18h/30h thresholds generate permanent false alerts against a weekly-refreshed source. | `models/crawlers_data/sources.yml` |
| MEDIUM | Remove or quarantine `stg_crawlers_data__dune_bridge_flows_v2` and `int_bridges_flows_daily_v2`. Both are dev-tagged, broken against the source schema, and have no production consumers. | `models/crawlers_data/staging/stg_crawlers_data__dune_bridge_flows_v2.sql` |
| MEDIUM | Replace `anyLast(price)` with `max(price)` in `stg_crawlers_data__dune_prices` and add a SQL comment documenting the explicit choice. File a follow-on request to add an `ingested_at` column to the `dune_prices` ETL for a future `argMax()` upgrade. | `models/crawlers_data/staging/stg_crawlers_data__dune_prices.sql` |
| MEDIUM | Trim `stg_dune_labels` `schema.yml` to the four actual output columns (`address`, `project`, `project_raw`, `introduced_at`). Remove all 13 CTE-internal column entries. | `models/crawlers_data/staging/stg_crawlers_data__dune_labels.sql` |
| MEDIUM | Rewrite `api_crawlers_data_distinct_projects_sectors_totals` to query `int_crawlers_data_labels` directly, or add `FINAL` to the read of `fct_crawlers_data_distinct_projects_sectors`. The ReplacingMergeTree without a version column may serve inflated counts to a tier0 API endpoint. | `models/crawlers_data/marts/fct_crawlers_data_distinct_projects_sectors.sql`, `models/crawlers_data/marts/api_crawlers_data_distinct_projects_sectors_totals.sql` |
| LOW | Rename `value1`/`value2` to `project_count`/`sector_count`; fix `as_of_date` description to "Wall-clock date when the view is queried (today())"; change semantic model aggregation type from `sum()` to `max()`. | `models/crawlers_data/marts/api_crawlers_data_distinct_projects_sectors_totals.sql` |
| LOW | Add `lower(label)` to `stg_crawlers_data__dune_gno_supply` and an `accepted_values` test in `schema.yml` to catch upstream Dune query label renames before they surface in the API. | `models/crawlers_data/staging/stg_crawlers_data__dune_gno_supply.sql` |

---

## Open disagreements

None. Review converged.

---

## Review log

| Round | Agent | Challenge / Resolution |
|---|---|---|
| R1 → R2 | Inspector | Challenged: is `dune_labels` a ReplacingMergeTree requiring FINAL? — **Resolved**: `dune_labels FINAL` raises exception code 181 (SharedMergeTree does not support FINAL); risk closed. |
| R1 → R2 | Inspector | Challenged: can `anyLast()` be upgraded to `argMax()` for deterministic price dedup? — **Resolved**: `DESCRIBE TABLE` confirms only 3 columns, no ingestion timestamp; argMax() not feasible without ETL change. |
| R1 → R2 | Inspector | Challenged: quantify non-DEX scope of `int_crawlers_data_labels_dex` and confirm production exposure. — **Resolved**: 11,844 total rows, 8,575 (72.4%) non-DEX confirmed; sole consumer is `api_execution_live_trades`; filter correct for use case, rename only required. |
| R1 → R2 | Context | Challenged: is the dedup guard `lower(project) = 'gpay'` actually dead code for canonical Gnosis Pay addresses? — **Resolved**: stg pipeline line 144 maps all Gnosis Pay variants to 'Gnosis Pay' (title case); `lower('Gnosis Pay') = 'gnosis pay'` ≠ `'gpay'`; guard confirmed dead letter. |
| R1 → R2 | Context | Challenged: which side of the freshness / refresh cadence conflict is authoritative? — **Resolved**: `cerebro-docs/docs/models/crawlers.md` Data Freshness table is authoritative (weekly / 1-7 day lag); `sources.yml` 18h/30h thresholds are misconfigured. |
| R1 → R2 | Context | Challenged: is `int_crawlers_data_labels_dex` filter wrong for its actual use case? — **Resolved**: sole consumer confirmed as `api_execution_live_trades` (grep); join populates `aggregator` column for any non-noise protocol; filter is correct for use case; rename only. |
