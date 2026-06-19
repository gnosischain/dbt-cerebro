# Model review: execution/pools

**Convergence:** converged in 1 round — both shards (staging-intermediate and marts) reached identical conclusions on all material issues; no inspector challenges went unresolved.

---

## Scope and inventory

| Layer | Count | Key models |
|---|---|---|
| Staging | 18 | `stg_pools__*` — per-protocol event parsing and pool registry |
| Intermediate | 14 | `int_execution_pools_*` — per-protocol daily balance/fee accumulation, unified combinator, trades enrichment |
| Marts (fact) | 12 | `fct_execution_pools_*`, `fct_execution_trades_*` — daily grain and snapshots |
| Marts (API views) | 19 | `api_execution_pools_*`, `api_execution_trades_stats_*` — consumer-facing endpoints |
| **Total** | **63** | — |

Protocols: Uniswap V3, Swapr V3 (Algebra fork), Balancer V2, Balancer V3. Primary outputs: pool-level daily TVL/fee APR/LVR/net APR, snapshot KPIs, LP unique-address counts, and a dev-tagged cross-chain trade-stats sub-section. Full-refresh start dates range from 2021-01-01 (Balancer V2) to 2024-01-01 (Balancer V3).

---

## Business context

This unit is the canonical DEX liquidity-pool analytics layer for Gnosis Chain. It answers four categories of questions:

1. **LP yield assessment** — fee APR (7-day trailing, annualised) and net APR (fee APR minus LVR adverse-selection cost) per pool per token, feeding both the pools dashboard and `fct_execution_yields_opportunities_latest` (the Yields dashboard).
2. **Pool health / liquidity depth** — TVL per token/pool, 7-day unique LP counts, add/remove activity.
3. **DEX trading activity** — daily and 7-day volume, swap counts, aggregator share, hop and size distributions.
4. **MCP graph traversal** — LP-to-pool and pool-to-token edges for the cerebro graph explorer.

**Canonical definitions (from `analytics.md` and schema.yml):**

- **TVL**: sum of `reserve_amount * price_usd` per token per pool; reserve excludes unclaimed fees. Balancer V2 does not yet separate fees from reserves (`fee_amount = 0`). Price source: Dune oracle prices via ASOF join on `int_execution_token_prices_daily`.
- **fee_apr_7d**: `(fees_usd_7d / tvl_usd_7d_avg) * (365/7) * 100`. NULL when < 3 days in window, avg TVL <= $500, or protocol is Balancer V2. Double-smoothed with a 7-day moving average in `fct_execution_pools_daily` (~14-day effective window).
- **Gross fees**: computed from swap events at execution time (UniV3: `swap_in * fee_ppm / 1e6`; Swapr V3: dynamic fee via ASOF; Balancer V3: explicit `swapFeeAmount` field). Balancer V2: not implemented.
- **lvr_apr_7d**: `(swap_flow_usd_7d - fees_usd_7d) / tvl_usd_7d_avg * (365/7) * 100`. Schema documents sign as "always <= 0". Covers Uniswap V3 and Swapr V3 only.
- **net_apr_7d**: `avg(fee_apr_7d + lvr_apr_7d)` over a 7-day window; schema defines it as always <= fee_apr_7d.
- **Top-5 pool filter**: per-token, top 5 pools by 30-day avg TVL (minimum $1,000). Balancer V2 excluded from this filter entirely.
- **Aggregator label**: named project if `tx_to` matches crawlers labels; else "Other Router" (unlabeled, >= 2 hops) or "Direct" (unlabeled, 1 hop).
- **Unique LP count (7D)**: distinct owner addresses from Mint/Burn events, deduplicated via `groupBitmap(cityHash64)`. Uniswap V3 and Swapr V3 only.

**Contract context:** Balancer V2 Vault `0xBA1222...2C8`, Balancer V3 Vault `0xba1333...9ba9`, Uniswap V3 Factory `0xe32F7d...B1`, Swapr V3 AlgebraFactory `0xa0864c...a766`. Pools and tokens whitelisted via seeds; 45 tokens in `tokens_whitelist.csv`.

---

## Implementation assessment

### Critical

**Balancer V3 negative TVL on ERC4626-wrapper pools served downstream**
`models/execution/pools/intermediate/int_execution_pools_balancer_v3_daily.sql`, `int_execution_pools_balances_daily.sql`

`int_execution_pools_balancer_v3_daily` accumulates deltas from two paths: `deltas_pool` (LiquidityAdded/Removed events via `token_index` join) and `deltas_swap` (Swap events carrying wrapper `token_address` directly). Inspector verified the two paths are correctly partitioned — Swap events always have `token_index = NULL` and are dropped from `deltas_pool` by the INNER JOIN. The root cause is unresolved but confirmed real: pool `0x6e6bb...` (waGnowstETH/waGnoWETH) shows `token_amount = -710 wstETH` and `tvl_component_usd = -$2.1M` per day. Warehouse confirms 5,808 rows (28.1% of all Balancer V3 rows) have `tvl_component_usd < 0`; these reach `int_execution_pools_balances_daily` and downstream mart TVL aggregations. Working hypothesis is a sign-convention mismatch between liquidity event deltas and wrapper-vs-underlying unit scale in the enrichment CTE.

### High

**Balancer V2 entirely absent from `int_execution_pools_fees_daily` — zero volume and fees for 93% of pool-rows**
`models/execution/pools/intermediate/int_execution_pools_fees_daily.sql`, `int_execution_pools_metrics_daily.sql`

`fees_daily` computes fees only for Uniswap V3, Swapr V3, and Balancer V3. Balancer V2 holds 858,870 rows in `balances_daily` (93% of all rows) but zero fee rows. `metrics_daily` LEFT JOINs `fees_daily`, so `volume_usd_daily` and `fees_usd_daily` are coalesced to 0 for all Balancer V2 pool-days. Every ecosystem-wide volume or fee total silently understates by the entire V2 contribution. Balancer V2 Swap events carry `amountIn`/`amountOut`; the swap fee is recoverable from the pool registration.

**LVR lacks the $500 TVL floor, producing 5e19-scale outliers in production**
`models/execution/pools/marts/fct_execution_pools_il_daily.sql`

`fee_apr_7d` guards `tvl_usd_7d_avg <= $500`; `lvr_apr_7d` guards only `<= 0`. Sub-dollar-TVL pools emit `lvr_apr_7d` up to `5.0e19` (pool `0x22eb73...`, 2026-01-14), `1.9e16`, `1.5e11`. The $1k 30-day-avg filter in `fct_execution_pools_daily` catches most but not all: 1,702 rows with positive `lvr_apr_7d` survive into the final table.

**LVR sign contract violated: schema claims "always <= 0" but 12% of rows are positive**
`models/execution/pools/marts/fct_execution_pools_daily.sql`, `fct_execution_pools_il_daily.sql`, `marts/schema.yml`

Warehouse audit: 1,702 positive, 11,432 negative, 48 null; max positive = 22.4, min negative = -9,315. When `lvr_apr_7d` is positive, `net_apr_7d = fee_apr_7d + lvr_apr_7d` exceeds `fee_apr_7d`, directly contradicting the canonical definition ("net always <= fee"). This is an external-facing yield metric shown to LPs making capital-allocation decisions.

**13 `api_execution_trades_stats_*` and `fct_execution_trades_*` models carry `dev` tag, bypassing the CI tag guard**
`models/execution/pools/marts/api_execution_trades_stats_*.sql`, `fct_execution_trades_*.sql`, `intermediate/int_execution_trades_by_tx.sql`

All 9 `api_execution_trades_stats_*` views and 4 `fct_execution_trades_*` tables use tag `dev` and lack `api:`/`granularity:`/`tier:` tags. `check_api_tags.py` only validates `production`-tagged models, so these escape the convention guard entirely despite the `api_` prefix implying consumer endpoints. `int_execution_trades_by_tx` also carries `dev` while holding 9.49M rows with `max_date = 2026-06-10`; if a production selector excludes `dev`-tagged models, rebuilds of its downstream marts are silently skipped.

**schema.yml column contracts diverge from actual model output across multiple API views**
`models/execution/pools/marts/schema.yml`

- `api_execution_pools_lp_activity_daily` emits `(date, token, label, type, value)` but schema documents `(date, token, mints, burns)`.
- `api_execution_trades_stats_hop_distribution` and `size_distribution` emit `(time_window, label, value, bucket_order)` but schema lists a phantom `trade_count` column and omits `time_window` (the dashboard filter key).
- `api_execution_trades_stats_net_flow` exposes 4 time windows (`1m/3m/6m/1y`) and a `time_window` column; schema documents 30-day only with no `time_window`.
- `fct_execution_trades_lifetime` documents `lifetime_volume_usd` as "all-time sum of per-tx `trade_usd`" but the SQL sums at swap (hop) grain; multi-hop trades count each leg.

**`prev_balances` CTE reads `{{ this }}` without `FINAL` on the normal incremental path**
`models/execution/pools/intermediate/int_execution_pools_balancer_v2_daily.sql`, `int_execution_pools_uniswap_v3_daily.sql`, `int_execution_pools_swapr_v3_daily.sql`, `int_execution_pools_balancer_v3_daily.sql`

All four protocol daily models use `delete+insert` over ReplacingMergeTree; the non-backfill `prev_balances` CTE reads `FROM {{ this }}` without `FINAL` (the backfill path correctly uses `FINAL`). If a background merge has not completed between daily runs, a stale or duplicate previous balance is carried into the cumulative window sum. No observable balance drift was confirmed in the current data, but the risk is structurally present.

### Medium

**Balancer V3 `token_index` derived from observed Swap tokens, not the authoritative registry**
`models/execution/pools/staging/stg_pools__balancer_v3_pool_tokens.sql`, `stg_pools__dex_liquidity_balancer_v3.sql`

`stg_pools__balancer_v3_pool_tokens` assigns `token_index` via `ROW_NUMBER() OVER (PARTITION BY pool_address ORDER BY token_address)` over distinct Swap-event tokens. Balancer V3 LiquidityAdded/Removed `amountsRaw` arrays are positionally ordered by pool registration order. For pools with partial swap coverage or differing emit order, this can misalign amounts to tokens — a plausible contributor to the negative-balance symptom. `int_execution_pools_balancer_v3_daily` uses the authoritative `PoolRegistered` tokenConfig; the staging path is the weaker one and feeds `stg_pools__dex_liquidity_balancer_v3`.

**Hardcoded 5-entry Balancer V3 wrapper map with no CI guard; GHO underlying missing from whitelist**
`models/execution/pools/staging/stg_pools__balancer_v3_token_map.sql`

Static `VALUES` list covers only `waGnoWETH`, `waGnowstETH`, `waGnoUSDCe`, `waGnoGNO`, `waGnoGHO`. Any new Aave/ERC4626 wrapper resolves to the wrapper address, misses `tokens_whitelist`, and produces NULL price → zero TVL silently. More immediately: GHO's underlying (`0xfc421...e73`) is absent from `tokens_whitelist.csv`, so any active Balancer V3 pool containing `waGnoGHO` fails the V3 completeness filter in `fct_execution_pools_daily` today with no alert.

**Balancer V2 staging LEFT JOIN on pool registry can emit NULL `pool_address`**
`models/execution/pools/staging/stg_pools__dex_liquidity_balancer_v2.sql`, `stg_pools__dex_trades_balancer_v2.sql`

Both LEFT JOIN the pool registry; upstream WHERE guards require `pool_id IS NOT NULL` but not `pool_address`. `int_execution_pools_dex_liquidity_events` only filters `amount_raw > 0`, so NULL-pool-address orphan rows can propagate.

**`int_execution_pools_balances_daily` is a full-rebuild table over four incremental sources**
`models/execution/pools/intermediate/int_execution_pools_balances_daily.sql`

Plain `table` materialization (no `incremental_strategy`, no `partition_by`), scanning all history from Balancer V2 (since 2021) on every run. Risks the CH Cloud >100-partitions-per-insert cap (code 252) per project memory.

**`fct_execution_pools_tvl_token_daily` uses `nullIf` workaround instead of `join_use_nulls` hook**
`models/execution/pools/marts/fct_execution_pools_tvl_token_daily.sql`

Uses `nullIf(p0.price, 0)` on LEFT JOIN results instead of the project-preferred `join_use_nulls` hook (per project memory). Currently clean (0 null rows in 28,176-row warehouse scan), but if the price source returns a non-zero default for missing rows the guard silently breaks; no `not_null` test on `tvl_in_token0`/`tvl_in_token1`.

**`api_execution_pools_net_apr_daily` filters `fee_apr_7d IS NOT NULL`, silently excluding LVR-only rows**
`models/execution/pools/marts/api_execution_pools_net_apr_daily.sql`

Rows where LVR exists but `fee_apr` is NULL (e.g., Balancer V3 pools or pools active < 3 days) are excluded entirely. If the intent is to show net APR wherever either component is available, the filter should target `net_apr_7d IS NOT NULL`.

### Low

**V3 pool registry joined without `FINAL` despite ReplacingMergeTree**
`models/execution/pools/staging/stg_pools__v3_pool_registry.sql`

Downstream views joining this table do not use `FINAL`; a duplicate `PoolCreated` event (reorg/re-index) would fan out those joins.

**`fct_execution_trades_by_*` tables lack explicit `engine` and `order_by`**
`models/execution/pools/marts/fct_execution_trades_by_aggregator_daily.sql`, `fct_execution_trades_by_protocol_daily.sql`, `fct_execution_trades_by_token_daily.sql`, `fct_execution_trades_lifetime.sql`

All four materialize as plain `table` with no `engine` or `order_by`, unlike every other `fct_` mart in the unit which explicitly specifies ReplacingMergeTree and `order_by`.

**`api_execution_pools_lps_count_7d` uses non-standard `granularity:last_7d` tag**
`models/execution/pools/marts/api_execution_pools_lps_count_7d.sql`

`last_7d` is in `POINT_GRANS` so CI passes, but all other 7d-aggregation API models in this unit use `granularity:snapshot`; the inconsistency complicates selector patterns.

---

## Business-logic assessment

### High

**Ecosystem volume and fee totals understate by the entire Balancer V2 contribution**
`int_execution_pools_fees_daily.sql`, `fct_execution_pools_daily.sql`

Balancer V2 has run on Gnosis Chain since 2021-01-01 and accounts for 93% of pool-rows in the intermediate layer. Because it is excluded from `fees_daily` and filtered out of `fct_execution_pools_daily` (protocol IN guard), every ecosystem-wide DEX volume or fee figure served to API consumers, dashboards, or quarterly reporting silently omits the longest-lived AMM on the chain. The exclusion is documented in `schema.yml` but not surfaced to end consumers.

**LVR sign contract is wrong; net APR can exceed fee APR for external-facing yield metrics**
`fct_execution_pools_il_daily.sql`, `fct_execution_pools_daily.sql`, `marts/schema.yml`

1,702 of 14,088 rows (12%) in `fct_execution_pools_daily` have positive `lvr_apr_7d` (max +22.4). This produces `net_apr_7d > fee_apr_7d`, directly contradicting the canonical definition published in `analytics.md` and `schema.yml`. The Yields dashboard's opportunity table (`fct_execution_yields_opportunities_latest`) consumes `net_apr_7d`; LPs reading it see misleadingly inflated yields for those rows.

### Medium

**Unique-LP counts and LP-activity are internally inconsistent and exclude Balancer**
`fct_execution_pools_lps_latest.sql`, `int_execution_pools_lps_daily.sql`, `api_execution_pools_lp_activity_daily.sql`

`int_execution_pools_lps_daily` and `fct_execution_pools_lps_latest` cover only Uniswap V3 and Swapr V3 at the bitmap level. `fct_execution_pools_lps_latest`'s `pool_token_map` queries only `stg_pools__v3_pool_registry`, so Balancer V3 LP addresses tracked in the daily bitmap are silently dropped from unique-address counts. Meanwhile `api_execution_pools_lp_activity_daily` (via `fct_execution_pools_daily` pool labels) does include Balancer V3 pools, creating a documented inconsistency between activity counts and unique-address counts for the same protocol.

**No MetricFlow metrics for any pool KPI; MCP semantic queries route to a coverage gap**
`semantic/authoring/execution/pools/semantic_models.yml`

`semantic_models.yml` defines only graph-edge models (LP-to-pool, pool-to-token) and a measure-less `execution_pools_balances_daily` candidate; `metrics: []`. Every fee-APR/TVL/volume/LVR question from the MCP cerebro semantic layer hits `semantic_coverage_gap` and falls back to raw SQL, meaning governed KPI definitions live only in mart SQL with no single source of truth.

**46.8% of Balancer V2 balance rows have null `price_usd`; TVL understated with no quantified alert**
`int_execution_pools_balancer_v2_daily.sql`, `int_execution_pools_balances_daily.sql`

401,881 of 858,870 Balancer V2 rows (98.9% of all 406,090 null-price rows in `balances_daily`) lack `price_usd`, so `tvl_component_usd` is NULL and silently excluded from pool TVL sums. Driven by BPT tokens and minor DeFi tokens outside `tokens_whitelist`. Not a code defect, but a material unmonitored coverage gap that biases TVL downward for the protocol already excluded from marts.

**`fct_execution_trades_lifetime` sums at hop grain, documented as per-tx**
`fct_execution_trades_lifetime.sql`, `marts/schema.yml`

`lifetime_volume_usd` sums `amount_usd` at swap (hop) grain from `int_execution_pools_dex_trades`; for multi-hop trades each leg contributes independently. Schema documents it as "all-time sum of per-tx `trade_usd`". Analysts comparing `lifetime_volume_usd` to `trade_count` get an inflated volume-per-trade ratio.

### Low

**Swapr V3 pools with no Fee events report `fee_apr_7d = 0` instead of NULL**
`int_execution_pools_swapr_v3_daily.sql`, `int_execution_pools_fees_daily.sql`

Pools that never emitted a Fee event default to `fee_ppm = 0` via `coalesce(..., 0)`, producing a misleading 0% APR rather than NULL/unknown.

**"Direct" aggregator label conflates single-hop router calls with true direct pool interactions**
`int_execution_trades_by_tx.sql`, `fct_execution_trades_by_aggregator_daily.sql`

Unlabeled single-hop txs (hop=1) are labeled "Direct"; this may include simple router calls, overstating "Direct" market share in aggregator-share reporting.

---

## Data findings

Seven warehouse queries were run across the two shards. Key numbers:

| Metric | Value | Source |
|---|---|---|
| `balances_daily` total rows | 915,945 | `int_execution_pools_balances_daily` |
| Balancer V2 rows in `balances_daily` | 858,870 (93.7%) | warehouse query |
| Rows with null `price_usd` in `balances_daily` | 406,090 (44.3%) | warehouse query |
| Null-price rows from Balancer V2 | 401,881 (98.9% of nulls) | warehouse query |
| Balancer V3 rows with `tvl_component_usd < 0` | 5,808 (28.1% of V3) | warehouse query |
| Worst negative TVL (pool `0x6e6bb...`) | -$2.1M/day | warehouse query |
| `fees_daily` Balancer V2 rows | 0 | warehouse query |
| `fct_execution_pools_daily` rows | 14,088 | warehouse query |
| Rows with positive `lvr_apr_7d` | 1,702 (12%) | warehouse query |
| Max `lvr_apr_7d` in `fct_execution_pools_il_daily` | 5.0e19 | warehouse query |
| `int_execution_trades_by_tx` rows | 9,490,222 | warehouse query |
| Trades with null `trade_usd` | 168,358 (1.8%) | warehouse query |
| `fct_execution_pools_daily` data lag | 3 days (max 2026-06-08 vs today 2026-06-11) | warehouse query |
| Grain duplicates in `fct_execution_pools_daily` | 0 | warehouse query |

---

## Pros / Cons

**Strengths:**

- Event-delta accumulation (not `balanceOf` snapshots) with consistent two's-complement int256 decoding and ASOF daily price joins across four AMM protocols — architecturally sound and uniform.
- Grain uniqueness confirmed clean by warehouse query across all protocols in `balances_daily` and `fct_execution_pools_daily` (0 duplicates despite ReplacingMergeTree).
- Data freshness healthy at the intermediate layer (`max_date` 2026-06-10/11); the 3-day mart lag is monitored via an Elementary `freshness_anomalies` test.
- Gross-fee accounting is principled and protocol-aware: UniV3 `swap_in * fee_ppm / 1e6`, Swapr dynamic fee via ASOF on `event_order`, Balancer V3 explicit `swapFeeAmount` field.
- Top-5-pool-per-token filter with a $1k TVL floor sensibly suppresses dust pools from consumer views.
- Graph semantic models (LP-to-pool, pool-to-token edges) are well-formed and feed the MCP graph explorer at `approved` quality tier.
- Incremental design uses `insert_overwrite` with monthly partitions and correct `FINAL` usage in self-referential lookbacks for most marts.
- Both review shards achieved full file coverage (27 staging/intermediate, 31 marts) with no skimming.

**Weaknesses:**

- Balancer V2 — 93% of all pool-rows and the longest history (since 2021) — is silently served as zero volume and zero fees, excluded from marts, the Yields dashboard, and any ecosystem-wide total.
- Balancer V3 produces negative TVL on ERC4626-wrapper pools (-$2.1M/day, 28% of V3 rows negative), reaching downstream TVL aggregations.
- LVR is broken twice: missing $500 TVL floor emits 5e19-scale values; schema contract ("always <= 0") contradicted by 12% of rows, misleading yield consumers.
- 13 `api_`/`fct_execution_trades_*` models carry `dev` tag, bypassing the check_api_tags.py CI guard and lacking required API convention tags.
- `schema.yml` column contracts are wrong for several API views, misleading schema-based column discovery.
- 44% of `balances_daily` rows have null `price_usd` — silently excluded from TVL sums with no quantified monitoring test.
- No MetricFlow metrics exist for any pool KPI; MCP semantic queries must fall back to raw SQL.
- Hardcoded 5-entry Balancer V3 wrapper map with GHO underlying absent from `tokens_whitelist`.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 | Fix Balancer V3 negative TVL: confirm whether the vault accounts in ERC4626 wrapper shares vs underlying; reconcile `deltas_pool` and `deltas_swap` sign conventions; add a `not_negative` test on `token_amount`/`tvl_component_usd` in `int_execution_pools_balances_daily`. | `int_execution_pools_balancer_v3_daily.sql`, `int_execution_pools_balances_daily.sql` |
| P0 | Apply the $500 TVL floor to `lvr_apr_7d` in `fct_execution_pools_il_daily` (matching `fee_apr`); add a bounded-range test preventing 5e19-scale values from reaching `fct_execution_pools_daily`. | `fct_execution_pools_il_daily.sql` |
| P0 | Resolve the LVR sign contract: decide the canonical sign, correct the formula or schema description, and guarantee `net_apr_7d <= fee_apr_7d` (or explicitly document that net can exceed fee under directional imbalance). This is an external-facing yield metric shown to LPs. | `fct_execution_pools_il_daily.sql`, `fct_execution_pools_daily.sql`, `schema.yml` |
| P1 | Remove `dev` tag from all 13 `api_execution_trades_stats_*` / `fct_execution_trades_*` models and `int_execution_trades_by_tx`; add required `api:`/`granularity:`/`tier:` tags; re-run `check_api_tags.py`. If any are genuinely pre-production, rename them off the `api_` prefix. | `api_execution_trades_stats_*.sql`, `fct_execution_trades_*.sql`, `int_execution_trades_by_tx.sql` |
| P1 | Correct `schema.yml` to match actual output columns for `api_execution_pools_lp_activity_daily`, `hop_distribution`, `size_distribution`, `net_flow` (document `time_window` and all 4 windows), and `fct_execution_trades_lifetime` (hop-grain, not per-tx). | `marts/schema.yml` |
| P1 | Make the Balancer V2 exclusion an explicit product decision: either implement V2 fee/volume separation (Swap `amountIn`/`amountOut` + pool registration swap fee) and include it in `fct_execution_pools_daily`, or add a prominent data caveat to every ecosystem-volume/fee API view and the Yields dashboard. | `int_execution_pools_fees_daily.sql`, `fct_execution_pools_daily.sql` |
| P2 | Wire Balancer V3 LP addresses (and ideally Balancer V2 via `PoolBalanceChanged.liquidityProvider`) into `int_execution_pools_lps_daily` and `fct_execution_pools_lps_latest`'s `pool_token_map` so unique-LP counts are consistent with LP-activity coverage. | `fct_execution_pools_lps_latest.sql`, `int_execution_pools_lps_daily.sql` |
| P2 | Convert `stg_pools__balancer_v3_token_map` to a seed/source with a monitoring test; add `waGnoGHO`'s underlying (`0xfc421...e73`) and any new wrappers to `tokens_whitelist.csv`; add a test that flags Balancer V3 pools dropped by the completeness filter for missing token metadata. | `stg_pools__balancer_v3_token_map.sql`, `seeds/tokens_whitelist.csv` |
| P2 | Confirm whether `prev_balances`-without-`FINAL` has caused balance drift (query for between-run duplicates in each daily model); if so, add `FINAL` to the normal incremental path after assessing query-performance impact. | `int_execution_pools_balancer_v2_daily.sql`, `int_execution_pools_uniswap_v3_daily.sql`, `int_execution_pools_swapr_v3_daily.sql`, `int_execution_pools_balancer_v3_daily.sql` |
| P2 | Quantify and surface the 46.8% null-price coverage gap for Balancer V2 (and 28% for V3) — add a coverage metric/test or Elementary warning so consumers know what fraction of TVL is unpriced rather than silently dropped. | `int_execution_pools_balancer_v2_daily.sql`, `int_execution_pools_balances_daily.sql` |
| P3 | Add `FINAL` to `stg_pools__v3_pool_registry` joins in downstream views to prevent fanout from duplicate `PoolCreated` events. Add explicit `engine` + `order_by` config to the four `fct_execution_trades_by_*` tables. Replace `nullIf` workaround with `join_use_nulls` hook in `fct_execution_pools_tvl_token_daily`; add `not_null` tests on `tvl_in_token0`/`tvl_in_token1`. | `stg_pools__v3_pool_registry.sql`, `fct_execution_trades_by_*.sql`, `fct_execution_pools_tvl_token_daily.sql` |

---

## Open disagreements

None — review converged in one round.

---

## Review log

**Round 1 — challenges and resolution:**

| Challenge | Issued to | Outcome |
|---|---|---|
| Balancer V3 double-counting hypothesis (deltas_pool + deltas_swap both catching Swap events) | Inspector (staging-intermediate) | Resolved: warehouse confirmed 100% of Swap rows have `token_index = NULL` and are dropped by the INNER JOIN in `deltas_pool`; paths are strictly partitioned. |
| 46.8% null-price rate for Balancer V2 — code bug vs coverage gap | Inspector (staging-intermediate) | Resolved: confirmed data coverage gap (tokens not in whitelist), not a code defect; material impact on TVL totals quantified. |
| `delete+insert` incremental strategy concern (insert_overwrite aliasing) | Inspector (staging-intermediate) | Resolved: strategy is consistent with the project convention; the FINAL concern on `prev_balances` is flagged as latent risk, not a confirmed defect. |
| WINDOW clause in `fct_execution_pools_daily` partitions by `(protocol, pool_address)` without `token_address` | Inspector (marts) | Resolved: not a computation error — fee/LVR APR are pool-level values and both token rows hold identical values; `avg()` over identical values returns that value. |
| ReplacingMergeTree on `table` materializations — FINAL required? | Inspector (marts) | Resolved: every dbt run truncates and fully replaces these tables, so no version history to deduplicate at read time. FINAL is correctly used only in incremental intermediate self-referential lookbacks. |
| `combined_usd` double-counting in `fct_execution_trades_by_token_daily` | Inspector (marts) | Resolved: explicitly documented in schema.yml and SQL comment; correct for per-token activity views, not chain-level volume totals. |
