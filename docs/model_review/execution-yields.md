# Model review: execution/yields

**Convergence:** converged in 1 round — inspector and context reports are mutually consistent; no contradictions; verdict validator independently confirmed all four headline defects from source code.

---

## Scope and inventory

27 SQL models across `models/execution/yields/`: 4 intermediates and 23 marts. Three logical sub-domains share the unit:

| Sub-domain | Intermediates | Marts |
|---|---|---|
| Savings xDAI vault (ERC-4626) | `int_yields_savings_xdai_rate_daily` | `fct_yields_savings_xdai_apy_daily`, `fct_yields_sdai_apy_daily` (legacy wrapper), 2 `api_execution_yields_overview_sdai_*` |
| Lending (Aave V3 + SparkLend) | — (delegates to execution/lending) | `fct_execution_yields_overview_snapshot`, 4 overview API KPI cards, 2 user-lending marts |
| LP positions (Uniswap V3, Swapr V3, Balancer V2/V3) | `int_execution_yields_user_lp_positions`, `int_execution_yields_user_activity` | `fct_execution_yields_user_lifetime_metrics`, 7 `api_execution_yields_user_*`, `fct_execution_yields_user_fee_collections_daily`, `fct_execution_yields_opportunities_latest` |

Six of the 23 mart models are thin wrappers over `fct_execution_yields_overview_snapshot` with a `WHERE metric=` filter.

---

## Business context

This unit answers four question families for the Gnosis Chain analytics dashboard (Yields section), MCP semantic layer, and quarterly DeFi reporting:

1. **Yield opportunities** — `fct/api_execution_yields_opportunities_latest` surfaces LP pools and lending markets ranked by yield in one table.
2. **Trend indicators** — `fct_execution_yields_overview_snapshot` and its six API KPI cards (lending TVL, lending best APY, active lenders, LP TVL, LP best APR, sDAI APY, sDAI supply) provide current value plus 7-day change pct.
3. **Savings xDAI APY time series** — `int_yields_savings_xdai_rate_daily` → `fct_yields_savings_xdai_apy_daily` (Daily/7DMA/30DMA/7DMM/30DMM labels, long format) is the canonical sDAI/sUSDS APY pipeline consumed by dashboard charts and approved semantic metrics.
4. **User portfolio** — seven `api_execution_yields_user_*` marts back a wallet-lookup tab exposing LP in-range status, fee income, lending positions, and lifetime KPIs.

**Canonical definitions confirmed:**

- `daily_rate`: 7-day geometric slope — `pow(share_price_t / share_price_t-7, 1/7) - 1`. Not a day-over-day ratio; designed to smooth lump-sum `relayInterest()` spikes.
- Savings APY: `floor(pow(1 + daily_rate, 365) - 1, 4) * 100`.
- Lending supply APY: continuous-compounding annualisation of on-chain `liquidityRate` (RAY=1e27 per-second).
- `fee_apr_7d` (LP): `(fees_usd_7d / tvl_usd_7d_avg) * (365/7) * 100`, double-smoothed via 7-day trailing sum then 7-day MA.
- Active lending position: `(user, reserve)` with `balance_usd > 0.01` on the latest date in `int_execution_lending_aave_user_balances_daily`.
- Active LP (V3): `net_liquidity > 0`; active LP (Balancer): `capital_in_usd > capital_out_usd` (heuristic).
- Top wallets: union of top-25 LPs by fees, top-25 dual LP+lending wallets, top-25 lenders by balance (up to ~50 total).

**Contract context:** Savings xDAI vault `0xaf204776c7245bF4147c2612BF6e5972Ee483701` (ERC-4626, deployed 2023-09-28); regime flip at block 43027713 (2025-11-07) from DAI/sDAI to USDS/sUSDS. Aave V3 pool `0xb50201558B00496A145fE76f7424749556E326D8` (6 reserves); SparkLend pool `0x2Dae5307c5E3FD1CF5A72Cb6F698f915860607e0` (9 reserves). All addresses cross-verified against `seeds/lending_market_mapping.csv` and `seeds/atoken_reserve_mapping.csv`.

---

## Implementation assessment

### Critical

**`least(DateTime, NULL)` coerces `first_yield_date` to 1970-01-01 for all 6,055 wallets**
`models/execution/yields/marts/fct_execution_yields_user_lifetime_metrics.sql` lines 55/57 call `least(lp.first_lp_date, ll.first_lending_date)` with no `coalesce` guard. For LP-only or lending-only wallets one argument is NULL; ClickHouse coerces the result to epoch on INSERT into the non-Nullable DateTime column, also zeroing `tenure_days`. All 6,055 rows confirmed by warehouse query (`min = max = 1970-01-01`). The broken value propagates directly to `api_execution_yields_user_kpis`.
Fix: `least(coalesce(a, b), coalesce(b, a))` on both args.

**`active_lending_positions = 0` for all wallets despite 27.9M lending-balance rows (stale table)**
`models/execution/yields/marts/fct_execution_yields_user_lifetime_metrics.sql` — the table-materialized model was last built before `int_execution_lending_aave_user_balances_daily` was populated (27.9M rows, `max_date` 2026-06-13, 15.7M rows with balance > 0). All 6,055 wallet rows show `active_lending_positions = 0`. This is a separate defect from the epoch bug: both exist simultaneously. A `dbt run --full-refresh` is required after the code fix.

### High

**Uniqueness grain on `int_execution_yields_user_activity` omits `token_address`; ReplacingMergeTree can silently collapse multi-token rows**
`models/execution/yields/intermediate/int_execution_yields_user_activity.sql` — the declared grain `(block_timestamp, source, transaction_hash, log_index)` is not unique: `lp_events` selects per-token rows from `int_execution_pools_dex_liquidity_events`, which emits one row per token for multi-token Balancer V2 Mint/Burn events. Confirmed duplicate: tx `350e2ce9...`, log_index 46, two rows for wstETH and WETH. Beyond causing the `unique_combination_of_columns` test to flap, the ReplacingMergeTree engine can collapse duplicates on merge — dropping a token leg from the activity feed entirely (completeness failure, not just a test failure). 26,097 LP rows (1.3%) have NULL `token_symbol`, indicating wider scope. Fix: add `token_address` to both the `ORDER BY` key and the schema.yml test grain. See also `models/execution/yields/intermediate/schema.yml`.

**Overview snapshot forward-references `lending_tvl_latest_date` CTE**
`models/execution/yields/marts/fct_execution_yields_overview_snapshot.sql` lines 90–105 reference the CTE `lending_tvl_latest_date`, which is defined at line 149. Standard SQL forbids forward references in a `WITH` block. ClickHouse resolves it lazily today — all 7 metrics materialize correctly — but the pattern is non-standard, breaks on any SQL-standard-compliant engine, and is a maintenance trap: reordering CTEs or inserting a dependent CTE between lines 90 and 149 would silently break the date dependency. Move the date CTE above its first reference.

**Approved-tier semantic measures reference non-existent columns (MCP path broken)**
`semantic/authoring/execution/yields/semantic_models.yml` lines 1152–1157 define `yields_sdai_apy_7dma_value` (`expr: apy_7DMA`) and `yields_sdai_apy_30dma_value` (`expr: apy_30DMA`) on model `fct_yields_sdai_apy_daily`. That view returns only `(date, apy, label)` in long format — the wide columns `apy_7DMA` and `apy_30DMA` do not exist. These are approved-tier measures; any MCP or semantic-layer query invoking them will fail at runtime. Fix: repoint to label-filtered rows on `fct_yields_savings_xdai_apy_daily` (`WHERE label = '7DMA'`), or drop the wide measures and reload the semantic registry.

### Medium

**`opportunities` `as_of_date` derived from Swapr event recency, not actual source tables**
`models/execution/yields/marts/api_execution_yields_opportunities_latest.sql` — `as_of_date` reads `max(block_timestamp)` from `contracts_Swapr_v3_AlgebraPool_events` (returns 2026-06-08), while data is sourced from `fct_execution_pools_daily` and `int_execution_lending_aave_daily`. If Swapr goes quiet, the freshness indicator lags even when lending and other pool data are current. Derive `as_of_date` from `max(date)` of the actual source tables.

**`daily_rate` column description contradicts the geometric-slope implementation**
`models/execution/yields/intermediate/schema.yml` line ~143 describes `daily_rate` as `(share_price_t / share_price_t_minus_1) - 1` (day-over-day ratio). The model computes `pow(share_price_t / share_price_t-7, 1/7) - 1` (7-day geometric slope). The canonical docs and the model description (lines 122–125) are correct; only the column description is wrong. This misleads every downstream API and MCP consumer reading the field definition.

**Same-day collect-minus-burn netting can zero legitimate fee claims**
`models/execution/yields/marts/fct_execution_yields_user_fee_collections_daily.sql` — `greatest(SUM collect_amount_usd - SUM burn_amount_usd, 0)` per `(date, provider, pool_address)`. In the common Uniswap V3 pattern where `Collect` and `Burn` (remove-liquidity) share the same transaction and day, subtracting burn USD from collect USD effectively zeroes the fee claim. The behavior is arguably intentional but is undocumented and understates fee income in common removal flows.

**`api_execution_yields_user_lending_balances_daily` missing `as_of_date` — inconsistent with peer API views**
All other user-facing API views include an `as_of_date` correlated subquery for freshness signaling; this daily time-series view omits it and does not document the omission in schema.yml, making data-freshness assessment inconsistent across the user-portfolio API surface.

### Low

**`int_yields_savings_xdai_rate_daily` uses `apply_monthly_incremental_filter` without `is_incremental` guard at call site**
`models/execution/yields/intermediate/int_yields_savings_xdai_rate_daily.sql` — the macro checks `is_incremental()` internally and emits no SQL for table materializations, so today's behavior is correct. However, the call site lacks the `{% if not (start_month and end_month) %}` guard pattern used in sibling models. If materialization is changed to incremental, backfill filtering may produce incorrect results.

**Six overview API views share a single `api:yields_overview` tag**
`models/execution/yields/marts/api_execution_yields_overview_lending_tvl.sql` et al. — the six KPI wrapper views all carry `api:yields_overview`, creating an ambiguous endpoint namespace. The `multi_api` CI rule fires per-node so it does not error today, but distinct API tags per card would be cleaner.

---

## Business-logic assessment

### High

**Balancer V2 profit-as-fee proxy mislabels ~$35.9M of PnL as fee income**
`models/execution/yields/intermediate/int_execution_yields_user_lp_positions.sql` lines 96–99: for non-V3 pools where `tick_lower IS NULL` and `has_active_tokens = 0`, `fees_collected_usd = greatest(capital_out_usd - capital_in_usd, 0)`. This conflates impermanent-loss profit (or exit-at-gain PnL) with fee income. 1,931 Balancer V2 positions use this fallback with a combined $35.9M attributed as fees. The value rolls up into `fct_execution_yields_user_lifetime_metrics.total_lp_fees_usd`, materially overstating LP fee income in the user-portfolio KPI surface. An external consumer reading "total fees earned" is materially misled. Fix: exclude the proxy from `total_lp_fees_usd`, rename it `estimated_pnl_usd`, and add a `has_approximate_fees` flag.

**Seven user-facing API marts expose plaintext wallet addresses with no privacy tier tag**
`models/execution/yields/marts/api_execution_yields_user_activity.sql`, `api_execution_yields_user_lp_positions.sql`, `api_execution_yields_user_lending_positions.sql`, `api_execution_yields_user_kpis.sql`, `api_execution_yields_user_top_wallets.sql`, `api_execution_yields_user_lending_balances_daily.sql`, `api_execution_yields_user_fee_collections_daily.sql` — all output `wallet_address` / `user_address` in plaintext, are tagged `tier1`, but carry no `privacy:tier_*` tag and no `expose_to_mcp` override. Comparable models in `gnosis_app` and `gpay` subgraphs are tagged `privacy:tier_internal`. These endpoints are MCP-accessible by default unless gated upstream. Any caller can look up any wallet's positions and full transaction history. Add `privacy:tier_internal` (or an explicitly confirmed public tag) as defense-in-depth.

### Medium

**TVL threshold mismatch: user portfolio (`> 0.01`) vs overview lending TVL and lender count (`> 0`)**
`models/execution/yields/marts/fct_execution_yields_user_lifetime_metrics.sql` and `models/execution/yields/marts/fct_execution_yields_overview_snapshot.sql` apply different active-position thresholds. The two surfaces will not reconcile for dust positions, creating a lender-count and TVL discrepancy between the overview cards and per-user portfolio totals. A canonical threshold should be chosen and applied consistently.

**sDAI supply card keyed on `symbol = 'SDAI'` is fragile across the USDS regime flip**
`models/execution/yields/marts/fct_execution_yields_overview_snapshot.sql` — `sdai_supply` reads `fct_execution_tokens_metrics_daily WHERE upper(symbol) = 'SDAI'`. If the token is relabelled to `USDS` or `sUSDS` post-2025-11-07, the card silently returns zero without any error. The lookup should be keyed on the vault address, not the symbol.

**SparkLend coverage asymmetry between activity feed and positions/APY join**
`models/execution/yields/intermediate/int_execution_yields_user_activity.sql` includes SparkLend events via `contracts_spark_Pool_events`, but `models/execution/yields/marts/fct_execution_yields_user_lending_positions_latest.sql` joins to opportunities sourced from `int_execution_lending_aave_daily`. If SparkLend is not unified into that model, SparkLend positions appear in the activity feed but are absent from the positions and APY join — an internal inconsistency visible to users.

### Low

**Opportunities scope silently excludes pools and markets without recent activity**
`models/execution/yields/marts/fct_execution_yields_opportunities_latest.sql` holds 11 LP rows and 12 lending rows; LP inclusion requires a non-null `fee_apr_7d` on the latest `fct_execution_pools_daily` date. Quiet pools are silently dropped. By design but undocumented — an "all yield opportunities" reader may assume completeness.

---

## Data findings

Twelve warehouse queries were executed during review:

| Query | Result |
|---|---|
| `int_yields_savings_xdai_rate_daily` row count + freshness | 977 rows (987 expected), `max_date` 2026-06-07 (4 days behind); 10 missing days from 7-day warmup window |
| Overview snapshot metric count | 7 metrics, all present |
| Date lag on overview snapshot | 4 days, within daily SLA |
| Duplicate grain check on `user_activity` | 1 confirmed duplicate group (tx `350e2ce9...`, log_index 46, Balancer V2, wstETH + WETH) |
| LP position fee distribution by protocol | Confirmed Balancer V2 fallback scope |
| Balancer V2 profit-as-fee magnitude | $35.9M across 1,931 positions |
| Opportunities table type / count | 11 LP rows, 12 lending rows |
| Swapr events freshness | `max(block_timestamp)` = 2026-06-08 |
| Lending balances table health | 27.9M rows, `max_date` 2026-06-13, 15.7M with balance > 0 |
| User lifetime metrics null dates | `min = max(first_yield_date)` = 1970-01-01 for all 6,055 wallets; `active_lending_positions = 0` for all |
| CH `least(DateTime, NULL)` behavior | Returns epoch (1970-01-01) on INSERT to non-Nullable DateTime column |
| `fct_execution_yields_opportunities_latest` scope | Max LP APR 87.5%, max lending APY 22.75%; narrow by design |

---

## Pros / Cons

**Pros**
- Well-architected canonical sDAI/sUSDS APY pipeline: 7-day geometric-slope `daily_rate` correctly tames lump-sum `relayInterest()` spikes, with `backing_asset` columns covering the 2025-11-07 DAI→USDS regime flip.
- Broad, coherent yield coverage in one unit (vault, lending, LP) unified into a single opportunities-ranking and overview-KPI surface.
- On-chain-first derivations (`liquidityRate` continuous compounding, WadRayMath utilization, scaled-balance TVL) rather than naive `totalSupply` calls — defensible for quarterly reporting.
- Long-format APY label set (Daily/7DMA/30DMA/7DMM/30DMM via `UNION ALL`) is a clean single-series chart contract for dashboard consumers.
- Strong seed-backed contract resolution (`lending_market_mapping`, `atoken_reserve_mapping`, `savings_xdai_regimes`); all addresses cross-verified.
- Good candidate-tier semantic coverage across lending, pools, and overview KPI cards.
- Freshness within tolerance for table-materialized models (sDAI rate 4 days behind, acceptable against daily SLA).

**Cons**
- User-portfolio tier currently serves objectively wrong numbers: every wallet shows `first_yield_date = 1970-01-01` and `active_lending_positions = 0`, breaking the dashboard's user-KPI surface entirely.
- Fee figures are not trustworthy for external consumption: ~$35.9M of Balancer LP PnL is mis-labeled as fee income and rolls up into lifetime KPIs.
- Grain integrity is broken on the activity feed; ReplacingMergeTree can silently drop multi-token Balancer rows — a completeness failure, not just a flapping test.
- An approved-tier semantic metric (sDAI 7DMA/30DMA measures) is wired to non-existent columns — broken in the MCP/natural-language path that users are told is production-grade.
- Seven user-facing API marts expose plaintext wallet addresses with no privacy tier tag, MCP-accessible by default.
- Freshness and `as_of_date` semantics are inconsistent: opportunities `as_of_date` is pinned to Swapr event recency, not actual source dates.
- Definition drift in published docs: `daily_rate` column description contradicts the model's own geometric-slope logic.
- Same-day collect-minus-burn netting in `fee_collections` can silently zero legitimate fee claims in common Uniswap V3 remove-and-collect flows.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 — hotfix | Wrap `least()` args with `coalesce` in `fct_execution_yields_user_lifetime_metrics`; then `dbt run --full-refresh` to also clear stale `active_lending_positions = 0`. These two together unbreak the entire user-KPI surface. | `models/execution/yields/marts/fct_execution_yields_user_lifetime_metrics.sql` |
| P0 — hotfix | Add `token_address` to `int_execution_yields_user_activity` `ORDER BY` key and `schema.yml` unique-combination grain; verify ReplacingMergeTree is no longer collapsing multi-token Balancer legs by row count comparison before/after. | `models/execution/yields/intermediate/int_execution_yields_user_activity.sql`, `models/execution/yields/intermediate/schema.yml` |
| P0 — MCP | Fix approved-tier semantic measures: repoint `yields_sdai_apy_7dma_value` / `yields_sdai_apy_30dma_value` to label-filtered rows on `fct_yields_savings_xdai_apy_daily`, or drop them. Reload semantic registry after. | `semantic/authoring/execution/yields/semantic_models.yml` |
| P1 — data quality | Stop labeling Balancer PnL as fees: exclude the `capital_out - capital_in` proxy from `total_lp_fees_usd`, rename it `estimated_pnl_usd`, add `has_approximate_fees` boolean, and update schema + semantic descriptions. | `models/execution/yields/intermediate/int_execution_yields_user_lp_positions.sql`, `models/execution/yields/marts/fct_execution_yields_user_lifetime_metrics.sql` |
| P1 — privacy | Apply `privacy:tier_internal` (or an explicitly confirmed public tag) to all seven `api_execution_yields_user_*` marts; confirm MCP exposure matches intent. | `models/execution/yields/marts/api_execution_yields_user_*.sql` (7 files) |
| P2 — correctness | Correct the `daily_rate` column description in `intermediate/schema.yml` to match the 7-day geometric slope; align with canonical docs. | `models/execution/yields/intermediate/schema.yml` |
| P2 — freshness | Derive `opportunities` `as_of_date` from `max(date)` of `fct_execution_pools_daily` and `int_execution_lending_aave_daily` rather than Swapr event recency. | `models/execution/yields/marts/api_execution_yields_opportunities_latest.sql` |
| P2 — CTE | Move `lending_tvl_latest_date` CTE above its first reference in the overview snapshot to eliminate the forward-reference hazard. | `models/execution/yields/marts/fct_execution_yields_overview_snapshot.sql` |
| P2 — consistency | Unify the active-lending threshold (`> 0` vs `> 0.01`) across overview snapshot and user-portfolio models so TVL and lender counts reconcile across surfaces. | `models/execution/yields/marts/fct_execution_yields_user_lifetime_metrics.sql`, `models/execution/yields/marts/fct_execution_yields_overview_snapshot.sql` |
| P2 — resilience | Make the sDAI supply card regime-robust by keying on vault/token address rather than `symbol = 'SDAI'`; confirm post-USDS symbol. | `models/execution/yields/marts/fct_execution_yields_overview_snapshot.sql` |
| P3 — documentation | Document the same-day collect-minus-burn netting behavior in `fct_execution_yields_user_fee_collections_daily` schema.yml; consider keying fees off Collect-only amounts. | `models/execution/yields/marts/fct_execution_yields_user_fee_collections_daily.sql` |
| P3 — SparkLend | Confirm whether SparkLend is unified into `int_execution_lending_aave_daily`; if not, surface the gap in `fct_execution_yields_user_lending_positions_latest` so SparkLend positions are not silently absent from the positions/APY join. | `models/execution/yields/marts/fct_execution_yields_user_lending_positions_latest.sql` |

---

## Open disagreements

None — review converged in one round.

---

## Review log

| Round | Agent | Challenge | Resolution |
|---|---|---|---|
| 1 | Inspector→self | `lending_tvl_latest_date` forward-CTE reference: potential runtime error | Rebutted by warehouse validation (all 7 metrics present); finding maintained at high severity as maintenance hazard and non-portability risk |
| 1 | Inspector→self | `fct_execution_yields_user_lifetime_metrics` stale table: code bug vs stale materialization | Resolved — confirmed as two simultaneous independent defects (epoch coercion + stale full-refresh) both requiring separate fixes |
| 1 | Inspector→self | `int_execution_yields_user_activity` grain violation: intentional vs bug | Resolved — confirmed structural defect of upstream multi-token event source; ReplacingMergeTree collapse risk elevates severity beyond a flapping test |
