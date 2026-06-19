# Model review: execution/gnosis_app

**Convergence:** converged in 1 round — all material claims warehouse-validated; one mechanism correction (user_activity_daily truncation is driven by persisted start_month/end_month vars, not is_incremental()), severity unchanged.

---

## Scope and inventory

| Layer | Count | Description |
|---|---|---|
| Intermediate | 25 | Identity heuristics, event log, swaps, GP topups, marketplace, token-offer claims, MTA events/conversions/coverage, GP wallet linkage |
| Fact (fct_) | ~40 | Materialized time-series tables: users, swaps, topups, marketplace, token offers, retention/churn/cohort, attribution/journeys, WAU/WEAU |
| API (api_) | ~65 | Thin views over fct_/int_ models; KPI singletons |
| Semantic | 2 models, 5 metrics | activity_by_action_daily and users_distinct only |
| **Total** | **~130** | |

Three shards reviewed: 25 intermediates, marts files 1-51, marts files 52-102. All ~130 SQL files read directly. Eight warehouse queries executed per shard (24 total). Schema.yml (~1,350 lines intermediates; ~1,650 lines marts), dbt_project.yml, and key macros (apply_monthly_incremental_filter, pseudonymize_address, build_attribution_lookback) fully read.

---

## Business context

This unit is the on-chain analytics layer for the Gnosis App consumer wallet (app.gnosis.io, launched 2025-11-12 via Cometh v4 ERC-4337 bundler). It answers seven product questions: user reach and identity, in-app actions (swaps, GP topups, marketplace buys, token-offer claims, Circles heuristic events), month-over-month retention and churn, user journeys preceding each conversion, GP wallet acquisition mode (onboarded vs imported), protocol fee revenue from in-app swaps, and weekly active / economically active user (WAU/WEAU) counts. Outputs feed the Gnosis Analytics dashboard (FastAPI tier0/tier1), the cerebro-mcp semantic layer, MTA attribution research, and periodic reporting. The unit also owns the canonical `user_pseudonym` key used for cross-sector joins to Gnosis Pay, Circles, and Mixpanel.

**Key canonical definitions (as implemented):**

- **GA user:** Any EOA appearing as an event parameter inside a Cometh v4 bundler transaction (`tx.from` IN `gnosis_app_relayers.csv`, `tx.to` = EntryPoint v0.7 `0x0000000071727de22e5e9d8baf0edac6f37da032`, `tx.block_timestamp >= 2025-11-12`). Seven independent heuristic rules in `int_execution_gnosis_app_user_events`.
- **High-confidence GA user:** `n_distinct_heuristics >= 2` (marts/fct SQL and semantic layer). Note: intermediate schemas say >= 3 — a live drift (see Business-logic issues).
- **onboard activity_kind:** First-ever heuristic hit per address; anchors cohort_month for retention.
- **swap (GA):** CoW PreSignature with `signed=true`, relayed by Cometh, owner is a GA user. Count includes unfilled; volume restricted to filled trades.
- **topup (GA):** CoW Trade by GA user whose bought token transfers to a known GP wallet in the same settlement tx.
- **marketplace_buy:** PaymentReceived from a non-excluded PaymentGateway, payer is GA user, relayed by Cometh. USD value is NULL pipeline-wide (CRC price feed TBD).
- **token_offer_claim:** OfferClaimed from a Circles v2 ERC20TokenOfferCycle where `claimer` is GA user. User pays CRC, receives the offer token (currently GNO).
- **WAU ecosystem vs in-app:** Ecosystem WAU adds any-app Circles avatar activity on top of in-app Cometh-relayed actions. Not comparable; in-app <= ecosystem by construction.
- **WEAU:** Intersection of GA WAU and weekly Circles earners (>= 1 gCRC cashback or CRC inviter fee via in-app tx).
- **swap fee:** `fee_amount / amount_sold * amount_usd` (pro-rated to USD, filled trades only). Currently universally zero.
- **user_pseudonym:** `sipHash64(salted lowercase address)` via `pseudonymize_address` macro with `CEREBRO_PII_SALT`. Same hash space as Mixpanel and Gnosis Pay sectors.

---

## Implementation assessment

### Critical

**C1. user_activity_daily truncated to 2 months — 89% of onboard anchors missing**
`models/execution/gnosis_app/intermediate/int_execution_gnosis_app_user_activity_daily.sql`

Warehouse: `activity_kind='onboard'` has 2,477 rows (`min=2026-04-04`, `max=2026-06-10`), but `users_current` has 22,644 users with `first_seen_at >= 2025-11-12`. The compiled artifact in `target/compiled/` shows every CTE filtered to `toStartOfMonth >= '2026-05-01' AND <= '2026-06-01'` — the `start_month`/`end_month` var branch fired and was persisted into the live build from a prior backfill invocation. The model is `materialized='table'`; `is_incremental()` is always False for a table materialization and the `apply_monthly_incremental_filter` macro emits nothing — this corrects the inspector-intermediate mechanism attribution (the symptom and severity are unchanged). Fix: rebuild with no vars (full range), or migrate to `materialized='incremental'` with `insert_overwrite` so the standard whole-month macro path governs history. Every downstream cohort, retention, churn, DAU, and repeat-purchase mart is affected.

**C2. Swap fee revenue is zero across all 40,790 filled trades**
`models/execution/gnosis_app/intermediate/int_execution_gnosis_app_swap_fees_daily.sql`, `int_execution_gnosis_app_swaps.sql`

Warehouse: `zero_fee_amount_filled = 40,790`, `null_fee_amount_filled = 0`. The pro-rate formula is mathematically correct; the input `fee_amount` is uniformly zero. Root cause is upstream in `int_execution_cow_trades` (the GPv2 Trade event likely no longer carries protocol fee post-CoW-upgrade). All `fee_usd_total`, `fee_pct_of_volume`, and `kpi_swap_fees_7d` metrics report zero and must not be served as revenue. Add a not-null/non-zero guard that fails loudly.

### High

**H1. Identity bridge silently drops 828 users**
`models/execution/gnosis_app/intermediate/int_execution_gnosis_app_user_identity_bridge.sql`, `int_execution_gnosis_app_user_identities.sql`

Both tables are `materialized='table'` (full rebuild). `users_current` = 22,644 rows (zero null addresses); `user_identities` = 21,816 rows — a 828-row gap. RMT background dedup does not apply to full-rebuild tables. Most likely cause: build-ordering gap (bridge compiled against a stale upstream snapshot). Downstream MTA touchpoint and conversion models INNER JOIN the bridge, silently dropping these 828 users from all attribution. Fix: add a `dbt_utils.equality` or row-count test between the two tables so the divergence fails CI.

**H2. retention_pct_latest KPI returns 0.0, not NULL, during bootstrap**
`models/execution/gnosis_app/marts/api_execution_gnosis_app_kpi_retention_pct_latest.sql`

`anyIf(retention_pct, months_since=1 AND cohort_month=max_with_m1)`: when no months_since=1 row exists, `anyIf` never fires and ClickHouse returns the Float64 default 0.0. Confirmed: retention table has exactly 1 row (May 2026 M0, 2,061 users, 100%). Dashboard tile shows "0% M1 retention" — factually wrong, the metric is not yet computable. A NULL guard (e.g. `nullIf(anyIf(...), 0)` gated on a count check) should render "no data" until a July 2026 cohort matures.

**H3. gpay_wallets uses banned delete+insert strategy; 10-day freshness lag**
`models/execution/gnosis_app/intermediate/int_execution_gnosis_app_gpay_wallets.sql`, `int_execution_gnosis_app_swaps.sql`

`incremental_strategy='delete+insert'` is project-banned (CI guard enforced; model is on the allowlist pending migration). Additionally, `trade_rollup` in `int_execution_gnosis_app_swaps` uses `any(block_number)` for `first_fill_block` — non-deterministic under concurrent writes; `argMin(block_number, block_timestamp)` is the deterministic form. Warehouse: `max(last_event_at) = 2026-06-01`, 10 days behind 2026-06-11; the 2-day lookback window (`apply_monthly_incremental_filter(lookback_days=2)`) appears too narrow for current execution.logs processing lag.

**H4. 248 token_offer_claims (3.1%) priced at $0 due to registry gap**
`models/execution/gnosis_app/intermediate/int_execution_gnosis_app_token_offer_claims.sql`, `int_execution_gnosis_app_token_offers.sql`

Warehouse: 248/7,901 claims have `cycle_address IS NULL`, `offer_token_symbol=''`, `amount_received_usd=0`. Dates 2026-05-07 to 2026-05-13 (days 20588-20594). Real token claims produce zero USD in all downstream aggregations and attribution credit because `received_raw / 10^18 * coalesce(price, 0) = 0`. Root cause: new ERC20TokenOfferCycle contracts not yet in the contracts seed/registry. Fix: backfill the registry; add a not_null test on `cycle_address`.

**H5. Cohort/retention/churn/DAU marts measure the wrong population (flows from C1)**

Because the onboard anchor (`activity_kind='onboard'`) is truncated to 2026-05-01+, every model keyed on it reflects only ~11% of the actual user base. The retention grid has one cohort, churn is all-zero bootstrap. These are served to dashboards and quarterly reporting as if complete. Not trustworthy in front of external consumers until C1 is resolved and dependent marts rebuilt.

### Medium

**M1. churn_monthly uses lagInFrame without explicit ROWS frame**
`models/execution/gnosis_app/marts/fct_execution_gnosis_app_churn_monthly.sql`

`lagInFrame(total_active, 1) OVER (ORDER BY month)` with no `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` frame. ClickHouse window default for `lagInFrame` is not guaranteed ROWS semantics. Currently latent (table has 1 month of data, all values zero). Add the explicit frame before history accumulates.

**M2. coverage_daily mixes approximate countDistinct with exact count() in one ratio**
`models/execution/gnosis_app/intermediate/int_execution_gnosis_app_coverage_daily.sql`

The `tracked` CTE uses `countDistinct` (HLL approximate) for `tracked_conversions`; `total_conversions` uses exact `count()`. The `tracked_conversion_coverage` ratio mixes estimators. At current volume (~7.9K claims) the bias is negligible, but the pattern is inconsistent with `tracked_users` which already uses `uniqExact`. Use `uniqExact(user_pseudonym, conversion_ts)` throughout.

**M3. token_offers: toFloat64(NULL UInt256) silently yields 0.0**
`models/execution/gnosis_app/intermediate/int_execution_gnosis_app_token_offers.sql`

`token_price_in_crc = toFloat64(o.token_price_in_crc_raw) / 1e18`: if `token_price_in_crc_raw` is NULL (from `toUInt256OrNull`), `toFloat64(NULL)` returns 0.0 in ClickHouse, not NULL. Zero-price offers become indistinguishable from offers with missing price data downstream in `token_offer_claims`. Use `toFloat64OrNull` or guard the raw NULL.

**M4. gpay_volume_daily double-casts through String**
`models/execution/gnosis_app/marts/fct_execution_gnosis_app_gpay_volume_daily.sql`

`sumIf(toFloat64OrNull(toString(amount_usd)), ...)` round-trips a Decimal/Float through String, introducing precision loss and silently nulling out unparseable values (e.g. 'NaN', 'Inf'). Use `sumIf(amount_usd, ...)` directly. Same anti-pattern appears in related rollup models.

**M5. fct_token_offer_claims_by_offer_daily uses any(offer_price_in_crc)**
`models/execution/gnosis_app/marts/fct_execution_gnosis_app_token_offer_claims_by_offer_daily.sql`

`any()` is non-deterministic; if a price update lands within a day the selected value is arbitrary. Use `argMax(offer_price_in_crc, block_timestamp)` or `min`/`max` for determinism.

**M6. Attribution as_of_date derived from gpay_topups for all four conversion types**
`models/execution/gnosis_app/marts/api_execution_gnosis_app_attribution_30d.sql`, `api_execution_gnosis_app_attribution_60d.sql`, `api_execution_gnosis_app_attribution_7d.sql`

`as_of_date = max(block_timestamp) FROM int_execution_gnosis_app_gpay_topups`. Attribution covers topup, swap_filled, token_offer_claim, and marketplace_buy. For non-topup conversions this freshness proxy can lead or lag actual data. Use `max(conversion_date)` from the attribution fct itself.

**M7. Foundation tables lack uniqueness and not_null tests**

`int_execution_gnosis_app_users_current` has no uniqueness test on `address`; `int_execution_gnosis_app_gpay_wallets` on `pay_wallet`; `int_execution_gnosis_app_marketplace_offers` on `gateway_address`; `int_execution_gnosis_app_swap_fees_weekly` and `swap_fees_monthly` have no uniqueness or `elementary.schema_changes` tests. Silent duplicates in foundation tables cascade into every downstream aggregation. Add `dbt_utils.unique_combination_of_columns` tests to each.

**M8. Large api-tag/column-schema contract debt (60+ models allowlisted)**
`models/execution/gnosis_app/marts/schema.yml`

`check_api_tags.allow` exempts essentially all `api_execution_gnosis_app_*` models from `columns_missing` and `no_grain_col` — including the high-traffic time-series endpoints (users, swaps, topups, retention, churn, activity_by_action). The MCP/FastAPI layer serves undocumented, untyped column contracts to external consumers. Needs a tracked sprint with a target date, not an open-ended allowlist.

### Low

**L1. fct_users_daily / fct_gpay_wallets_daily expose today() partial rows via api views with no date < today() guard**
`models/execution/gnosis_app/marts/api_execution_gnosis_app_users_daily.sql`, `api_execution_gnosis_app_gpay_wallets_daily.sql`

Calendar spines extend to `today()`; both api views pass `SELECT *` with no upper-bound filter, exposing partial current-day data. Other time-series api views (swaps, token claims) correctly apply `WHERE date < today()`.

**L2. swaps fill-rate (73%, 15,091 unfilled) has no anomaly test**
`models/execution/gnosis_app/intermediate/int_execution_gnosis_app_swaps.sql`

An unexpected spike in unfilled pre-signatures (solver failures, expired orders) would not be caught by current tests.

**L3. Most 7d KPI models missing window: tag; window tags inconsistently applied**
Only `api_execution_gnosis_app_kpi_swap_fees_7d` carries `window:7d`. All other 7d KPIs lack this tag per the canonical api-tag convention.

**L4. churn_retention_complementary test will warn on current bootstrap data**
`models/execution/gnosis_app/marts/fct_execution_gnosis_app_churn_monthly.sql`

Test checks `churn_rate + retention_rate BETWEEN 80 AND 120`. With May 2026 all-zero data, 0+0=0 falls outside the range and the warn-severity test fires. Name also implies a strict mathematical complement it does not enforce (new/returning users sit in neither numerator).

**L5. user_activity_daily config contradiction (table materialization + unique_key)**
`models/execution/gnosis_app/intermediate/int_execution_gnosis_app_user_activity_daily.sql`

`unique_key='(date, address, activity_kind)'` has no effect on `materialized='table'`; it is only meaningful for `delete+insert` incremental strategy. Remove or correct to reduce configuration confusion.

---

## Business-logic assessment

### High

**BL1. Retention denominator (initial_users) is peak-activity, not cohort size — will yield retention > 100% when multi-cohort data arrives**
`models/execution/gnosis_app/marts/fct_execution_gnosis_app_retention_monthly.sql`, `fct_execution_gnosis_app_retention_by_action_monthly.sql`, `fct_execution_gnosis_app_gpay_topups_cohort_monthly.sql`, `fct_execution_gnosis_app_token_offer_claims_cohort_monthly.sql`

`initial_users = max(users) OVER (PARTITION BY cohort_month)`. The monthly_activity CTE excludes `onboard` rows; users who onboard in month M but first act in M+1 are absent from M0. Once M1 data exists, `max()` can resolve to M1 actives, making M0 retention < 100% and M1 = 100% — backwards semantics. Standard cohort practice: fix the denominator at months_since=0 cohort count. Currently masked because only one cohort exists; the bug materializes when July 2026 data lands.

**BL2. High-confidence user threshold drifts between layers (>= 2 vs >= 3)**

`int_execution_gnosis_app_users_current` and `int_execution_gnosis_app_user_identity_bridge` schema docs define high-confidence as `n_distinct_heuristics >= 3`. The mart `fct_execution_gnosis_app_users_distinct` SQL/schema and semantic metric `gnosis_app_high_confidence_users` use `>= 2`. A consumer reading docs gets a different reach number than querying the metric. Pick one threshold and reconcile intermediate schema docs, fct SQL, semantic layer definition, and public docs site.

### Medium

**BL3. MTA event-kind seed mismatches cause silent relationship-test warns and incomplete attribution taxonomy**
`models/execution/gnosis_app/intermediate/int_execution_gnosis_app_events_chain_unified.sql`

The heuristic SQL emits `heuristic_kind = 'circles_fee'`; `events_chain_unified` builds `event_kind = 'chain.circles_fee'`. But `mta_event_kinds.csv` lists `'chain.circles_metri_fee'` (legacy Metri brand name). Separately, the 7th heuristic rule `'chain.circles_personal_mint'` is absent from the seed. All affected touchpoints fail the relationship test at warn severity and are mis-/un-classified in the MTA taxonomy. Update the seed to match emitted kinds.

**BL4. Marketplace_buy carries no USD value pipeline-wide; MTA attribution credit structurally incomplete**
`models/execution/gnosis_app/intermediate/int_execution_gnosis_app_marketplace_payments.sql`, `int_execution_gnosis_app_conversions.sql`

`amount_usd` is NULL for all `marketplace_buy` rows (CRC price feed TBD) through activity, conversions, and attribution. One of four MTA conversion kinds contributes zero economic value. Biases any USD-weighted attribution. Flag the metric explicitly as count-only until the price feed lands.

**BL5. TopUp volume implausibly low (11 rows, 5 users) for a flagship flow**
`models/execution/gnosis_app/intermediate/int_execution_gnosis_app_gpay_topups.sql`

`gpay_topups` requires a CoW Trade AND a GP 'Crypto Deposit' in the same settlement tx with a whitelisted-token payout. With 22K GA users, 11 lifetime topup rows is implausible for a core product flow. The INNER JOIN heuristic may be too narrow if GP deposits link via a different tx pattern, or non-whitelisted tokens dominate. Validate against known production topups before serving topup KPIs.

**BL6. Daily vs weekly "returning user" definitions are inconsistent; 8-30-day-active users unclassified**
`models/execution/gnosis_app/marts/fct_execution_gnosis_app_users_daily.sql`, `fct_execution_gnosis_app_users_weekly.sql`

`returning` (daily) = active in prior 7 days; `reactivated` = inactive for prior 30 days. Users last seen 8-30 days ago are active but fall into neither bucket. Weekly/monthly models use adjacent-period retention, producing a different population. A consumer comparing daily vs weekly returning_users will see inconsistent results with no warning. Document the gap or align definitions.

### Low

**BL7. fct_funnel_daily emits level=0 rows; consumers can over-count top-of-funnel entrants**
`models/execution/gnosis_app/marts/fct_execution_gnosis_app_funnel_daily.sql`

`windowFunnel` output includes a row per user per day with `level=0` when only step-1 was hit. Downstream aggregations without a `level >= 1` filter over-count funnel entrants. No test enforces the filter. Document the level semantics in the API contract and consider a guarded view.

**BL8. Semantic layer covers only 2 of ~12 metric surfaces**
`semantic/authoring/execution/gnosis_app/semantic_models.yml`

Only `activity_by_action_daily` and `users_distinct` are exposed as semantic models. Swaps, topups, GP wallets, marketplace, token offers, retention, churn, attribution, WAU/WEAU are API-only. MCP agents cannot reach those metrics through the governed semantic layer, risking ad-hoc inconsistent definitions. Confirm intent or expand coverage for the core KPIs.

---

## Data findings

Key warehouse measurements taken across the review (24 queries total):

| Metric | Value | Notes |
|---|---|---|
| users_current rows | 22,644 | Clean, `first_seen_at >= 2025-11-12`, zero null addresses |
| user_identities rows | 21,816 | 828-user gap vs users_current |
| user_activity_daily onboard rows | 2,477 | min 2026-04-04; 20,167 missing (89%) |
| swaps total | 55,881 | 40,790 filled (73%), 15,091 unfilled pre-signatures |
| fee_amount=0 on filled swaps | 40,790 / 40,790 | Universal; revenue metrics are all zero |
| token_offer_claims total | 7,901 | 248 (3.1%) with cycle_address NULL, amount_received_usd=0 |
| gpay_wallets total | 1,231 | 1,164 currently GA-owned; max(last_event_at)=2026-06-01 (10 days stale) |
| gpay_topups (activity rows) | 11 | 5 distinct users — implausibly low for a flagship flow |
| retention_monthly rows | 1 | May 2026 M0 only; 2,061 users, 100% retention |
| churn_monthly rows | 2 | May 2026, scope Any + Swap; all rates = 0 |
| users_daily max date | 2026-06-09 | 2 days stale; 209 rows total |
| swaps_daily max date | 2026-06-08 | 3 days stale; swaps data starts 2025-10-13 |
| WAU rows | 60 | Weeks 2025-11-10 to 2026-06-01 |
| marketplace_offers | 10 | 3 with zero buys; 420 lifetime buys / 301 payers |
| swaps_by_pair_daily | 3,771 | Zero unresolved token symbols — wrapper-token JOIN working correctly |

The 3-day freshness lag on swaps and token claims means 7d KPI windows effectively cover only 4 of 7 full days.

---

## Pros / Cons

**Pros**

- Clean, well-layered architecture: identity heuristics to event log to user activity to MTA conversions to WAU/WEAU, with a canonical `user_pseudonym` enabling cross-sector joins to Gnosis Pay, Circles, and Mixpanel.
- Privacy tier correctly enforced: raw address + pseudonym bridge is internal-only; PII salting via `pseudonymize_address` is consistent across sectors.
- Strong ClickHouse discipline: `join_use_nulls` on LEFT joins, `grace_hash` for OOM-bound MTA joins, `ReplacingMergeTree` with explicit `order_by`, `insert_overwrite` (not banned `delete+insert`) on most incrementals.
- Canonical definitions are documented and largely traceable to `docs/economic_concepts.md` and the docs site (onboard, WAU ecosystem vs in-app, WEAU, topup, conversion kinds).
- Swaps anchored on PreSignature signing with explicit signed-vs-filled separation matching stated Dune parity; symbol resolution via wrapper-token join is clean (zero unresolved pairs confirmed in warehouse).
- Marketplace LEFT JOIN correctly preserves zero-buy offers without NULL-to-zero inflation.
- Honest caveats documented: additive-only GP wallet count, marketplace USD pricing TBD, Mixpanel diagnostic-not-gating, two non-comparable WAU scopes, OOM history of attribution models.
- MTA attribution uses a three-step GROUP BY pattern (not arrays or windows) specifically to stay under the 10 GiB per-query cluster cap — a real engineering constraint handled correctly.

**Cons**

- Foundation table `user_activity_daily` is truncated to a 2-month window due to persisted build vars, breaking every cohort, retention, churn, DAU, and repeat-purchase mart it feeds (89% of onboard anchors missing).
- Swap fee revenue is structurally zero everywhere yet exposed as a metric and KPI to consumers without warning.
- Retention/churn marts are in an unusable bootstrap state (1 cohort, all-zero churn) and the retention KPI serves a factually wrong 0% rather than "no data."
- Latent retention denominator bug: `initial_users = max(users)` over cohort rather than months_since=0 cohort size will produce inverted semantics the moment July 2026 data lands.
- Identity bridge silently drops 828 users; MTA touchpoint/conversion INNER JOINs lose them with no test to catch the divergence.
- Definition drift: high-confidence threshold is >= 3 in two intermediate schemas but >= 2 in marts/fct/semantic layer; MTA seed name mismatches cause silent relationship-test warns on every run.
- Large api-tag/schema-contract debt: 60+ models allowlisted for `columns_missing`/`no_grain_col`, so MCP/API serve untyped, undocumented column contracts.
- Semantic layer covers only 2 of ~12 metric surfaces; swaps, topups, retention, churn, attribution, WAU/WEAU are API-only and unreachable through the governed semantic layer.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 | Rebuild `int_execution_gnosis_app_user_activity_daily` without persisted start_month/end_month vars (full history), then rebuild all dependent cohort/retention/churn/DAU/repeat-purchase marts. Single highest-leverage fix — cascades to ~6 downstream marts and the retention KPI. | `int_execution_gnosis_app_user_activity_daily`, `fct_execution_gnosis_app_retention_monthly`, `fct_execution_gnosis_app_churn_monthly`, `fct_execution_gnosis_app_users_daily`, and ~4 others |
| P0 | Quarantine swap-fee revenue: stop serving `fee_usd_total` / `kpi_swap_fees_7d` until `int_execution_cow_trades` populates `fee_amount`. Investigate whether the GPv2 Trade event still carries protocol fee post-upgrade. Add a non-zero guard test that fails loudly. | `int_execution_gnosis_app_swap_fees_daily/weekly/monthly`, `int_execution_cow_trades` |
| P1 | Fix retention denominator: define `initial_users` as the count of users at `months_since=0` (onboard-cohort size), not `max(users)` over activity months. Apply to all four cohort/retention models before multi-cohort data accrues in July 2026. | `fct_execution_gnosis_app_retention_monthly`, `fct_execution_gnosis_app_retention_by_action_monthly`, `fct_execution_gnosis_app_gpay_topups_cohort_monthly`, `fct_execution_gnosis_app_token_offer_claims_cohort_monthly` |
| P1 | Add NULL guard to `api_kpi_retention_pct_latest` so the dashboard tile renders "no data" instead of 0% during the bootstrap period. | `api_execution_gnosis_app_kpi_retention_pct_latest` |
| P1 | Reconcile `users_current` vs `user_identities` (828-user gap): ensure the bridge rebuilds in the same run as `users_current`; add a row-count equality test so the divergence fails CI rather than silently dropping users from MTA. | `int_execution_gnosis_app_user_identity_bridge`, `int_execution_gnosis_app_user_identities` |
| P1 | Backfill the ERC20TokenOfferCycle registry for the 248 unmatched claims (2026-05-07 to 2026-05-13); add a not_null test on `cycle_address` in `token_offer_claims`. | `int_execution_gnosis_app_token_offer_claims`, `int_execution_gnosis_app_token_offers` |
| P2 | Resolve high-confidence threshold drift (>= 2 vs >= 3): pick one definition, then align intermediate schema docs, fct SQL, the semantic metric, and the public docs site. | `int_execution_gnosis_app_users_current`, `fct_execution_gnosis_app_users_distinct`, `semantic_models.yml` |
| P2 | Update `mta_event_kinds.csv`: change `chain.circles_metri_fee` to `chain.circles_fee` and add `chain.circles_personal_mint`, clearing silent relationship-test warns. | `seeds/mta_event_kinds.csv`, `int_execution_gnosis_app_events_chain_unified` |
| P2 | Add explicit `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` frame to `lagInFrame` calls in `fct_churn_monthly` before history accumulates. | `fct_execution_gnosis_app_churn_monthly` |
| P2 | Validate the `gpay_topups` INNER JOIN heuristic (11 rows, 5 users) against known production topups. If the heuristic is too narrow, consider looking at GP safe inflows directly. | `int_execution_gnosis_app_gpay_topups` |
| P2 | Migrate `gpay_wallets` off banned `delete+insert` to `insert_overwrite`; replace `any(block_number)` in `int_execution_gnosis_app_swaps` with `argMin(block_number, block_timestamp)`; widen the lookback window to clear the 10-day freshness lag. | `int_execution_gnosis_app_gpay_wallets`, `int_execution_gnosis_app_swaps` |
| P3 | Open a tracked milestone to clear the 60+ api-tag/column-schema allowlist entries, prioritizing `users_daily`, `swaps_daily`, `retention_monthly`, `churn_monthly`, `activity_by_action`. Add `WHERE date < today()` guards to `api_users_daily` and `api_gpay_wallets_daily`. | `models/execution/gnosis_app/marts/schema.yml`, `check_api_tags.allow` |
| P3 | Fix `toFloat64(NULL)` silent zero in `int_execution_gnosis_app_token_offers`; replace double String cast in `fct_gpay_volume_daily`; replace `any(offer_price_in_crc)` with `argMax` in `fct_token_offer_claims_by_offer_daily`; update `as_of_date` in attribution api views to use the attribution fct's own max conversion date. | Various |
| P3 | Add `dbt_utils.unique_combination_of_columns` tests to `users_current` (address), `gpay_wallets` (pay_wallet), `marketplace_offers` (gateway_address), `swap_fees_weekly/monthly` (week/month); add `elementary.schema_changes` to the fee rollup tables. | Foundation intermediates |

---

## Open disagreements

None. All findings converged in round 1.

---

## Review log

**Round 1**

| Item | Resolution |
|---|---|
| Inspector-intermediate attributed user_activity_daily truncation to `is_incremental()` firing on a `table` materialization | Corrected by analyst: `materialized='table'` never sets `is_incremental()=True`; truncation is caused by persisted `start_month`/`end_month` var values from a prior backfill invocation. Symptom and severity stand. |
| Challenge on whether fee_amount=0 might be design (fee-exempt routing) | Rejected: warehouse confirms 100% of 40,790 filled swaps have fee_amount=0 — not selective, universal. Input column is unpopulated upstream. |
| Challenge on user_activity_daily truncation being expected for a daily-replayed partition table | Rejected: the compiled artifact confirms the filter is a full-history exclusion (all CTEs scoped to 2026-05-01 to 2026-06-01), not a rolling daily increment. |
| Challenge on the 828-user bridge gap being explained by RMT background dedup | Rejected: both tables are `materialized='table'` (full rebuild), not RMT-based appends. Background dedup does not apply. |
| Challenge on countDistinct being acceptable at current analytics volumes | Accepted at this scale; issue downgraded to consistency principle rather than active bias. Medium severity retained for standardization. |
