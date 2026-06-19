# Model review: execution/gpay

**Convergence:** Converged in 1 round — two mart shards and the intermediate shard were mutually consistent with no contradictions; five highest-stakes findings independently verified in code and warehouse before verdict.

---

## Scope and inventory

| Layer | Model count | Purpose |
|---|---|---|
| `intermediate/` | 19 SQL files | Wallet discovery, event decoding (Zodiac modules), activity classification, identity pseudonymisation, token prices, balances |
| `marts/fct_*` | ~30 fact tables | Time-series aggregations (daily/weekly/monthly), churn, retention cohorts, cashback impact, snapshots, MTA attribution |
| `marts/api_*` | ~63 API views | Public dashboard endpoints, user-portfolio endpoints, tier0/tier1 access-gated views |
| `semantic/` | 349 entries | Semantic layer coverage for all dashboard and portfolio surfaces |

Total: 112+ models across three sub-layers. Reviewed in three parallel shards (intermediate, marts-1 files 1–46, marts-2 files 47–93) plus a context shard for canonical definitions.

---

## Business context

Gnosis Pay is a crypto-native Visa-rail debit card product on Gnosis Chain. GP users hold Gnosis Safe smart accounts equipped with three Zodiac modules — DelayModule, RolesModule (Roles v2, manages daily allowances and delegate authorization), SpenderModule (emits `Spend` events on card transactions). The analytics unit tracks all on-chain activity through these Safes: card payments, fiat top-ups and off-ramps (EURe/GBPe mint/burn), GNO cashback, crypto deposits/withdrawals, and wallet balance state.

**Canonical definitions confirmed by context shard:**

- **Payment (action):** ERC-20 transfer from a GP Safe to the spender address `0x4822521e6135cd2599199c83ea35179229a172ee`. This is the sole criterion for "active user" in GP analytics — deposits and cashback do NOT count.
- **Active users (WAU/MAU):** Payment-only. Intentionally narrower than `fct_execution_gpay_kpi_monthly.mau`, which counts any activity type. The two are not comparable and must not be summed.
- **Funded wallet (gpay_funded):** First inflow (Fiat Top Up OR Crypto Deposit) per GP Safe, all-time, from `int_execution_gpay_conversions`. Note: `api_execution_gpay_total_funded` ships a different definition (see Business-logic findings).
- **Cashback:** GNO transfer to a GP Safe from the cashback wallet `0xcdf50be9061086e2ecfe6e4a1bf9164d43568eec`. Entirely separate from Circles gCRC cashback.
- **Identity roles:** `initial_owner`, `delegate`, `safe_self` — three rows per Safe enabling treasury-grain vs owner-grain analytics.

The cashback wallet is registered in `seeds/dao_treasury_wallets.csv` (label: GNO Micro). The spender address is hardcoded in SQL and absent from all seed registries — a material gap addressed under recommendations.

---

## Implementation assessment

### High severity

**`int_execution_gpay_activity_daily`: ReplacingMergeTree key AND uniqueness test both omit `direction` — engine-level data loss**
`models/execution/gpay/intermediate/int_execution_gpay_activity_daily.sql`, `schema.yml`

The model's GROUP BY includes `direction`, but the order_by (RMT dedup key) is `(date, wallet_address, action, symbol)` and the `dbt_utils.unique_combination_of_columns` test omits `direction` in the same way. On ReplacingMergeTree merge, the engine collapses `in` and `out` rows for the same `(date, wallet, action, symbol)`, silently dropping one direction's `amount`/`amount_usd`/`count`. Warehouse confirmed 234 such groups with `distinct_directions=2`. This is not merely a test gap — it is data loss on the central activity spine. 20+ downstream marts (actions_by_token, activity_weekly/monthly, snapshots, KPI, volume/cashback endpoints) inherit the corrupted aggregates. Fix: add `direction` to the `order_by` and uniqueness test, then full-refresh the table and all downstream marts; or drop `direction` from the grain if it is not needed.

**`coalesce(p.price, 0)` on LEFT JOIN without `join_use_nulls` silently zeros USD for unpriced tokens**
`models/execution/gpay/intermediate/int_execution_gpay_activity.sql`

The LEFT JOIN to `int_execution_token_prices_daily` has no `join_use_nulls` pre/post hook. In ClickHouse, the default type coercion sets unmatched price rows to 0 (not NULL), making "no price available" indistinguishable from a genuine zero-value transfer. Warehouse confirmed ~22,549 rows (0.5% of 4.48M) carry `amount_usd = 0`. The zero propagates to `int_execution_gpay_activity_daily`, `int_execution_gpay_conversions`, `int_execution_gpay_user_events_unified`, and all USD volume/revenue aggregates. Per project convention (see `feedback_clickhouse_left_join_nulls.md`), add `join_use_nulls` hook and retain `coalesce(..., 0)` as an explicit, documented fallback.

**`api_execution_gpay_user_top_wallets`: tier1 production endpoint with no `schema.yml` entry — fails `check_api_tags.py` `columns_missing`**
`models/execution/gpay/marts/api_execution_gpay_user_top_wallets.sql`, `scripts/checks/check_api_tags.py`

The model carries tags `api:gpay_user_top_wallets`, `tier1`, and `production` but has zero columns documented in `marts/schema.yml`. The CI guard (`check_api_tags.py` rule `columns_missing`, lines 81–84) requires every `production api:` endpoint to have at least one typed column. `fct_execution_gpay_users_distinct` is also absent from `schema.yml`. Either the CI guard is not running on this branch or these were merged without it — a process gap beyond a style issue. Add complete typed column schemas and re-run the guard.

### Medium severity

**`unique_key` on append-strategy decode streams is unenforced; row-level consumers exposed to duplicates**
`models/execution/gpay/intermediate/int_execution_gpay_roles_events.sql`, `int_execution_gpay_delay_events.sql`, `int_execution_gpay_spender_events.sql`

These three models use `incremental_strategy='append'` yet declare `unique_key`. dbt-clickhouse does not enforce `unique_key` for append — the declaration is metadata only. Duplicate prevention relies entirely on ReplacingMergeTree async merge (requires FINAL or a completed merge). Current downstream consumers (`int_execution_gpay_safe_modules`, `int_execution_gpay_allowances_current`) use `argMax`/`argMin` aggregation, which is duplicate-safe. But any future row-level reader is exposed without warning. Document this as metadata-only and require FINAL for direct reads.

**Monthly aggregate tables stale ~40 days; activity spine 3 days stale; no freshness tests**
`models/execution/gpay/intermediate/int_execution_gpay_activity.sql`, `models/execution/gpay/marts/fct_execution_gpay_churn_monthly.sql`, `fct_execution_gpay_cashback_impact_monthly.sql`

`fct_churn_monthly` and `fct_cashback_impact_monthly` max month = 2026-05-01 vs today 2026-06-11 (40-day lag). The current-month exclusion explains missing June, but May closed 11 days ago. The activity spine max date = 2026-06-08 (3-day lag from today). No freshness or recency tests are defined on these models. Add freshness tests and investigate whether the monthly pipeline cadence fires after month-close.

**ASOF LEFT JOIN on unsorted CTE in `fct_execution_gpay_cashback_impact_monthly` is non-deterministic**
`models/execution/gpay/marts/fct_execution_gpay_cashback_impact_monthly.sql`

The `ASOF LEFT JOIN` on `cashback_cumulative` requires the right side to be sorted on the ASOF key (`month`). The CTE is produced by a window function with no explicit `ORDER BY`; ClickHouse does not guarantee CTE row order. Data sampling shows coherent segment splits today, so this is latent. Add `ORDER BY month` to the `cashback_cumulative` CTE or replace with a nearest-prior equality join.

**`int_execution_gpay_activity_daily` GP-to-GP transfers double-count portfolio-level volume**
`models/execution/gpay/intermediate/int_execution_gpay_activity.sql`

When both sender and receiver of an ERC-20 transfer are GP wallets, the classified CTE emits two rows (Crypto Withdrawal for sender, Crypto Deposit for receiver). Warehouse confirmed 1,705 such outbound events. Any cross-wallet "total ecosystem volume" metric is inflated by this amount. The pattern is a defensible wallet-centric ledger design but is undocumented.

**`int_execution_gpay_wallets` carries inert batch logic that could leave the table in a partial state**
`models/execution/gpay/intermediate/int_execution_gpay_wallets.sql`

The model is `materialized='table'`, so `is_incremental()` always returns False and `apply_monthly_incremental_filter` is a no-op on all normal runs. A manual `--var 'start_month:...'` run activates the window filter, which would scope the wallet universe and leave an incomplete table with no guard. Remove the dead path or add an explicit guard.

### Low severity

**Cumulative window functions in `fct_actions_by_token_daily/weekly` rely on ClickHouse default ROWS frame**
`models/execution/gpay/marts/fct_execution_gpay_actions_by_token_daily.sql`, `fct_execution_gpay_actions_by_token_weekly.sql`

`SUM(volume) OVER (PARTITION BY action, token ORDER BY date)` omits `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. ClickHouse defaults to the correct frame today but implicit reliance is fragile across engine versions. Add the explicit frame clause.

**Redundant `ORDER BY` in `int_execution_gpay_activity` final SELECT and view subqueries**
`models/execution/gpay/intermediate/int_execution_gpay_activity.sql`, `models/execution/gpay/marts/api_execution_gpay_flows_snapshot.sql`

The `ORDER BY c.wallet_address, c.block_timestamp` at the end of the INSERT for a `ReplacingMergeTree` with `insert_overwrite` adds per-run sort cost with no effect on the physical sort (controlled by the table's `ORDER BY` key). Subquery `ORDER BY` in `api_flows_snapshot` is not guaranteed to propagate in ClickHouse. Remove or move to outer SELECT/client.

**Commented-out token and 'Other'-action filters silently admit unintended rows**
`models/execution/gpay/intermediate/int_execution_gpay_activity.sql`

The token whitelist filter (`--WHERE symbol IN (...)`) and the `WHERE action != 'Other'` filter are both commented out. Any token added to `tokens_whitelist` now flows into GP analytics, and `Other`-classified transfers (0 rows today) would pass into all aggregations if they appear. Make scoping explicit with a tag-based whitelist filter.

**`int_execution_gpay_conversions` full-activity scan on every run**
`models/execution/gpay/intermediate/int_execution_gpay_conversions.sql`

The `first_inflow` CTE reads all 4.4M+ rows from `int_execution_gpay_activity` with no incremental filter. The subsequent `WHERE first_inflow_ts` clause narrows which Safes are emitted, but the full scan cost is paid every run. Filtering to Safes not yet present in `{{ this }}` would reduce scan cost by orders of magnitude.

---

## Business-logic assessment

### High severity

**Cashback endpoints publish native GNO as "USD" — ~100–300x consumer-facing misstatement**
`models/execution/gpay/marts/api_execution_gpay_user_total_cashback.sql`, `api_execution_gpay_user_cashback_daily.sql`, `schema.yml`

`api_execution_gpay_user_total_cashback` returns `sum(amount)` (native GNO) and `api_execution_gpay_user_cashback_daily` returns `round(toFloat64(amount), 6)` (also native GNO). However, `schema.yml` line 3809 documents the value column as "The total cashback amount in USD." GNO trades at approximately $100–$300, so a portfolio consumer relying on the documented unit will misread the figure by two orders of magnitude on a money value shown in the user portfolio. This is the single most damaging defect for external trust. Fix: either switch both endpoints to `amount_usd`, or correct the description from "in USD" to "in GNO" and rename the column for clarity.

**`api_execution_gpay_total_funded` conflates funding with paying — ships payment users, not funded wallets**
`models/execution/gpay/marts/api_execution_gpay_total_funded.sql`, `fct_execution_gpay_snapshots.sql`, `intermediate/int_execution_gpay_conversions.sql`

The canonical `gpay_funded` definition in `int_execution_gpay_conversions` is the first inflow (Fiat Top Up OR Crypto Deposit) per Safe — a wallet has received money. But `api_execution_gpay_total_funded` reads `fct_execution_gpay_snapshots WHERE label='PaymentUsers'` (wallets with at least one Payment) and surfaces it as `funded_addresses`. In a card product, funding and paying are distinct funnel stages. The endpoint name and label create a misleading signal at the product level, and aligns the funnel incorrectly for stakeholder reporting. Confirm intended definition and align endpoint name, label, and source to one canonical concept.

**`churn_rate` and `retention_rate` use inconsistent denominators (current-month vs prior-month active)**
`models/execution/gpay/marts/fct_execution_gpay_churn_monthly.sql`

`churn_rate = churned_users / greatest(total_active_current, 1)` uses the current month's active base (lines 76, 149). Two lines below, `retention_rate` correctly uses `lagInFrame(total_active, 1)` (prior-month base). The two metrics are not complementary: a stakeholder comparing them will be misled into believing they relate to the same cohort. Pick one cohort base — prior-month is the industry standard and is already computed in the same SELECT — and apply it to both metrics.

### Medium severity

**Division-by-zero guard missing in `api_execution_gpay_wallet_balance_composition`**
`models/execution/gpay/marts/api_execution_gpay_wallet_balance_composition.sql`

The expression `balance_usd / t.total_usd >= 0.01` (line 24) has no `greatest(total_usd, 1)` or `nullIf` guard. If all GP wallets carry zero USD balance on the latest date (e.g., after a data gap), `total_usd` is 0/NULL and ClickHouse emits `inf`/`nan` rows surfaced directly to the tier1 endpoint. Add the guard.

**`api_execution_gpay_user_activity` exposes per-wallet transaction history at tier0 (public-access)**
`models/execution/gpay/marts/api_execution_gpay_user_activity.sql`

The model exposes `transaction_hash`, `wallet_address`, `block_timestamp`, amounts, and counterparty at per-event grain. It is protected by `allow_unfiltered:false` and `require_any_of:[wallet_address]`, preventing bulk extraction, but the `tier0` tag designates it as public-access. Project convention (see `project_api_tag_convention.md`) reserves `tier1` for data requiring an API key; per-user financial transaction history is a tier1 concern even with pseudonymous addresses. Promote to `tier1`.

**Retention `initial_users` computed via `max(users)` rather than cohort month-0 value — fragile across cohorts**
`models/execution/gpay/marts/fct_execution_gpay_retention_monthly.sql`, `fct_execution_gpay_retention_by_action_monthly.sql`, `fct_execution_gpay_cashback_cohort_retention_monthly.sql`

All three models use `max(users) OVER (PARTITION BY cohort_month)` as `initial_users`. If a later month exceeds month-0 (back-attributed late joiners) or the month-0 row is absent from the INNER JOIN, `max` picks the wrong base, overstating retention percentages. Cross-checks confirm max == month-0 in live data (monotone decay), so the defect is latent. The division also lacks a `nullIf`/`greatest` guard, making a zero-cohort edge case a runtime arithmetic error. Replace with `minIf(users, months_since = 0)` or an explicit month-0 join, and add the division guard.

**`fct_execution_gpay_snapshots` change_pct returns -100% when both periods are zero — live on FiatOfframp and Reversal**
`models/execution/gpay/marts/fct_execution_gpay_snapshots.sql`

The formula `(coalesce(curr / nullIf(prev, 0), 0) - 1) * 100` returns -100% when `prev = 0` (current period: NULL → 0 via coalesce, minus 1 → -100%). This is live for `FiatOfframpVolume`, `FiatOfframpCount`, `FiatOfframpUsers`, `ReversalVolume`, `ReversalCount`, and `ReversalUsers`, and would also misreport a genuinely fresh label (prev=0, curr>0) as -100% instead of NULL or +inf. Dashboards reading these endpoints will display a 100% drop for inactive/new product lines. Return NULL or a sentinel when `prev = 0`.

**The snapshots 7D window excludes `max_date - 7` — consistent 6-day effective window**
`models/execution/gpay/marts/fct_execution_gpay_snapshots.sql`

The bounds CTE sets `curr_start = max_date - 7` and filters `d.date > b.curr_start AND d.date <= b.curr_end`. This is 6 full days plus the partial max_date day; the earliest day (`max_date - 7`) is never counted in either window. If the intent is a rolling 7-calendar-day window, change to `d.date >= b.curr_start`.

**Hardcoded, unregistered classifier addresses create silent-breakage risk on contract migration**
`models/execution/gpay/intermediate/int_execution_gpay_wallets.sql`, `int_execution_gpay_activity.sql`

The spender address `0x4822521e6135cd2599199c83ea35179229a172ee` is hardcoded in two SQL files and one schema description but is absent from all seed registries (`contracts_whitelist.csv`, `dao_treasury_wallets.csv`, `contracts_factory_registry.csv`). No external on-chain verification citation exists in the codebase. A contract migration would silently stop Payment classification and zero active-user metrics. The cashback wallet `0xcdf50be9061086e2ecfe6e4a1bf9164d43568eec` is registered in `dao_treasury_wallets.csv` but also hardcoded in SQL without a dbt var reference. Register both in a seed/var and add a row-count/recency guard on Payment volume.

### Low severity

**Semantic layer dual-registers `api_*` and `fct_*` models for the same metric; MTA tables uncovered**
`semantic/authoring/execution/gpay/semantic_models.yml`

349 semantic entries include both `execution_gpay_*` (api views) and `fct_execution_gpay_*` (underlying tables) for the same metrics, risking MCP planner routing to the `fct_` table and bypassing API-layer filtering or format transformation. MTA journey/attribution models (`fct_execution_gpay_journeys_*`, `fct_execution_gpay_attribution_*`, `int_execution_gpay_conversions`, `int_execution_gpay_coverage_daily`) have no semantic entries. Deduplicate to the `api_*` canonical endpoint and confirm MTA exclusion is intentional.

**Attribution API granularity tags carry `rolling_180d` — non-standard value inconsistent with project convention**
`models/execution/gpay/marts/api_execution_gpay_attribution_30d.sql`, `api_execution_gpay_attribution_60d.sql`, `api_execution_gpay_attribution_7d.sql`

The `granularity:rolling_180d` tag documents the underlying fact window size, not the output grain. Every other model in the project uses time-period grain identifiers (daily, weekly, monthly, all_time). The CI guard accepts any free-form value, so it passes, but breaks self-documenting discoverability.

---

## Data findings

All figures from warehouse queries run by the inspection shards against production data as of 2026-06-11.

| Finding | Value |
|---|---|
| Total rows in `int_execution_gpay_activity` | 4,480,505 |
| Distinct GP wallets in activity | 34,771 |
| Activity rows with `amount_usd = 0` (missing price) | 22,549 (0.5%) |
| `int_execution_gpay_activity_daily` uniqueness violations | 234 groups with 2 distinct directions |
| GP-to-GP internal transfers (double-counted volume) | 1,705 outbound events |
| `int_execution_gpay_activity` max date vs today | 2026-06-08 (3-day lag) |
| `fct_churn_monthly` / `fct_cashback_impact_monthly` max month | 2026-05-01 (40-day lag) |
| Funded Safes with only 2 conversion rows (no delegate) | 1,770 (~5.4% of funded Safes) |
| `fct_execution_gpay_snapshots` PaymentUsers All-time | 34,771 |
| `fct_execution_gpay_snapshots` TotalBalance (USD) | ~$2.63M |
| PaymentUsers 7D change_pct | -62.3% (warrants investigation) |
| Cashback token diversity (fct_actions_by_token_weekly) | GNO only, 89 weeks — no fan-out risk today |
| Retention initial_users = month-0 users | confirmed across all cohorts (0 discrepancies) |
| RMT duplicate count on snapshots and actions_by_token_daily | 0 — deduplication operating correctly |

The -62.3% week-on-week drop in PaymentUsers (7D) and the -89.9% in CryptoWithdrawal were noted but not root-caused during this review; may reflect a cashback window boundary or a genuine activity event.

---

## Pros / Cons

**Pros**

- Strong, economically-grounded canonical definitions layer (`economic_concepts.md`) with explicit, defensible scoping: Payment-only active users, GNO cashback excluded from engagement, GA-WAU vs GP-WAU non-comparability documented.
- Privacy architecture is deliberate: salted pseudonyms for cross-domain Mixpanel joins; per-user endpoints gated by `allow_unfiltered:false` + `require_any_of:[wallet_address]` to prevent mass extraction.
- Event decoding is robust: three Zodiac module types decoded, mastercopies resolved dynamically (not hardcoded), operational wallets explicitly excluded from the user universe.
- Comprehensive analytics surface: activity time-series, balances, cohort retention, churn, flows, MTA attribution, and per-user portfolio views with consistent daily/weekly/monthly grains.
- Dedup-safe aggregation patterns: downstream `argMax`/`argMin` over append-strategy decode streams correctly tolerate RMT duplicates.
- Good incremental hygiene on the conversions funded path: all-time history is scanned before the incremental window filter, so first-inflow milestones are not truncated.
- Thorough inline documentation and 349-entry semantic-layer coverage for dashboard and portfolio surfaces.
- Elementary anomaly and schema-change tests present on high-traffic models.

**Cons**

- Consumer-facing unit lie: cashback endpoints publish native GNO as "USD" — a ~100–300x misstatement on a money figure shown in the user portfolio.
- Central activity_daily spine can silently lose a direction's amounts via a RMT key that omits `direction`; 20+ downstream marts (volume, snapshots, KPI) inherit the corrupted aggregates.
- Two "funded" definitions coexist (`snapshots PaymentUsers` vs `conversions gpay_funded = first inflow`); the public endpoint ships the payment-based one, conflating funding with paying in a card product where they are distinct funnel stages.
- Missing `join_use_nulls` hooks turn "no price available" into silent USD = 0, undercounting volume/revenue for unpriced tokens (~0.5% of rows).
- Churn and retention rates in the same fact table use inconsistent denominators (current-month vs prior-month active), making them non-complementary to stakeholders.
- API-tag/schema CI guard is being bypassed: a tier1 production endpoint (`user_top_wallets`) and a semantic-layer grain table (`users_distinct`) ship with no documented columns.
- Privacy-tier drift: per-wallet transaction history (hashes, amounts, counterparties) is exposed at `tier0` (public) rather than `tier1`.
- Critical classifier address (spender) is hardcoded in SQL with no seed/var registration or external verification citation — silent-breakage risk on contract migration.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 | Fix the cashback unit: switch `user_total_cashback` and `user_cashback_daily` to `amount_usd`, or correct `schema.yml` from "in USD" to "in GNO" and rename the column. Live money misstatement in user portfolio. | `api_execution_gpay_user_total_cashback.sql`, `api_execution_gpay_user_cashback_daily.sql`, `schema.yml` |
| P0 | Add `direction` to `int_execution_gpay_activity_daily` `order_by` and uniqueness test, then full-refresh the table and all downstream marts. Data loss on the central spine. | `int_execution_gpay_activity_daily.sql`, `schema.yml`, all 20+ downstream marts |
| P1 | Add `join_use_nulls` pre/post hooks to the price LEFT JOIN in `int_execution_gpay_activity`. Keep `coalesce(..., 0)` as an explicit, documented fallback. | `int_execution_gpay_activity.sql` |
| P1 | Register the spender address in a seed/var with an external on-chain verification citation. Add a row-count/recency guard that alarms if Payment volume drops to zero. | `int_execution_gpay_wallets.sql`, `int_execution_gpay_activity.sql` |
| P1 | Resolve the "funded" definition conflict: decide whether `api_execution_gpay_total_funded` tracks first-inflow (gpay_funded) or payment users, then align the endpoint name, label, and source. | `api_execution_gpay_total_funded.sql`, `fct_execution_gpay_snapshots.sql`, `int_execution_gpay_conversions.sql` |
| P1 | Add complete typed column schemas for `api_execution_gpay_user_top_wallets` and `fct_execution_gpay_users_distinct` in `schema.yml`, and confirm `check_api_tags.py` runs in CI on this branch. | `schema.yml`, `scripts/checks/check_api_tags.py` |
| P2 | Align `churn_rate` to the prior-month active denominator (`lagInFrame(total_active, 1)`) so it is consistent with `retention_rate`. Document the chosen definition for board reporting. | `fct_execution_gpay_churn_monthly.sql` |
| P2 | Fix `fct_execution_gpay_snapshots` `change_pct` to return NULL when `prev = 0`. Adjust the 7D window from `>` to `>=` at the start boundary. | `fct_execution_gpay_snapshots.sql` |
| P2 | Promote `api_execution_gpay_user_activity` from `tier0` to `tier1`. Add `greatest()/nullIf` guard to `api_execution_gpay_wallet_balance_composition` division. | `api_execution_gpay_user_activity.sql`, `api_execution_gpay_wallet_balance_composition.sql` |
| P2 | Replace `max(users)` initial_users with `minIf(users, months_since = 0)` in all three retention models. Add `greatest(initial_users, 1)` division guard. | `fct_execution_gpay_retention_monthly.sql`, `fct_execution_gpay_retention_by_action_monthly.sql`, `fct_execution_gpay_cashback_cohort_retention_monthly.sql` |
| P2 | Add freshness/recency tests on `int_execution_gpay_activity` and the monthly marts. Investigate pipeline cadence for post-month-close refresh. | `int_execution_gpay_activity.sql`, `fct_execution_gpay_churn_monthly.sql`, `fct_execution_gpay_cashback_impact_monthly.sql` |
| P3 | Add `ORDER BY month` to the `cashback_cumulative` CTE in `fct_execution_gpay_cashback_impact_monthly`, or replace the ASOF join with a nearest-prior equality join. | `fct_execution_gpay_cashback_impact_monthly.sql` |
| P3 | Add explicit `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` frames to cumulative window functions in `fct_actions_by_token_daily/weekly`. | `fct_execution_gpay_actions_by_token_daily.sql`, `fct_execution_gpay_actions_by_token_weekly.sql` |
| P3 | Deduplicate semantic layer to `api_*` canonical endpoints only. Confirm MTA attribution models are intentionally excluded. | `semantic/authoring/execution/gpay/semantic_models.yml` |
| P3 | Remove dead `apply_monthly_incremental_filter` paths from `int_execution_gpay_wallets` (table materialization makes them inert) or add an explicit guard against partial-table runs. | `int_execution_gpay_wallets.sql` |

---

## Open disagreements

None. Review converged in round 1.

---

## Review log

**Round 1 — Inspector challenges and resolution:**

- Inspector (intermediate shard) challenged: is the `activity_daily` uniqueness test gap intentional or an oversight? Verdict confirms it is a combined test gap + RMT key defect, not just documentation.
- Inspector (intermediate shard) challenged: is `coalesce(price, 0)` on LEFT JOIN intentional? Verdict confirms it should follow project convention (join_use_nulls hook) and is a defect.
- Inspector (marts-1 shard) raised `change_pct` formula producing -100% when `prev=0`; verified live on FiatOfframp/Reversal in warehouse. Confirmed as medium business-logic defect.
- Inspector (marts-1 shard) raised churn denominator inconsistency; shard-2 independently confirmed from the same model. Final verdict refined framing from "understatement" to "definition-consistency defect."
- Inspector (marts-2 shard) confirmed unit mismatch (GNO vs USD) in cashback endpoints by reading both SQL and `schema.yml`; independently verified by verdict agent. Elevated to highest-priority finding.
- Inspector (marts-2 shard) raised missing `schema.yml` entries for `user_top_wallets` and `users_distinct`; confirmed by verdict agent as a CI-guard process gap.
- Context shard clarified dual "funded" definitions; verdict agent reconciled against both mart shards' findings and confirmed as a business-logic defect.
- No challenges were rebutted. All open inspector questions either resolved by context report canonical definitions or recorded as owner-decision items in recommendations.
