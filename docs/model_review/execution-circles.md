# Model review: execution/Circles

**Convergence:** converged in 1 round — all three shard reports (intermediate, marts-1, marts-2) covered disjoint file sets with no contradictions; context report aligned; every load-bearing claim was independently verified via SQL and file reads before the verdict.

---

## Scope and inventory

The `execution/Circles` sector is the largest single-protocol analytics unit in the project, covering both Circles v1 (Hub deployed 2020-10-01) and v2 (Hub deployed 2024-10-01).

| Layer | Model count | Notes |
|---|---|---|
| Intermediate | 47 | Raw decode → cleaned events, SCD2 trust, demurrage-adjusted balances, invite funnel, backing lifecycle, pricing |
| Marts (fct_) | ~25 | Materialized fact tables: trusts, balances, supply, minters, cohorts, group economies |
| Marts (api_) | ~57 | Thin views serving the Circles Grafana dashboard, Cerebro MCP, and GA WEAU pipeline |
| Semantic models | ~15 | Mix of approved, candidate (several misconfigured) |

Total: approximately 129 SQL files plus a 1,500+ line `schema.yml`.

---

## Business context

The unit answers twelve business-question domains:

1. Ecosystem growth: Human/Group/Org registrations, daily and cumulative.
2. Protocol health (canonical Active Minter KPI): avatars that minted on each of the last 14 consecutive days with a 14-day cumulative sum >= 268.8 CRC (80% of 336 CRC theoretical maximum).
3. Trust-graph density: daily new/revoked trusts, cumulative active-trust stock, degree distribution.
4. Token supply and holders: network-wide nominal and demurrage-adjusted supply, supply by holder type (Human/Group/Org/DEX/AA wallets), wrapped vs unwrapped share, per-avatar snapshots.
5. Group economies: group size distribution, group-token supply leaderboard, collateral daily balances.
6. Invite funnel and referral economy: five-stage mint-cadence funnel per cohort-month; inviter leaderboard; invitation-fee events from same-tx wrapped-CRC transfers.
7. Economically active avatars (WEAU upstream): weekly earners via gCRC cashback (>= 1 gCRC from `circles_v2_cashback_wallet`) or inviter fees (>= 1 CRC); `is_gnosis_app_tx` flag for downstream GA WEAU in-app filtering.
8. Backing lifecycle: CirclesBackingInitiated depositor funnel, trust-defined backers under `circles_target_group_address`, depositor-to-backer conversion KPI.
9. CRC20 DEX pricing: trade-level and daily VWAP/median for ERC-20 wrapper tokens across Uniswap V3 and Swapr V3 pools.
10. Avatar identity: IPFS metadata from NameRegistry UpdateMetadataDigest; search lookup for the dashboard global filter.
11. ERC-20 wrapper supply: daily wrap/unwrap delta and cumulative wrapped supply per wrapper.
12. Per-avatar detail panels and cross-sector pseudonym bridge to GA/GP/Mixpanel overlap analysis.

**Key canonical definitions:**

- **Active Minter:** `mint_days_14dw = 14 AND mint_14dw >= 268.8 CRC`. Source: `fct_execution_circles_v2_active_minters_daily`.
- **Economically Active Avatar (ecosystem-wide):** earned >= 1 gCRC cashback OR >= 1 CRC inviter fee in the calendar week. GA WEAU filters downstream by `any_in_app_tx = 1`.
- **Trust (active, v2):** SCD2 interval with `valid_to IS NULL` or `valid_to > now()`. Trust (active, v1): `trust_limit > 0`.
- **Backer (trust-defined):** address currently trusted by `circles_target_group_address` (0x1aca75e38263c79d9d4f10df0635cc6fcfe6f026, start 2025-04-25).
- **Demurrage unit:** all CRC amounts normalized to demurrage. Annual decay 7%, daily factor gamma = 0.9998013320085989, inflation day zero = Unix 1602720000.
- **Invite funnel stages 1-5:** Invited → >= 2 mint days in first 30d → >= 7 → >= 14 → canonical Active Minter (lifetime, not 30-day).

**Contract context:** v1 Hub 0x29b9a7fbb8995b2423a71cc17cf9810798f6c543; v2 Hub 0xc12c1e50abb450d6205ea2c3fa861b3b834d13e8; ERC20Lift, NameRegistry, StandardTreasury, CMGroupDeployer, BaseGroupFactory, CirclesBackingFactory, InvitationModule, ReferralsModule, PaymentGatewayFactory all tracked via `contracts_circles_registry_static.csv`. `circles_target_group_address` and `circles_v2_cashback_wallet` are dbt_project.yml vars, not in the static registry — their correctness relies on configuration alone.

---

## Implementation assessment

### Critical

**`int_execution_circles_v1_transfers` SELECT has drifted below the live table schema; breaks `int_execution_circles_v1_balance_diffs` on next full refresh.**
The model's final SELECT emits 9 columns. The deployed table (confirmed via `describe_table`) still carries 13 columns from a prior build, including `batch_index`, `token_id`, `operator`, and `transfer_type`. `int_execution_circles_v1_balance_diffs` explicitly selects `batch_index`, `token_id`, and `transfer_type` from `int_execution_circles_v1_transfers`. The next `dbt run --full-refresh` of the transfers model will recreate the table without those columns, causing an immediate compile/query failure in balance_diffs. The current warehouse data is safe only until the next full refresh is triggered.
_Models:_ `models/execution/Circles/intermediate/int_execution_circles_v1_transfers.sql`, `models/execution/Circles/intermediate/int_execution_circles_v1_balance_diffs.sql`

### High

**`api_execution_circles_v2_wrapper_share_daily` serves a live sawtooth: `wrapped_supply = 0` on 57 of 566 days.**
`wrapped_cum` accumulates only over dates present in `wrapper_daily` (event-only dates). The LEFT JOIN onto the dense total series yields NULL on no-event days, which `coalesce` sets to 0, collapsing `wrapped_pct` to 0 on those days. Confirmed in the warehouse: 57/566 days affected, max correct value 18.2M CRC. The fix is forward-fill via `last_value(...) IGNORE NULLS OVER (ORDER BY date)` or an equivalent fill-forward CTE.
_Model:_ `models/execution/Circles/marts/api_execution_circles_v2_wrapper_share_daily.sql`

**`is_gnosis_app_tx` returns NULL instead of 0 for ~1% of inviter-fee events.**
`toUInt8(g.transaction_hash != '')` over a nullable `execution.transactions.transaction_hash` propagates NULL on unmatched LEFT JOIN rows. Data confirms 1,323 NULL rows alongside 129,246 rows flagged `1` and zero rows flagged `0`. Downstream WEAU filters on `is_gnosis_app_tx IN (0, 1)` silently drop these rows. Note: the 100%-of-non-NULL = 1 rate is not a join artifact — `inviter_fees` data begins 2025-12-22, post-relayer launch, so app dominance is expected. The NULL-vs-0 bug is still real. Fix: `toUInt8(isNotNull(g.transaction_hash))`.
_Models:_ `models/execution/Circles/intermediate/int_execution_circles_v2_inviter_fees.sql`, `models/execution/Circles/intermediate/int_execution_circles_v2_referrers.sql`

**V1 Circles stack is 70+ days stale and tagged `dev` while all v2 models are tagged `production`.**
`int_execution_circles_v1_transfers` max block: 2026-04-02; `int_execution_circles_v1_balances_daily` max date: 2026-03-27. All six v1 models (`v1_avatars`, `v1_transfers`, `v1_trust_updates`, `v1_trust_relations`, `v1_balance_diffs`, `v1_balances_daily`) carry `tag: 'dev'`. If CI or deployment scripts gate on these tags the entire v1 stack is excluded from production runs. The staleness most likely results from blocked incrementals after SQL was changed without a full refresh. Requires a deliberate decision: retire v1 explicitly or fix the pipeline, re-run the incrementals, and re-tag to `production`.
_Models:_ `models/execution/Circles/intermediate/int_execution_circles_v1_*.sql`

**`int_execution_circles_v1_trust_relations` incremental strategy cannot close open-interval `valid_to` in prior partitions.**
The model uses `delete+insert` keyed on `valid_from` (monthly partition). A trust interval whose `valid_from` is in an earlier month with `valid_to = NULL` will never be re-emitted when a later event should close it, because the incremental run only touches the current-month partition. Stale open intervals accumulate and inflate active-trust counts downstream. This is a structural SCD2 design gap that requires either a re-close pass over affected prior partitions or a full-table SCD2 rebuild strategy.
_Model:_ `models/execution/Circles/intermediate/int_execution_circles_v1_trust_relations.sql`

**`api_execution_circles_v2_crc20_prices_daily` is missing all required CI tag rules.**
Materialized as a `ReplacingMergeTree` table with tags `['production','execution','circles_v2','prices']`, but carries no `api:`, `granularity:`, or `tier:` tag. `check_api_tags.py` will flag all three rules absent.
_Model:_ `models/execution/Circles/marts/api_execution_circles_v2_crc20_prices_daily.sql`

**`fct_execution_circles_human_avatars_distinct` reads an `api_` view (layer inversion).**
This `production/circles/mixpanel` fact table reads from `api_execution_circles_v2_avatar_metadata`, a view that sits above it in the DAG. A dashboard-facing view is now a build-time dependency of a semantic-layer fact, risking circular-refresh issues. Refactor to read `int_execution_circles_v2_avatar_metadata` and `int_execution_circles_v2_avatars` directly.
_Model:_ `models/execution/Circles/marts/fct_execution_circles_human_avatars_distinct.sql`

### Medium

**Three `cnt_latest` KPI views divide by `p.value` without `nullIf`; an empty prior CTE can suppress all output rows.**
`api_execution_circles_v2_groups_cnt_latest`, `api_execution_circles_v2_humans_cnt_latest`, and `api_execution_circles_v2_active_trusts_cnt_latest` compute `(c.value - p.value) / p.value * 100` with no `nullIf` guard. If the exact prior date has no data row, the CROSS JOIN with an empty prior returns no rows at all rather than a NULL change_pct. `api_execution_circles_v2_kpi_active_minters_latest` uses `nullIf(p.value, 0)` correctly and should be the template.
_Models:_ `models/execution/Circles/marts/api_execution_circles_v2_groups_cnt_latest.sql`, `api_execution_circles_v2_humans_cnt_latest.sql`, `api_execution_circles_v2_active_trusts_cnt_latest.sql`

**`api_execution_circles_v2_orgs_cnt_latest` deployed view is missing `as_of_date` column.**
The SQL wraps a subquery adding `as_of_date` and `schema.yml` documents it; `describe_table` confirms the live view has only `(total, change_pct)` — the view was not rebuilt after the `as_of_date` pattern was introduced. MCP and dashboard consumers expecting `as_of_date` receive an error or NULL. Also uses bare `p.value` (no `nullIf`) in the division. The other `cnt_latest` views should be audited for the same staleness.
_Model:_ `models/execution/Circles/marts/api_execution_circles_v2_orgs_cnt_latest.sql`

**Full-rebuild fact tables approach the CH Cloud 100-partition-per-insert cap (error 252).**
`fct_execution_circles_v2_avatar_balances_daily` has 24.35M rows across 21 monthly partitions; `fct_execution_circles_v2_supply_by_holder_type_daily` runs a full scan of `int_execution_circles_v2_balances_daily` on every rebuild across a growing history; `int_execution_circles_v2_trust_pair_ranges` is a full-rebuild MergeTree table (no partition) accumulating 466k pairs with max range_count of 1,053. As history grows, these will hit the per-insert partition cap. Convert to incremental `insert_overwrite` with a lookback window, or repartition to `toStartOfYear`.
_Models:_ `models/execution/Circles/marts/fct_execution_circles_v2_avatar_balances_daily.sql`, `fct_execution_circles_v2_supply_by_holder_type_daily.sql`, `models/execution/Circles/intermediate/int_execution_circles_v2_trust_pair_ranges.sql`

**Two intermediate production models absent from `schema.yml`; no tests or column documentation.**
`int_execution_circles_v2_referrers` and `int_execution_circles_v2_trust_pair_ranges` are tagged `production`, are downstream-referenced (`trust_pair_ranges` feeds `backers_current` and the trusts daily chain), but have zero schema.yml entries, no uniqueness tests, and no column docs.
_Models:_ `models/execution/Circles/intermediate/int_execution_circles_v2_referrers.sql`, `models/execution/Circles/intermediate/int_execution_circles_v2_trust_pair_ranges.sql`

**Avatar-snapshot `fct_` tables lack explicit `engine` / `order_by`; `fct_economically_active_avatars_weekly` uses `ReplacingMergeTree` on a full-rebuild table.**
Five avatar-snapshot fact tables (`balances_latest`, `token_distribution`, `tokens_held_count`, `trusts_summary`, `personal_token_supply_latest`) declare `materialized='table'` with no `engine=` or `order_by=`, relying on adapter defaults. All sibling `fct_` tables declare explicit ENGINE + ORDER BY. Separately, `fct_execution_circles_v2_economically_active_avatars_weekly` uses `ReplacingMergeTree()` on a full-rebuild model where RMT provides no benefit and can leave unmerged duplicates between the per-kind UNION ALL branches; plain `MergeTree` with a uniqueness test is correct.
_Models:_ `models/execution/Circles/marts/fct_execution_circles_v2_avatar_balances_latest.sql`, `fct_execution_circles_v2_economically_active_avatars_weekly.sql` and four siblings.

**`api_execution_circles_v2_avatars_current` exposes potentially in-flight registration rows with no filter or `as_of_date`.**
The view selects directly from `int_execution_circles_v2_avatars` with no `WHERE avatar IS NOT NULL`, no `date < today()` guard, and no `as_of_date` sentinel. Every other snapshot-granularity `api_` view guards or anchors the data.
_Model:_ `models/execution/Circles/marts/api_execution_circles_v2_avatars_current.sql`

**`fct_execution_circles_v2_tokens_supply_daily` documented as `table` but materialized as a view.**
`schema.yml` introduces it with table-level column tests but the model is a view passthrough over `int_execution_circles_v2_tokens_supply_daily`. Elementary schema_changes tooling will observe a VIEW not a TABLE.
_Model:_ `models/execution/Circles/marts/fct_execution_circles_v2_tokens_supply_daily.sql`

### Low

**`fct_execution_circles_v2_crc20_prices_daily` is a `ReplacingMergeTree` queried without FINAL; `price_avg_in_backing` is a simple mean, not VWAP.**
The compound RMT key `(date, crc20_token, backing_token, pool_address)` means multi-pool rows are legitimate and the `api_` VWAP aggregation collapses them correctly. Risk is low today, but `price_avg_in_backing` (unweighted average) can mislead analysts querying the `fct_` directly, diverging from the VWAP in the `api_` layer.
_Model:_ `models/execution/Circles/marts/fct_execution_circles_v2_crc20_prices_daily.sql`

**`api_execution_circles_v2_avatar_trusts_daily` and active trusts expose partial today row to direct `fct_` consumers.**
`fct_` calendar extends to `today()`. The `api_` views correctly filter `date < today()`, but direct `fct_` consumers may include a partial-day row without realising.
_Models:_ `models/execution/Circles/marts/fct_execution_circles_v2_avatar_trusts_daily.sql`, `fct_execution_circles_v2_active_trusts_daily.sql`

**`fct_execution_circles_human_avatars_distinct` (pseudonymized) lacks explicit privacy/MCP-exposure controls.**
Tagged `mixpanel` but no `expose_to_mcp: false` or `privacy_tier`. Sits under `models/execution/Circles/marts/`, outside the `dbt_project.yml` `mixpanel_ga` API exclusion path, so it may be accessible via MCP queries.
_Model:_ `models/execution/Circles/marts/fct_execution_circles_human_avatars_distinct.sql`

---

## Business-logic assessment

### High

**Peer 7-day KPI windows disagree by one day, breaking cross-tile comparison.**
`api_execution_circles_v2_kpi_mints_7d` and `kpi_new_trusts_7d` use `date >= today()-7 AND date < today()` (7 complete days: today-7 through today-1). `kpi_new_backers_7d` and `kpi_new_groups_7d` use `date > today()-7 AND date <= today()` (today-6 through today; today's row is empty under typical pipeline lag, yielding 6 effective days). Same-named "last 7 days" tiles on the dashboard therefore count different day spans. Standardize on `>= today()-7 AND < today()` across all four models.
_Models:_ `models/execution/Circles/marts/api_execution_circles_v2_kpi_new_backers_7d.sql`, `kpi_new_groups_7d.sql`, `kpi_mints_7d.sql`, `kpi_new_trusts_7d.sql`

**Misconfigured semantic candidate models will error at MCP query time.**
`semantic_models.yml` binds `execution_circles_v1_avatars`, `execution_circles_v2_avatars`, and `int_execution_circles_backing` to columns that do not exist in their source tables (`user_address`, `inviter_address`, `date`, `event_name`, `from_address`, `to_address`, `registration_event_id`, `source_table`, `cnt` — none present in the actual intermediate models). Any MCP metric query against these three semantic models will fail at runtime. The backing semantic model likely intended `api_execution_circles_v2_backing_events_daily`, not `int_execution_circles_v2_backing`. These must be remapped to real columns or deleted.
_File:_ `semantic/authoring/execution/Circles/semantic_models.yml`

### Medium

**`int_execution_circles_v2_groups_overview_daily` documents `n_groups_total` but never computes it.**
The model header explicitly lists `n_groups_total` (cumulative group count up to and including the day) as a delivered column. The SELECT outputs only `n_new_groups`, `n_collateral_events`, and `n_distinct_groups_acting`. Any consumer reading `n_groups_total` will get a column-not-found error or silent NULL. Either add `sum(n_new_groups) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING)` or remove from the header and schema.
_Model:_ `models/execution/Circles/intermediate/int_execution_circles_v2_groups_overview_daily.sql`

**`migration` mint_kind = 10,808 contradicts the "four historical migrate() calls" documentation.**
`int_execution_circles_v2_mint_events` breakdown: personal 376,500; group 133,073; migration 10,808; other 4. The model comment states the V2 Hub `migrate()` ABI was used only 4 times historically. Either the `migration_operators` join is matching a broader population than documented, or the comment is stale. The personal/migration split feeds the inviter-fee personal-only filter, so this discrepancy is analytically load-bearing. Reconcile and update the documentation.
_Model:_ `models/execution/Circles/intermediate/int_execution_circles_v2_mint_events.sql`

**WAU under-count: `active_avatars_weekly` omits two event streams with no tracked issue.**
`int_execution_circles_v2_active_avatars_weekly` intentionally excludes `NameRegistry.UpdateMetadataDigest` and `Hub.StreamCompleted` events (decoded staging models not yet present), diverging from the Dune reference query. The delta is unquantified and untracked. Without a ticket or measurement, the WAU under-count can persist silently into reported KPIs indefinitely.
_Model:_ `models/execution/Circles/intermediate/int_execution_circles_v2_active_avatars_weekly.sql`

**Personal-mint classifier in `mint_events` has contradictory documentation.**
`schema.yml` and the model header describe the personal-mint classifier as requiring `token_address = to_address` (self-token shape). The actual SQL uses only `avatar_type = 'Human'` (intentionally broader, covering cross-token mints). The code is correct; the documentation misleads reviewers and downstream consumers relying on the documented shape.
_Model:_ `models/execution/Circles/intermediate/int_execution_circles_v2_mint_events.sql`

**`api_execution_circles_v2_crc20_prices_daily` `price_median_usd` is a median-of-pool-medians, not volume-weighted.**
The api-level aggregation computes `median(price_median_usd)` across pools. This is statistically biased when pools have different trade volumes. The `VWAP` column is correctly volume-weighted. Schema.yml does not call out the limitation.
_Model:_ `models/execution/Circles/marts/api_execution_circles_v2_crc20_prices_daily.sql`

**Pseudonymized human-avatar bridge lacks explicit privacy and MCP exposure controls.**
`fct_execution_circles_human_avatars_distinct` hashes avatar addresses (sipHash64) into the cross-sector pseudonym space, is tagged `mixpanel`, but carries no `expose_to_mcp: false` or `privacy_tier`. It sits outside the `dbt_project.yml` `mixpanel_ga` API exclusion path and may be reachable via MCP queries.
_Model:_ `models/execution/Circles/marts/fct_execution_circles_human_avatars_distinct.sql`

### Low

**Relayer-launch floor `2025-11-12` hardcoded in two models.**
`gnosis_app_txs` CTE in both `int_execution_circles_v2_inviter_fees` and `int_execution_circles_v2_referrers` filters `block_timestamp >= toDateTime('2025-11-12')`. Intentional (GA relayer launch date) but duplicated; retroactive labelling would require a coordinated two-file edit. Promote to a project variable.
_Models:_ `models/execution/Circles/intermediate/int_execution_circles_v2_inviter_fees.sql`, `int_execution_circles_v2_referrers.sql`

**Invite funnel mixes time horizons without schema documentation.**
Funnel stages 2-4 count distinct mint days within the first 30 days; stage 5 (`n_active_minter`) counts ever reaching canonical Active Minter status (lifetime). The asymmetry can be misread as funnel drop-off within 30 days. Make the horizon explicit in `schema.yml`.
_Model:_ `models/execution/Circles/marts/api_execution_circles_v2_invite_funnel_cohort_monthly.sql`

---

## Data findings

Queries run across the three shards (8 intermediate queries + 8 marts-1 queries + 7 marts-2 queries = 23 total):

| Model | Metric | Result |
|---|---|---|
| `int_execution_circles_v2_transfers` | row count / max block_timestamp | 11,306,113 rows; 2026-06-08 19:38 (3 days before review) |
| `int_execution_circles_v1_transfers` | max block / staleness | 2026-04-02 (70 days stale) |
| `int_execution_circles_v1_balances_daily` | max date / staleness | 2026-03-27 (76 days stale) |
| `int_execution_circles_v2_balances_daily` | max date | 2026-06-02 (9 days stale) |
| `int_execution_circles_v2_mint_events` | mint_kind breakdown | personal 376,500; group 133,073; migration 10,808; other 4 |
| `int_execution_circles_v2_trust_pair_ranges` | pair count / max range_count | 466,421 pairs; max 1,053 |
| `int_execution_circles_v2_inviter_fees` | is_gnosis_app_tx distribution | =1: 129,246; NULL: 1,323; =0: 0 |
| `fct_execution_circles_v2_active_minters_daily` | max date | 2026-06-11 (current) |
| `fct_execution_circles_v2_active_trusts_daily` | min / max active_trusts; negative rows | min=41; max=374,441; zero negative rows |
| `fct_execution_circles_v2_avatar_balances_latest` | grain uniqueness | 101,627 rows; 0 duplicates |
| `fct_execution_circles_v2_avatar_balances_daily` | row count / partition count | 24.35M rows; 21 monthly partitions |
| `fct_execution_circles_v2_total_supply_daily` | max date / lag | 2026-06-07 (4 days stale) |
| `fct_execution_circles_v2_avatar_trusts_daily` | negative counts | neg_given=0; neg_received=0; 5,781,074 rows |
| `api_execution_circles_v2_orgs_cnt_latest` | column enumeration | only (total, change_pct) live; as_of_date absent |
| `api_execution_circles_v2_wrapper_share_daily` (inferred) | sawtooth days | 57 of 566 days have wrapped_supply=0 |

**Freshness summary:** V2 marts are mostly current to 2026-06-08 to 2026-06-11 (1-4 day lag, within expected SLA). V1 data is 70+ days stale. The 3-4 day lag on `total_supply_daily` and trust models is consistent across all reviewed tables and appears to be an operational pipeline lag rather than a model defect.

---

## Pros / Cons

**Pros**
- Comprehensive two-version (v1 and v2) coverage of the Circles protocol with explicit canonical definitions for all key KPIs (Active Minter, WEAU, Trust, Backer, Depositor) traceable to the Dune reference and `economic_concepts.md`.
- Strong adherence to project conventions in the majority of mart models: `join_use_nulls` pre/post hooks, `nullIf`-guarded divisions, dense calendar generation, and explicit ENGINE/ORDER BY on most fact tables.
- Deliberate ecosystem-wide vs in-app scoping design: `is_gnosis_app_tx` lets the GA WEAU filter downstream while the base layer remains ecosystem-wide; cashback-path limitation (no tx-origin) is documented.
- Demurrage normalization (gamma, inflation-day-zero) and dust threshold (1e15 wei) are documented, versioned, and consistently applied across both wrapper types.
- V2 freshness is healthy (most facts current to within 1-4 days); grain-uniqueness and no-negative-cumulative integrity checks pass on all queried trust and balance facts.
- Schema.yml coverage is broad (1,500+ lines across both marts files) with column-level documentation and tests on most models.
- Graph-exploration semantic models (trust, metadata, balances) are correctly bound and production-ready.

**Cons**
- A latent critical break ships silently: `int_execution_circles_v1_transfers` SQL has drifted 4 columns below its live table schema; the next full refresh destroys `int_execution_circles_v1_balance_diffs`.
- A live wrong-number bug is on the dashboard: `wrapper_share_daily` drops `wrapped_supply` to 0 on 57 no-event days, producing a sawtooth instead of a monotone cumulative line.
- The entire V1 analytics stack is 70+ days stale and deploy-gated out of production by `tag: 'dev'` — either a deliberate undocumented pause or a broken incremental pipeline that went undetected.
- Multiple definition/documentation drifts: `groups_overview_daily` documents but never computes `n_groups_total`; `mint_events` migration bucket contradicts the "4 historical calls" doc; personal-mint classifier doc contradicts code.
- Peer KPI windows disagree by one day (backers/groups = 6 data-days; mints/trusts = 7), undermining cross-tile comparison on the Circles Grafana dashboard.
- `is_gnosis_app_tx` NULL-not-0 bug silently excludes ~1% of inviter-fee events from WEAU `=1/=0` filters.
- Three semantic candidate models reference nonexistent columns and will error at MCP query time; broad mart coverage gaps exist across active minters, trust distribution, invite funnel, group token supply, and backing/depositor metrics.
- Privacy/layer hygiene gaps: `fct_human_avatars_distinct` (pseudonymized cross-sector bridge) inverts DAG layering by reading an `api_` view and lacks explicit MCP-exposure controls.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 — before next full refresh | Restore `batch_index`, `operator`, `token_id`, `transfer_type` to `int_execution_circles_v1_transfers` SELECT (carry as constants/aliases if v1 source lacks them); add a schema-contract test pinning output columns | `int_execution_circles_v1_transfers.sql`, `int_execution_circles_v1_balance_diffs.sql` |
| P0 — live dashboard bug | Fix `api_execution_circles_v2_wrapper_share_daily` to forward-fill `wrapped_supply` over a dense calendar (e.g., `last_value(...) IGNORE NULLS OVER (ORDER BY date)`) | `api_execution_circles_v2_wrapper_share_daily.sql` |
| P1 | Change `is_gnosis_app_tx` to `toUInt8(isNotNull(g.transaction_hash))` in `inviter_fees` and `referrers`; re-verify WEAU `=0` bucket is populated after the fix | `int_execution_circles_v2_inviter_fees.sql`, `int_execution_circles_v2_referrers.sql` |
| P1 | Resolve V1 stack intent: decide retire vs revive; if revive, re-run broken incrementals (70+ days stale) and re-tag all six v1 models from `dev` to `production`; if retire, mark deprecated and remove downstream references | `int_execution_circles_v1_*.sql` (6 models) |
| P1 | Fix or delete the three misconfigured semantic candidate models (`execution_circles_v1_avatars`, `execution_circles_v2_avatars`, `int_execution_circles_backing`) referencing nonexistent columns; reload semantic registry after | `semantic/authoring/execution/Circles/semantic_models.yml` |
| P1 | Standardize all four `*_7d` KPI models on `date >= today()-7 AND date < today()` (7 data-days); add a CI/macro test guarding window boundary consistency | `kpi_new_backers_7d.sql`, `kpi_new_groups_7d.sql`, `kpi_mints_7d.sql`, `kpi_new_trusts_7d.sql` |
| P1 | Rebuild `api_execution_circles_v2_orgs_cnt_latest` view to expose `as_of_date`; audit `groups_cnt_latest`, `humans_cnt_latest`, `active_trusts_cnt_latest` for the same staleness | `api_execution_circles_v2_orgs_cnt_latest.sql` and three peers |
| P2 | Add `nullIf(p.value, 0)` guards (and default-empty-prior handling) to `groups_cnt_latest`, `humans_cnt_latest`, `active_trusts_cnt_latest` | Three `cnt_latest` models |
| P2 | Add `n_groups_total` cumulative column to `groups_overview_daily` (or remove from header and schema.yml); fix the personal-mint classifier documentation to reflect the `avatar_type='Human'` logic | `int_execution_circles_v2_groups_overview_daily.sql`, `int_execution_circles_v2_mint_events.sql` |
| P2 | Reconcile the 10,808 migration mints against the "four historical calls" doc; confirm the `migration_operators` join scope | `int_execution_circles_v2_mint_events.sql` |
| P2 | Refactor `fct_execution_circles_human_avatars_distinct` to read `int_` sources (not `api_`); add `expose_to_mcp: false` and `privacy_tier` metadata | `fct_execution_circles_human_avatars_distinct.sql` |
| P2 | Add `api:`, `granularity:`, and `tier:` tags to `api_execution_circles_v2_crc20_prices_daily` | `api_execution_circles_v2_crc20_prices_daily.sql` |
| P2 | Add schema.yml entries, grain tests, and column docs for `int_execution_circles_v2_referrers` and `int_execution_circles_v2_trust_pair_ranges` | Two intermediate models |
| P3 | Convert `fct_avatar_balances_daily`, `supply_by_holder_type_daily`, and `trust_pair_ranges` to incremental `insert_overwrite` or repartition to `toStartOfYear` before they hit the CH Cloud 100-partition-per-insert cap | Three full-rebuild models |
| P3 | Replace `ReplacingMergeTree` with plain `MergeTree` + uniqueness test on `fct_economically_active_avatars_weekly`; add explicit `engine` / `order_by` to the five avatar-snapshot `fct_` tables | `fct_execution_circles_v2_economically_active_avatars_weekly.sql` and five snapshot tables |
| P3 | Promote the relayer-launch floor `2025-11-12` to a `dbt_project.yml` variable; document `price_median_usd` median-of-medians limitation in schema.yml; make invite-funnel stage-5 time horizon explicit | `inviter_fees.sql`, `referrers.sql`, `crc20_prices_daily.sql`, `invite_funnel_cohort_monthly.sql` |
| P3 | Quantify the WAU delta from the two missing event streams (`NameRegistry.UpdateMetadataDigest`, `Hub.StreamCompleted`) and open a tracking issue | `int_execution_circles_v2_active_avatars_weekly.sql` |

---

## Open disagreements

None. The review converged in one round.

---

## Review log

| Round | Agent | Challenge / Resolution |
|---|---|---|
| 1 | Inspector (intermediate) | Self-challenged: is the v1_transfers column mismatch a false positive given the warehouse still has 13 columns? Resolved: rebutted — warehouse carries the old schema from a prior build; the SQL regression manifests on the next full refresh, not now. |
| 1 | Inspector (intermediate) | Self-challenged: is the `is_gnosis_app_tx` NULL finding an artifact of NULL handling in COUNT? Resolved: rebutted — a GROUP BY query (not countIf) shows two explicit groups: =1 (129,246) and NULL (1,323), with no =0 group. |
| 1 | Inspector (intermediate) | Self-challenged: is the v1_trust_relations SCD2 gap a false positive? Resolved: rebutted — delete+insert on a monthly partition does not touch earlier-month valid_from rows regardless of any closing event in the current window. |
| 1 | Arbiter (final verdict) | Corrected inspector's "100% app dominance is suspicious" theory — inviter_fees data only starts 2025-12-22 (post-relayer launch), so near-100% in-app is plausible and not evidence of a join-always-matches artifact. NULL-vs-0 bug is still confirmed independently. |
