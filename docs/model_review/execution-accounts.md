# Model review: execution/accounts

**Convergence:** converged in 1 round — inspector and context reports were mutually consistent and complementary with no open disagreements; final verdict independently confirmed all critical findings.

---

## Scope and inventory

The `execution/accounts` sector is the backend for the Account Portfolio mini-app and tier-1 Cerebro API endpoints. It is not an analytics dashboard sector — population-level metrics (active addresses, new accounts) live in `execution/transactions`. The unit contains 28 SQL files across two sub-layers.

| Layer | Count | Purpose |
|---|---|---|
| Intermediate (`intermediate/`) | 3 | Balance history, token movements in/out (daily incremental) |
| Fact / helper (`marts/fct_*`) | 14 | Profile, resolver, balances, movements, counterparty graph, retention, search index, Gnosis App profile |
| API views (`marts/api_*`) | 14 | REST tier-1 endpoints consumed by Portfolio mini-app and Cerebro API |
| **Total** | **28** | |

Schema YML files cover both sub-layers. A CI guard (`check_api_tags.py`) enforces `api:` / `granularity:` / `window:` / `tier:` tag conventions, but the entire unit is exempted via a `columns_untyped` allowlist rather than tagging individual endpoints.

---

## Business context

The unit answers eight portfolio-lookup questions: who is address X (role classification and display name); what is its financial footprint (current token balances, USD value, daily balance history back to 2020-07); what activity has it performed (whitelisted token movement ledger, transaction summary, recent transactions); who does it interact with (counterparty graph edges); what is its relationship to Gnosis ecosystem entities (linked-entity navigation for MCP `navigate_portfolio_relation`); what Gnosis App / GPay activity does it have; what validators does it control; and what is the network-level cohort retention (a standalone population-level fact).

**Canonical definitions used in this unit:**

- `address_type`: binary — `safe` if the address appears in `int_execution_safes`, `eoa_or_contract` otherwise. Non-Safe contracts and plain EOAs are not differentiated.
- `display_name`: priority-ordered composition — Circles avatar handle > validator operator label > Safe owner label > Safe contract > Gnosis Pay wallet > Circles avatar > empty string.
- `first_seen_date`: `arrayMin` over coalesced sentinels (`toDate('2100-01-01')`) across first token-movement date, first GPay activity date, first yield date, first Gnosis App `seen_at`, and Safe creation date.
- `cohort` (retention): all addresses whose first successful transaction (`success=1`) landed in a given calendar month; identity pseudonymized as `cityHash64(lower(from_address))`.
- `retention`: retained at 30d/90d/180d is counted by calendar-month offsets 1/3/6 (cumulative), not exact days. Source activity table holds only 181 rolling days.
- `token movements`: transfers from `int_execution_transfers_whitelisted_daily` only; zero-address excluded; counterparty non-null enforced; `token_class = 'WHITELISTED'`.
- `is_validator_withdrawal_address`: derived from type-0x01/0x02 withdrawal credentials (last 20 bytes); BLS/0x00 credentials are excluded.

No smart contract addresses are hardcoded within the accounts unit SQL. Upstream seeds (Safe factory registry, ERC-4337 EntryPoint v0.7, Circles registry, GPay relayers) carry the protocol addresses.

---

## Implementation assessment

### Critical

**`fct_execution_account_token_movements_daily` is empty — all downstream transaction data zeroed**
`models/execution/accounts/marts/fct_execution_account_token_movements_daily.sql`

Confirmed 0 rows in the fct (with and without `FINAL`) while both int_ legs are fresh at 2026-06-11 with ~40 M combined rows (`in` = 20.27 M, `out` = 20.43 M). The fct is a thin `UNION ALL` pass-through, making this a failed or missing incremental refresh after branch migration, not a logic bug. Propagation is total: 1,318,764 of 1,318,764 profile rows (100 %) have `token_transfer_count` NULL/zero; `fct_execution_account_transaction_summary_latest`, `api_execution_account_recent_transactions`, and the `token_transfer` edges in `fct_execution_account_counterparty_edges_daily` are all empty.

Affected downstream: `models/execution/accounts/marts/fct_execution_account_transaction_summary_latest.sql`, `models/execution/accounts/marts/fct_execution_account_profile_latest.sql`, `models/execution/accounts/marts/api_execution_account_recent_transactions.sql`, `models/execution/accounts/marts/api_execution_account_transaction_summary_latest.sql`.

---

### High

**Two `api_` views over `ReplacingMergeTree` read without `FINAL` — can serve duplicate `(address, date)` rows**
`models/execution/accounts/marts/api_execution_account_balance_history_daily.sql`, `models/execution/accounts/marts/api_execution_account_token_movements_daily.sql`

`api_execution_account_balance_history_daily` is a view directly over `int_execution_account_balance_history_daily` (ReplacingMergeTree, monthly partitions). `api_execution_account_token_movements_daily` reads the fct (also ReplacingMergeTree). Neither view adds `FINAL`. Until ClickHouse background merges complete — which can lag hours in busy insert windows — consumers may receive duplicate rows. The `schema.yml` uniqueness test on `(address, date)` is windowed to the last 7 days and would not catch pre-merge duplicates at serve time.

**`fct_execution_account_token_balances_latest`: silent empty output on >14-day upstream gap**
`models/execution/accounts/marts/fct_execution_account_token_balances_latest.sql`

The `max(date)` CTE restricts its scan to `date >= today() - 14`. If `int_execution_tokens_balances_daily` has a gap longer than 14 days (failed backfill, maintenance window), `max_date` returns NULL, the subsequent `WHERE date = (SELECT max_date)` matches zero rows, and the table rebuilds empty with no error. All portfolio balance displays go blank. Current data is fresh (`max_date = today`), but the failure mode is silent.

**Retention 90d/180d for cohorts older than ~6 months is uncomputable on rebuild**
`models/execution/accounts/marts/fct_execution_network_retention_monthly.sql`

`int_execution_transactions_daily_active_addresses` hard-filters to the last 181 days. The retention model joins cohorts back to 2018-10 against this rolling window; on any full rebuild the 90d/180d rates (and some 30d rates) for cohorts older than ~6 months cannot be re-derived — values computed when those cohorts were fresh are now frozen and non-reproducible. A consumer comparing two report runs on a rebuilt table would see historical retention silently change or zero out.

**Dead `incr_end` variable in `int_execution_account_token_movements_in_daily` — microbatch intent never activates**
`models/execution/accounts/intermediate/int_execution_account_token_movements_in_daily.sql`, `models/execution/accounts/intermediate/int_execution_account_token_movements_out_daily.sql`

`in_daily` captures `mb_var('incremental_end_date')` into `incr_end` but never references the variable in its `WHERE` clause; config always uses `insert_overwrite`. `out_daily` does not capture `incr_end` at all. The `schema.yml` meta annotates both as microbatch-enabled, which cannot activate. The two siblings are inconsistent in intent.

---

### Medium

**`fct_execution_address_resolver`: `AggregatingMergeTree` stores plain ints, not aggregate states**
`models/execution/accounts/marts/fct_execution_address_resolver.sql`

Engine is `AggregatingMergeTree()` but the SELECT emits plain `UInt64`/`Int8` columns, not `AggregateFunction(max, ...)` states. Background merges therefore apply last-write-wins semantics (identical to `ReplacingMergeTree`), not state aggregation. Live data confirms partial merge: 1,099,448 rows against 1,093,444 distinct addresses, so multi-source rows coexist and the `api_` view's `GROUP BY address` + `max()` is the sole correctness guarantee. The documented "background merges collapse per-source rows" comment is wrong.

**`fct_execution_network_retention_monthly`: self-referential lookback fails on cold table and ignores late arrivals**
`models/execution/accounts/marts/fct_execution_network_retention_monthly.sql`

On an empty table `max()` returns NULL, collapsing the `monthly_active` window to NULL and landing all retention counts at zero. The 1-day lookback before `max(cohort_month)` also drops late-arriving daily-active rows for a closed cohort. Confirmed stale: latest cohort is 2026-05-01, 10 days behind today (2026-06-11).

**`fct_execution_gnosis_app_user_profile_latest`: `LEFT JOIN` without `join_use_nulls`**
`models/execution/accounts/marts/fct_execution_gnosis_app_user_profile_latest.sql`

LEFT JOINs `int_execution_gnosis_app_gpay_wallets` onto `int_execution_gnosis_app_users_current` without `SET join_use_nulls = 1` in a pre-hook. ClickHouse returns default-typed zero values (0 for integers, empty string for strings) instead of NULL for unmatched right-side rows. Consumers cannot distinguish "not a GPay wallet" (NULL) from "GPay wallet with zero owners" (0/`''`). This is a documented house convention for LEFT JOINs (see memory: `feedback_clickhouse_left_join_nulls.md`).

---

### Low

**`fct_execution_account_counterparty_edges_daily`: redundant `max(date) AS last_seen_date` in `token_edges` CTE**
`models/execution/accounts/marts/fct_execution_account_counterparty_edges_daily.sql`

`token_edges` groups by `(date, source, target, edge_type)` and computes `max(date) AS last_seen_date`. Since `date` is a GROUP BY key, `max(date) = date` always. The column is harmless but misleading; the true last-seen-date across time is correctly computed in the `_latest` rollup.

**`fct_execution_network_retention_monthly` missing `granularity:` tag**
`models/execution/accounts/marts/fct_execution_network_retention_monthly.sql`

Carries `['production', 'execution', 'accounts', 'monthly']` — no `granularity:` tag. The CI guard only enforces `granularity:` on `api:`-tagged models, so this passes CI. The absence makes the model invisible to any granularity-routed tooling.

---

## Business-logic assessment

### High

**Entire unit has zero semantic-layer coverage**

No `semantic/authoring/execution/accounts/` directory exists. None of balance, `first_seen_date`, token movements, or retention are queryable via `query_metrics` / `quick_metric_chart`. `preflight_analytics_request` returns `semantic_coverage_gap` for all portfolio-related queries and falls back to raw SQL. For a unit that backs the Portfolio mini-app and tier-1 API, this blocks governed cross-querying with OnChain Activity and any quarterly reporting pipeline that goes through the semantic layer.

---

### Medium

**Network retention fact is stranded — no `api_` view, not in semantic layer**
`models/execution/accounts/marts/fct_execution_network_retention_monthly.sql`

The model has no `api_*` view and no metric registration, so it is unreachable by REST API or MCP and undiscoverable except by direct SQL. If it is meant to power a cohort-retention dashboard tile, an `api_execution_network_retention_monthly` view with the standard `as_of_date` pattern is missing. If it is internal-only, its purpose should be documented to avoid treating it as an orphan fact.

**`address_type` is binary — advertised "EOA/contract breakdown" is not delivered**
`models/execution/accounts/marts/fct_execution_account_profile_latest.sql`, `models/execution/accounts/marts/fct_execution_address_resolver.sql`

The unit charter ("active addresses, new accounts, EOA/contract breakdowns") implies a contract-vs-EOA split. The implementation only distinguishes Safe from non-Safe; plain EOAs and non-Safe contracts (DEX routers, lending contracts, bridge relayers) are merged into `eoa_or_contract`. Consumers expecting a contract-vs-EOA split will be misled.

**Native xDAI coverage in balances/movements unverified**
`models/execution/accounts/marts/fct_execution_account_token_balances_latest.sql`, `models/execution/accounts/marts/fct_execution_account_token_movements_daily.sql`

Token movements and balance history derive from `int_execution_transfers_whitelisted_daily`. Whether native xDAI (via `native_transfers`) is included depends on `seeds/tokens_whitelist.csv`. If excluded, `total_balance_usd` in the portfolio omits native xDAI holdings — a material understatement for wallets whose primary asset is native gas token.

---

### Low

**Counterparty `_latest` graph has no recency window — can surface stale relationships**
`models/execution/accounts/marts/fct_execution_account_counterparty_edges_latest.sql`

`counterparty_edges_latest` aggregates daily edges across all history (2020-07 onward) with no date filter. The Relationships tab can show Safe/Circles links that are no longer current, and very active addresses may accumulate a graph too large to render. No top-N or recency window is documented on the `api_` view.

**Gnosis App heuristic floored at 2025-11-12 relayer `since_date`**
`models/execution/accounts/marts/fct_execution_gnosis_app_user_profile_latest.sql`

`is_gnosis_app_user` depends on the relayer seed (`seeds/gnosis_app_relayers.csv`) whose three bundlers all carry `since_date = 2025-11-12`. Addresses using earlier app versions may not be flagged, biasing GA cohort and profile counts downward for the pre-November 2025 period. This is not documented as a known floor.

---

## Data findings

Eleven warehouse queries were executed across the review.

| Query target | Result |
|---|---|
| `int_execution_account_balance_history_daily` | 296,577,514 rows; `max_date` = 2026-06-11; 422,098 distinct addresses; 0 duplicate `(address, date)` pairs in last 7 days |
| `fct_execution_account_token_movements_daily` (with and without FINAL) | **0 rows** (critical) |
| `int_execution_account_token_movements_in_daily` | 20,273,879 rows; `max_date` = 2026-06-11; 635,218 addresses |
| `int_execution_account_token_movements_out_daily` | 20,432,106 rows; `max_date` = 2026-06-11; 497,527 addresses |
| `fct_execution_account_profile_latest FINAL` | 1,318,764 rows = 1,318,764 unique addresses (grain correct); 100 % have `token_transfer_count` NULL/zero |
| `fct_execution_account_token_balances_latest` | 384,132 rows; 300,575 distinct addresses; 0 zero-USD rows (unpriced tokens correctly filtered) |
| `fct_execution_account_profile_latest` balance distribution | 300,575 addresses with positive balance; 1,018,189 (77 %) zero or NULL — plausible for a chain with many dormant and contract addresses |
| `fct_execution_account_counterparty_edges_latest` | 2,339,497 edges: `safe_relation` 1,533,773; `circles_trust` 629,529; `gpay_activity` 173,567; `validator_relation` 2,628; `token_transfer` **0** (propagation of empty fct) |
| `fct_execution_network_retention_monthly FINAL` | 93 cohorts; earliest 2018-10-01; latest 2026-05-01 (10 days stale); max retention rate 0.353; no rate > 1.0 |
| `fct_execution_address_resolver` | 1,099,448 rows; 1,093,444 distinct addresses — partial merge confirmed; multi-source rows coexist |

---

## Pros / Cons

**Pros**

- Well-structured per-address Portfolio backend with correct grain keys (one row per address on profile, resolver, and balances) and proper `FINAL` on point-lookup tables.
- Identity resolution is rich and verifiable: Safe, Circles, GPay, Gnosis App, and validator-withdrawal derivation (inline from 0x01/0x02 credentials; BLS excluded) with a clear, documented `display_name` priority order.
- Division-by-zero guarded via `nullIf`; timezone-safe date handling throughout; unpriced tokens correctly filtered with `ifNull(balance_usd, 0)`.
- Counterparty graph spans five economically meaningful edge types with bidirectional structural edges (Safe ownership, Circles trust).
- Token movements correctly scoped to whitelisted tokens; zero-address and null-counterparty rows excluded.
- Balance history is deep and fresh: 296 M rows back to 2020-08, 422 k addresses, `max_date` = today, zero recent duplicate pairs.
- Network retention rates are economically plausible (max 0.353, no impossible >1.0 values); cohort definition uses pseudonymized `cityHash64` identities — privacy-conscious by construction.

**Cons**

- Movements fact table is empty in production: portfolio transaction history and the token-transfer half of the counterparty graph are currently 100 % blank for every consumer.
- Entire unit has zero semantic-layer coverage: MCP routes return `semantic_coverage_gap`; balance, retention, and movement numbers are unqueryable via `query_metrics`.
- Retention's 181-day source window means 90d/180d rates for cohorts older than ~6 months cannot be recomputed on rebuild — historical cohort values are effectively frozen and non-reproducible.
- Network retention fact is stranded: no `api_` view, not in the semantic layer, undiscoverable except by direct SQL.
- `AggregatingMergeTree` on the resolver stores plain ints, not aggregate states — documented merge semantics are wrong; correctness depends entirely on the `api_` view's `GROUP BY`.
- `address_type` is binary (Safe vs everything else); the unit's advertised "EOA/contract breakdown" is not delivered.
- Two `api_` views over `ReplacingMergeTree` read without `FINAL`, risking transient duplicate `(address, date)` rows at serve time.
- Multiple fragile silent-empty failure modes: 14-day `max(date)` lookback on balances; 1-day self-reference lookback on retention — both rebuild empty on extended upstream gaps with no error or alert.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 — urgent | Rerun the incremental for `fct_execution_account_token_movements_daily` and rebuild its downstream chain (`transaction_summary_latest`, `account_profile_latest`, `recent_transactions`, `counterparty_edges`). Production portfolio transaction history is currently 100 % blank. | `fct_execution_account_token_movements_daily`, `fct_execution_account_transaction_summary_latest`, `fct_execution_account_profile_latest`, `api_execution_account_recent_transactions` |
| P0 — urgent | Add a non-empty / row-count post-hook assertion (or dbt test) on the movements fct so an empty rebuild fails loudly instead of silently zeroing 1.3 M profiles. | `fct_execution_account_token_movements_daily` |
| P1 | Fix resolver engine/semantics mismatch: switch `fct_execution_address_resolver` to `ReplacingMergeTree` (matches actual last-write-wins behaviour) or encode real `AggregateFunction` columns; correct the misleading merge-semantics comment. | `fct_execution_address_resolver` |
| P1 | Add `FINAL` (or a deduplicating `GROUP BY`) to `api_execution_account_balance_history_daily` and `api_execution_account_token_movements_daily` to prevent serving pre-merge duplicate `(address, date)` rows. | `api_execution_account_balance_history_daily`, `api_execution_account_token_movements_daily` |
| P1 | Resolve the retention 181-day reproducibility gap: extend `int_execution_transactions_daily_active_addresses` or build a cumulative per-cohort active-month table so 90d/180d rates for older cohorts are recomputable; document that older-cohort rates are window-limited. | `fct_execution_network_retention_monthly` |
| P2 | Widen the 14-day `max(date)` lookback in `fct_execution_account_token_balances_latest` to 30 days and add a freshness/non-empty guard; add a NULL cold-start guard and a wider late-arrival lookback in `fct_execution_network_retention_monthly`. | `fct_execution_account_token_balances_latest`, `fct_execution_network_retention_monthly` |
| P2 | Decide the network-retention fact's fate: add `api_execution_network_retention_monthly` (standard `as_of_date` pattern) plus `granularity:monthly` tag, or document it as an internal-only fact and add a note to the schema YML. | `fct_execution_network_retention_monthly` |
| P2 | Stand up minimal semantic-layer coverage for the unit (balance, `first_seen_date`, retention rate, active counts) so MCP preflight stops returning `semantic_coverage_gap` and Portfolio metrics are governed and cross-queryable with OnChain Activity. | `semantic/authoring/execution/accounts/` (new) |
| P3 | Remove the dead `incr_end` capture in `int_execution_account_token_movements_in_daily` and reconcile `schema.yml` microbatch meta with the actual `insert_overwrite` strategy across both in/out siblings. | `int_execution_account_token_movements_in_daily`, `int_execution_account_token_movements_out_daily` |
| P3 | Verify native xDAI inclusion against `seeds/tokens_whitelist.csv` and either include it in balances/movements or document the omission; correct the unit description so `address_type`'s Safe-vs-other scope is not advertised as an EOA/contract breakdown. | `fct_execution_account_token_balances_latest`, docs |
| P3 | Add `join_use_nulls` pre-hook to `fct_execution_gnosis_app_user_profile_latest` per house convention for LEFT JOINs. | `fct_execution_gnosis_app_user_profile_latest` |

---

## Open disagreements

None. The review converged in one round with no outstanding challenges between inspector and context agents.

---

## Review log

| Round | Agent | Challenge / resolution |
|---|---|---|
| 1 | Inspector | Identified critical empty-fct finding via direct warehouse query; confirmed propagation to 100 % of profile rows. |
| 1 | Context | Confirmed unit is Portfolio backend, not analytics dashboard; surfaced semantic-layer gap and retention 181-day window as structural limitations. |
| 1 | Final verdict | Independently re-confirmed empty-movements finding; surfaced 181-day reproducibility risk as an additional under-weighted point; declared convergence. No challenges issued. |
