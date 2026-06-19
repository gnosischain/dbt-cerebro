# Model review: execution/tokens

**Convergence:** Converged in 1 round — both agents reached consistent findings with no unresolved disagreements; the final verdict confirmed all load-bearing claims against source.

---

## Scope and inventory

The `execution/tokens` sector is Gnosis Chain's authoritative ERC-20 token analytics pipeline. It covers the full chain from raw transfer events through per-address cumulative balances, daily supply and holder counts, balance cohort distributions, sector attribution, and UBO (Ultimate Beneficial Owner) unwinding for Phase 1 container protocols.

| Layer | Count | Examples |
|---|---|---|
| Intermediate | 7 | `int_execution_tokens_transfers_daily`, `int_execution_tokens_balances_native_daily`, `int_execution_tokens_balances_daily`, `int_execution_tokens_supply_holders_daily`, `int_execution_tokens_balance_cohorts_daily`, `int_execution_tokens_balances_by_sector_daily`, `int_execution_tokens_address_diffs_daily` |
| Fact | 6 | `fct_execution_tokens_metrics_daily`, `fct_execution_tokens_overview_by_class_latest`, `fct_execution_tokens_top_holders_ranked`, `fct_execution_tokens_top_holders_latest`, `fct_execution_tokens_ubo_coverage_latest`, `fct_execution_tokens_ubo_venue_breakdown_latest` |
| API mart | ~17 | `api_execution_tokens_supply_daily`, `api_execution_tokens_holders_daily`, `api_execution_tokens_balances_daily`, `api_execution_tokens_overview_latest`, `api_execution_tokens_top_holders_latest`, `api_execution_tokens_ubo_coverage_latest`, etc. |

Total: 30 SQL files reviewed in full. The pipeline covers 47 whitelisted tokens (seeds/tokens_whitelist.csv) across four token classes (STABLECOIN, OTHERS, RWA, and the synthetic xDAI sentinel), with history from 2020-07-01.

---

## Business context

**Intended purpose.** This unit answers: What is the on-chain circulating supply and holder count per token class, and how did those change in the last 7 days? How is supply distributed across holder sectors (DeFi, CEX, EOA, Unknown)? What are the daily transfer volumes and active sender counts? Who are the top-500 holders of each token and what share of supply has been traced to confirmed end-holders via UBO unwinding?

**Canonical definitions (as documented).**

- **supply:** Sum of positive balances across all addresses excluding `0x0000...0000`, in native token units. Source: `int_execution_tokens_supply_holders_daily`.
- **holders:** Count of distinct addresses with `balance > 0` excluding the zero address. Source: same model.
- **volume_token / volume_usd:** Sum of absolute transfer amounts normalized by decimals; USD = native * that day's price. Source: `int_execution_tokens_transfers_daily`.
- **active_senders:** Distinct `from_address` values in whitelisted token transfers on a date; computed via `groupBitmapState` for cross-token bitmaps.
- **balance_bucket (cohort):** 10-tier log-scale bucket (USD: $0–0.01 through $1M+; same tiers for native units). Source: `int_execution_tokens_balance_cohorts_daily`.
- **token_class:** One of STABLECOIN, OTHERS, RWA from `seeds/tokens_whitelist.csv`. Aave aTokens and Spark spTokens are whitelisted as OTHERS but excluded from default incremental runs via `symbol_exclude` in `dbt_project.yml`.
- **is_terminal_ubo:** 1 = EOAs, Wallets & AA, Bridges, Payments; 0 = Lending & Yield, DEX (labeled but undecomposed containers); NULL = no label.

**Contract context highlights.** Standard ERC-20 `Transfer(address,address,uint256)` events (topic0 `0xddf252ad...`) from `execution.logs`; WxDAI (`0xe91d153e...`) is decoded separately via `contracts_wxdai_events` with Deposit/Withdrawal mapped to mint-from/burn-to zero address. Native xDAI uses the synthetic sentinel address `0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee`. UBO Phase 1 containers unwound: Aave V3, SparkLend, Balancer V2, Uniswap V3, Swapr V3, Curve, sDAI vault.

---

## Implementation assessment

### HIGH — supply computed without positive-balance guard; negative circulating supply reaches production

`int_execution_tokens_supply_holders_daily` computes supply as `sumIf(b.balance, address != zero)` with **no** `balance > 0` condition. This directly contradicts the model's own schema description ("total circulating supply") and the canonical definition above, which both state sum of *positive* balances. The holders count on the same model does filter `balance > 0`, making the inconsistency intra-model.

Negative running-sum balances arise where recorded outflows exceed recorded inflows — likely from addresses whose earliest inflows predate the pipeline's ingestion window (bridging events, missing historical transfer range).

Confirmed in warehouse: wstETH on 2026-06-10 shows `supply_incl_neg = 3811` vs `supply_pos_only = 4195` — a ~9% undercount (~384 wstETH, ~$1.3M at current price). Three negative-supply rows exist in production for wstETH on 2026-05-18 through 2026-05-20 (values: -133, -133, -70 wstETH).

Affected models: `models/execution/tokens/intermediate/int_execution_tokens_supply_holders_daily.sql`, `models/execution/tokens/intermediate/int_execution_tokens_balances_daily.sql`

### HIGH — negative supply_usd propagates to API with no test guard

`fct_execution_tokens_metrics_daily` passes `supply` straight through and computes `supply_usd = supply * coalesce(price, 0)`. Negative supply therefore yields negative `supply_usd` served via `api_execution_tokens_supply_daily` to external consumers. No `not_negative` or `min_value=0` test exists on `supply` or `supply_usd` in `models/execution/tokens/marts/schema.yml`. Elementary anomaly tests on the API views are warn-only; nothing blocks a negative value from shipping.

Affected models: `models/execution/tokens/intermediate/int_execution_tokens_supply_holders_daily.sql`, `models/execution/tokens/marts/fct_execution_tokens_metrics_daily.sql`

### MEDIUM — symbol_filter applied twice in transfers and balances base CTEs

In `int_execution_tokens_transfers_daily` and `int_execution_tokens_address_diffs_daily`, the include/exclude filter is (a) passed as `filters_sql` into the `apply_monthly_incremental_filter` macro AND (b) re-applied explicitly in the same WHERE block after the macro call. Under normal incremental runs this produces a redundant `AND` clause, which is harmless. Under a non-incremental (full refresh) run the macro emits nothing, leaving the explicit copy as the only guard — intentional per the inline comment, but fragile: a future change at either site can silently drop or duplicate the filter with no observable build failure.

Affected models: `models/execution/tokens/intermediate/int_execution_tokens_transfers_daily.sql`, `models/execution/tokens/intermediate/int_execution_tokens_balances_daily.sql`, `models/execution/tokens/intermediate/int_execution_tokens_balances_native_daily.sql`

### MEDIUM — semantic model `int_execution_tokens_balances_daily` schema-mismatched against the SQL

`semantic/authoring/execution/tokens/semantic_models.yml` registers a semantic model for `int_execution_tokens_balances_daily` exposing dimensions `from_value_binary`, `from_value_string`, `to_value_binary`, `to_value_string`, `chain_id`, `block_timestamp` and measures `net_delta_raw_value`, `from_value_f64_value`, `insert_version_value`. None of these columns exist in the actual SQL, whose output is `(date, token_address, symbol, token_class, address, balance_raw, balance, balance_usd)`. The semantic YAML appears to reference the raw ClickHouse source `execution.balance_diffs` or a prior model version. This will cause MCP query failures when the semantic layer is invoked against this model. Additionally, two duplicate semantic pairs exist: one pointing at `api_execution_tokens_supply_by_sector_latest` and one at `fct_execution_tokens_supply_by_sector_latest`, both with the same `question_synonyms`; same duplication for `supply_distribution`. All metrics are `quality_tier: candidate` with no promoted production metrics despite serving live dashboard and MCP consumers.

Affected models: `semantic/authoring/execution/tokens/semantic_models.yml`

### MEDIUM — `balances_native_daily` uses delete+insert without explicit `join_use_nulls` hook

`int_execution_tokens_balances_native_daily` uses `incremental_strategy='delete+insert'` with a ReplacingMergeTree and LEFT JOINs on deltas and prev_balances. The window function uses `COALESCE(d.net_delta_raw, toInt256(0))`, which guards against NULLs, so there is no current data defect. However, project convention (per `feedback_clickhouse_left_join_nulls.md`) requires explicit `join_use_nulls` pre/post hooks when NULL is semantically meaningful. The absence is a latent risk when the query shape changes.

Affected models: `models/execution/tokens/intermediate/int_execution_tokens_balances_native_daily.sql`

### MEDIUM — INNER JOIN silently drops newly-debuted token classes from overview KPI card

`fct_execution_tokens_overview_by_class_latest` joins a 7d-ago snapshot via INNER JOIN on `(token_class, label)`. Any token class that debuted within the last 7 days (no anchor row) is silently omitted from the KPI card entirely. A LEFT JOIN with `COALESCE(t2.value, 0)` would surface the class with a 100% change figure instead of hiding it.

Affected models: `models/execution/tokens/marts/fct_execution_tokens_overview_by_class_latest.sql`

### LOW — spurious `AS` columns with empty `data_type` in intermediate schema.yml

`models/execution/tokens/intermediate/schema.yml` lists a column named `AS` with `data_type: ''` under both `int_execution_tokens_address_diffs_daily` and `int_execution_tokens_transfers_daily`. These are copy-paste artefacts from auto-generation. They mislead documentation and will trip CI typed-column checks if either model ever receives an `api:` tag.

Affected models: `models/execution/tokens/intermediate/schema.yml`

### LOW — holders filter methodology inconsistent across models

`fct_execution_tokens_overview_by_class_latest` filters holders on `balance_raw > 0`; `int_execution_tokens_supply_holders_daily` filters on `balance > 0`. These are equivalent for most tokens but diverge when `balance_raw` is positive yet rounds to zero after decimal normalization. Minor, but worth aligning to one rule.

Affected models: `models/execution/tokens/marts/fct_execution_tokens_overview_by_class_latest.sql`

### LOW — possible 7d-ago balance double-count in top_holders for addresses held both directly and via UBO

`fct_execution_tokens_top_holders_latest` sums direct and UBO `prev_7d` balances per `(token_address, address)`. An address that holds a token both directly and as a UBO unwound from a container protocol has its 7d-ago balance doubled in `change_usd_7d`. Edge-case but real for whale addresses that participate in DeFi while also holding directly.

Affected models: `models/execution/tokens/marts/fct_execution_tokens_top_holders_latest.sql`

### LOW — per-wallet balance-history API lacks an explicit privacy tier tag

`api_execution_tokens_balances_daily` serves individual wallet balances over time (up to 200 addresses per request). `allow_unfiltered: false` is set, but no `privacy:` tag or `expose_to_mcp:` policy decision is recorded. Should be reviewed against the project's address-level exposure conventions.

Affected models: `models/execution/tokens/marts/api_execution_tokens_balances_daily.sql`

---

## Business-logic assessment

### HIGH — definition drift: "circulating supply" includes negative balances

The supply column is defined in canonical documentation and the model's own schema as sum of *positive* balances excluding the zero address. The SQL sums all non-zero-address balances including negatives. This is not a rounding corner-case — it produces an observable negative number for wstETH and a ~9% understatement of its on-chain circulating supply on the current date. Any consumer of `supply` or `supply_usd` for OTHERS-class trend or quarterly reporting reads a number that does not match its own published definition.

**Decision point:** floor balances at zero in `int_execution_tokens_balances_native_daily` / `int_execution_tokens_balances_daily` (preferred, fixes the defect at source) vs `sumIf(balance, balance > 0 ...)` in `int_execution_tokens_supply_holders_daily` (fixes the supply aggregate but leaves negative balances in the address-level table). Either way a separate diagnostic metric tracking the negative-balance mass should be emitted so the anomaly is visible rather than buried.

Affected models: `models/execution/tokens/intermediate/int_execution_tokens_supply_holders_daily.sql`, `models/execution/tokens/intermediate/int_execution_tokens_balances_daily.sql`

### MEDIUM — `api:tokens_supply` and `api:holders_per_token` resource names claimed by two models each

`api_execution_tokens_supply_daily` and `api_execution_tokens_supply_latest_by_token` both carry `api:tokens_supply`; `api_execution_tokens_holders_daily` and `api_execution_tokens_holders_latest_by_token` both carry `api:holders_per_token`. The CI guard (`scripts/checks/check_api_tags.py`) forbids grain suffixes in the name but does not prevent two models from sharing a name. The MCP router cannot disambiguate between the daily series and the latest-snapshot variant without also reading the `granularity:` tag. Confirm the router keys jointly on `(api:, granularity:)` or rename the snapshot variants (e.g., `api:tokens_supply_snapshot`).

Affected models: `models/execution/tokens/marts/api_execution_tokens_supply_daily.sql`, `models/execution/tokens/marts/api_execution_tokens_supply_latest_by_token.sql`, `models/execution/tokens/marts/api_execution_tokens_holders_daily.sql`, `models/execution/tokens/marts/api_execution_tokens_holders_latest_by_token.sql`

### MEDIUM — supply = sum-of-balances, not `totalSupply()`; no reconciliation for vault/rebasing tokens

For sDAI (ERC-4626) and Aave/Spark wrapper tokens, sum-of-transfer-balances can diverge from the ERC-20 `totalSupply()` call due to shares-vs-assets accounting and rebasing. Tokens bridged out without an on-chain burn will inflate this supply figure. No reconciliation or tolerance check exists. This caveat must travel with any externally-published supply figure for these token types.

### MEDIUM — default `symbol_exclude` omits Aave/Spark wrapper balances from routine OTHERS supply

The 15 aToken and spToken symbols are excluded from default incremental runs via `dbt_project.yml`. OTHERS-class supply therefore does not include balances inside Aave/Spark supply-token contracts in routine daily refreshes. This is correct by design to avoid double-counting the underlying reserve, but it is a material scoping caveat that must accompany any OTHERS-class supply figure shown externally or used in executive reporting.

### LOW — `supply_distribution` and `supply_by_sector` semantic model duplicates create routing ambiguity

Two semantic models each exist for `supply_by_sector` and `supply_distribution`: one pointing at the `api_` view and one pointing at the `fct_` table, both with identical `question_synonyms`. MCP routing cannot deterministically resolve which to query.

---

## Data findings

Queries run during the review (8 total):

| Query | Result |
|---|---|
| `int_execution_tokens_balances_daily` scale and freshness | 383.7M rows; max_date = 2026-06-10 (yesterday); min_date = 2020-07-01; 29 distinct tokens |
| NULL `balance_usd` rate over last 7 days | 0% — 1,497,429 rows with `balance > 0`, zero NULLs; price join healthy for all 29 tokens |
| Duplicate grain check (last 3 days) | 0 duplicate `(date, token_address, address)` rows in `balances_daily FINAL`; 0 duplicate `(date, token_address)` rows in `transfers_daily FINAL` |
| Negative supply count in `supply_holders_daily` | 3 rows confirmed (all wstETH: 2026-05-18, 2026-05-19, 2026-05-20) |
| wstETH supply impact of negative balances | `supply_incl_neg` = 3,811 wstETH vs `supply_pos_only` = 4,195 wstETH on 2026-06-10 — ~9% undercount (~384 wstETH, ~$1.3M) |
| `metrics_daily` zero-price check | No zero-price anomalies for the latest date |
| `overview_by_class_latest` change_pct values | Values confirmed present; no newly-debuted class to trigger the INNER JOIN drop during this window |
| `fct_execution_tokens_metrics_daily` freshness | max_date = 2026-06-10; 25 rows (some tokens have no supply or transfers on some dates) |

Data freshness is healthy (all models at yesterday's date). Grain uniqueness is clean for recent data.

---

## Pros / Cons

**Pros**

- Sophisticated incremental architecture (`insert_overwrite`, `delete+insert`, microbatch append) explicitly tuned around ClickHouse limits (code 252 partition cap, CH bug 341 OOM) — each model documents its batch strategy.
- Clear canonical definitions for supply, holders, volume, cohorts, sectors, `token_class`, and a documented four-tier price-source hierarchy (Chainlink native → Backed Finance/aToken wrappers → Dune historical → $1 hard peg).
- Healthy data: max_date = yesterday, all 29 active tokens present, 0% NULL `balance_usd` over the last 7 days, clean grain in recent window.
- ReplacingMergeTree + `FINAL` + `dbt_utils` unique-combination tests deliver a clean `(date, token_address, address)` grain for recent data.
- Ambitious UBO unwinding layer (Phase 1: Aave V3, SparkLend, Balancer V2, Uniswap V3, Swapr V3, Curve, sDAI vault) resolving true end-holders behind protocol wrappers; `fct_execution_tokens_ubo_coverage_latest` quantifies remaining container share per token to guide Phase 2 prioritization.
- Per-wallet balance-history API blocks unbounded scans (`allow_unfiltered: false`, requires `symbol` or `address`).
- Long history from 2020-07-01 supports quarterly/trend reporting; WxDAI special-decode path (Deposit/Withdrawal as mint/burn) is correctly documented and consistently applied.
- Decimals, zero-address exclusion, and WxDAI mint/burn-from-zero accounting handled explicitly and consistently throughout the pipeline.

**Cons**

- Circulating supply can go negative: no `balance > 0` guard in the supply aggregate, contradicting the model's own documented definition; wstETH currently undercounted ~9% and 3 negative-supply rows are in production.
- Negative `supply_usd` reaches external API and dashboard consumers with no test blocking or even warning on it.
- `symbol_filter` applied twice in transfers and balances base CTEs — fragile pattern one refactor away from silently dropping or duplicating filters.
- Two spurious `AS` columns with empty `data_type` in intermediate `schema.yml` pollute documentation and will trip CI typing checks.
- `api:tokens_supply` and `api:holders_per_token` resource names each claimed by two models — ambiguous for the MCP router.
- Semantic layer is stale and misaligned: `int_execution_tokens_balances_daily` semantic model exposes columns that do not exist in the SQL; duplicate semantic pairs for `supply_by_sector` / `supply_distribution`; all metrics still `quality_tier: candidate` despite serving live consumers.
- INNER JOINs silently drop new token classes from the overview KPI card; potential 7d-ago balance double-count in `top_holders_latest` for addresses held both directly and via UBO.
- Default `symbol_exclude` omits Aave/Spark wrapper balances from routine OTHERS-class supply — correct by design but a material reporting caveat.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 | Fix negative supply at the definition boundary: change supply to `sumIf(balance, balance > 0 AND address != zero)` in `int_execution_tokens_supply_holders_daily` (or floor balances at zero in the balances layer); emit a separate `negative_balance_mass` diagnostic metric; backfill the affected wstETH dates | `int_execution_tokens_supply_holders_daily.sql`, `int_execution_tokens_balances_daily.sql` |
| P0 | Investigate why wstETH addresses carry negative `balance_raw` (missing pre-window inflows vs bridging vs rebasing); document the root cause before deciding between a floor fix or a supply-layer fix | `int_execution_tokens_balances_native_daily.sql` |
| P0 | Add `not_negative` / `min_value=0` tests on `supply` and `supply_usd` in both `int_execution_tokens_supply_holders_daily` and `fct_execution_tokens_metrics_daily`; promote at least one to `error` severity so negative supply cannot ship again | `marts/schema.yml`, `intermediate/schema.yml` |
| P1 | Reconcile the semantic registry: fix `int_execution_tokens_balances_daily` semantic model columns to match actual SQL output; resolve duplicate `supply_by_sector` / `supply_distribution` semantic pairs; review candidate-tier metrics before external reliance | `semantic/authoring/execution/tokens/semantic_models.yml` |
| P1 | Consolidate `symbol_filter` application in transfers and balances models to a single path, removing the double-filter fragility | `int_execution_tokens_transfers_daily.sql`, `int_execution_tokens_balances_daily.sql`, `int_execution_tokens_balances_native_daily.sql` |
| P2 | Resolve `api:tokens_supply` and `api:holders_per_token` resource-name reuse: confirm the MCP router keys jointly on `(api:, granularity:)` or rename the snapshot variants | `api_execution_tokens_supply_daily.sql`, `api_execution_tokens_supply_latest_by_token.sql`, `api_execution_tokens_holders_daily.sql`, `api_execution_tokens_holders_latest_by_token.sql` |
| P2 | Remove the two spurious `AS` columns (`data_type: ''`) from `intermediate/schema.yml` | `intermediate/schema.yml` |
| P2 | Switch INNER JOINs in `fct_execution_tokens_overview_by_class_latest` to LEFT JOIN + COALESCE so newly-debuted token classes appear with a 100% change figure rather than being silently dropped | `fct_execution_tokens_overview_by_class_latest.sql` |
| P3 | Add explicit `join_use_nulls` pre/post hooks to the `delete+insert` balances models per project convention, or document why default ClickHouse NULL behavior is safe | `int_execution_tokens_balances_native_daily.sql` |
| P3 | Add a periodic reconciliation check of balance-derived supply vs ERC-20 `totalSupply()` for vault/rebasing tokens (sDAI, aTokens); ensure the `symbol_exclude` OTHERS-supply caveat travels with externally-shown figures | `int_execution_tokens_supply_holders_daily.sql` |
| P3 | Align holders filter methodology: standardize on `balance > 0` (or `balance_raw > 0`) consistently across `overview_by_class_latest` and `supply_holders_daily` | `fct_execution_tokens_overview_by_class_latest.sql` |
| P3 | Review `api_execution_tokens_balances_daily` against the project's address-level exposure conventions; add a `privacy:` tag or `expose_to_mcp:` decision | `api_execution_tokens_balances_daily.sql` |

---

## Open disagreements

None. The review converged in one round with no unresolved disagreements between the inspector and context agents.

---

## Review log

| Round | Agent | Challenge issued | Resolution |
|---|---|---|---|
| 1 | Inspector | Negative-balance claim verified against warehouse (3 negative-supply rows, wstETH ~9% undercount) | Confirmed — no rebuttal needed |
| 1 | Inspector | Semantic model column mismatch flagged against actual SQL output | Confirmed by context agent as a registry/source confusion |
| 1 | Context | Canonical supply definition ("sum of positive balances") documented; inspector finding checks against it | Consistent — inspector's SQL reading matches the definition gap |
| 1 | Verdict | All material claims checked against source files and warehouse data; no outstanding challenges | Converged |
