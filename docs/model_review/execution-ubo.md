# Model review: execution/ubo

**Convergence:** converged in 1 round — inspector and context reports were mutually consistent with no contradictions; all load-bearing claims verified against warehouse queries and source files.

---

## Scope and inventory

The `execution/ubo` sector unit is a multi-protocol "Ultimate Beneficial Owner" attribution pipeline. It traces pooled token balances held by DeFi contract containers (Aave aTokens, Balancer V2 Vault, Uniswap V3 pools, Swapr V3 pools, Curve 3pool, and the sDAI ERC-4626 vault) down to individual end-wallet withdrawable claims. A second-level resolution pass then unrolls container-of-container positions (e.g. aGnosDAI = Aave lenders whose collateral is sDAI).

| Layer | Count | Key models |
|---|---|---|
| Intermediate — protocol claims | 6 | `int_ubo_claims_{aave,balancer_v2,uniswap_v3,swapr_v3,curve,sdai}_daily` |
| Intermediate — resolution | 2 | `int_ubo_second_level_daily`, `int_ubo_known_containers_daily` (staging) |
| Mart — fact | 2 | `fct_ubo_supply_claims_daily`, `fct_ubo_supply_claims_resolved_daily` |
| Mart — reference | 2 | `fct_ubo_known_containers_daily`, `fct_ubo_address_classification` |
| Downstream API views (tokens/) | 3 | `api_execution_tokens_top_holders_latest`, `api_execution_tokens_ubo_coverage_latest`, `api_execution_tokens_ubo_venue_breakdown_latest` |

Analyst rounds: 1. All 13 SQL files and both `schema.yml` files were fully read. Eight warehouse queries were executed covering row counts, uniqueness, second-level container identity, protocol distribution, and EURe date-range handling.

---

## Business context

**Intended question:** For each whitelisted token, which real-world wallets hold it — including supply locked inside DeFi protocol contracts — and how much?

**Canonical definitions:**

- **UBO (Ultimate Beneficial Owner):** any address that is not itself a pooled-position container. Operationally: wallets that can directly withdraw a token balance.
- **Supply claim:** one row per `(date, protocol, container_address, ubo_address, token_address)` with `balance_raw`, `balance`, and `balance_usd`. Source: `fct_ubo_supply_claims_daily`.
- **Container contract:** a protocol contract that custodies tokens on behalf of multiple depositors. Tracked in `fct_ubo_known_containers_daily`.
- **is_terminal_ubo = 1:** label-confirmed end-holder (sectors: EOAs, Wallets & AA, Bridges, Payments).
- **is_terminal_ubo = 0:** labeled but non-decomposed container (sectors: Lending & Yield, DEX) — Phase 2+ decomposition target.
- **Second-level container:** a `ubo_address` in `fct_ubo_supply_claims_daily` that is itself a known container for the bridge token (canonical case: aGnosDAI depositing sDAI shares). Resolved by `fct_ubo_supply_claims_resolved_daily` via proportional redistribution.
- **pct_direct_terminal / pct_unwound_terminal / pct_known_container / pct_unclassified:** coverage KPI quadrant in `fct_execution_tokens_ubo_coverage_latest` that makes the pipeline's own completeness measurable.
- **Token canonicalization:** `token_address` is resolved via `tokens_whitelist` date-range joins, collapsing migrated tokens (EURe V1 `0xcb444e90` → V2 `0x420ca0f9` at 2024-08-25) into a single series.
- **Balancer V2 LP attribution:** daily cumulative sum of `PoolBalanceChanged` event deltas, keyed by `(ubo_address, symbol)` — all pools collapse under the single Vault container address.
- **Uniswap V3 / Swapr V3 Track A (~45%):** `Mint`/`Burn` event `owner` field is the real LP.
- **Uniswap V3 / Swapr V3 Track B (~55%):** real LP resolved via ERC-721 NFT ownership chain from `NonfungiblePositionManager` Transfer events (`argMax` by block timestamp = current owner).
- **sDAI claim:** `(sDAI_balance / total_sDAI_supply) * vault_WxDAI_reserve` per holder per day.
- **Curve 3pool claim:** proportional reserve claim via effective LP shares, handling both direct x3CRV holders and gauge depositors.

**Key contract addresses:**

- Balancer V2 Vault: `0xBA12222222228d8Ba445958a75a0704d566BF2C8`
- Uniswap V3 NPM: `0xae8fbe656a77519a7490054274910129c9244fa3`
- Swapr V3 NPM: `0x91fd594c46d8b01e62dbdebed2401dde01817834`
- Curve 3pool (AMM): `0x7f90122bf0700f9e7e1f688fe926940e8839f353`; LP token: `0x1337BedC9D22ecbe766dF105c9623922A27963EC`; gauge: `0xb721cc32160ab0da2614cc6ab16ed822aeebc101`
- sDAI vault (SavingsXDai): `0xaf204776c7245bf4147c2612bf6e5972ee483701`
- aGnosDAI (Aave aToken for sDAI): `0x7a5c3860a77a8DC1b225BD46d0fb2ac1C6D191BC`

---

## Implementation assessment

### HIGH — Resolved mart container-strip relies on Nullable column type, not join_use_nulls

`fct_ubo_supply_claims_resolved_daily` uses `LEFT JOIN known_containers kc ... WHERE kc.container_address IS NULL` with no `join_use_nulls = 1` pre_hook. The anti-join succeeds today only because `container_address` is `Nullable(String)` (via `allow_nullable_key = 1`). If that column type is changed to non-Nullable `String`, the `IS NULL` predicate silently matches nothing: second-level container rows are not stripped, and second-level UBOs are double-counted into supply totals without any error or test failure.

This directly violates the project convention documented in `feedback_clickhouse_left_join_nulls`. Fix: add an explicit `join_use_nulls = 1` pre_hook and matching post_hook to `models/execution/ubo/marts/fct_ubo_supply_claims_resolved_daily.sql`.

### HIGH — Cumsum prev_balances reads `{{ this }}` without FINAL in the regular incremental path

Confirmed in all three cumsum models (`int_ubo_claims_balancer_v2_daily`, `int_ubo_claims_uniswap_v3_daily`, `int_ubo_claims_swapr_v3_daily`): the batch path (when `start_month`/`end_month` vars are set) reads `FROM {{ this }} FINAL`, but the regular non-batch incremental path reads `FROM {{ this }}` without `FINAL`. On a `ReplacingMergeTree` with `delete+insert` strategy, unmerged duplicate rows at the boundary date can inflate the cumulative starting balance, corrupting all forward cumsum values without any immediate error signal.

Fix: apply `FINAL` in the regular incremental `prev_balances` CTE to match the batch path, or add a clear comment justifying why the asymmetry is safe.

Affected: `models/execution/ubo/intermediate/int_ubo_claims_balancer_v2_daily.sql`, `int_ubo_claims_uniswap_v3_daily.sql`, `int_ubo_claims_swapr_v3_daily.sql`.

### MEDIUM — Resolved mart has no unique_combination_of_columns test

`fct_ubo_supply_claims_resolved_daily` performs a `UNION ALL` + proportional redistribution + `GROUP BY` at grain `(date, protocol, container_address, ubo_address, token_address)`. The upstream fact `fct_ubo_supply_claims_daily` carries this uniqueness test (confirmed in `schema.yml` line 110), but the resolved descendant does not (its entry begins at line 128 with no such test). A redistribution bug that duplicates sub-holder rows would inflate supply figures undetected.

Fix: add `dbt_utils.unique_combination_of_columns` to the resolved mart entry in `models/execution/ubo/marts/schema.yml`.

### MEDIUM — Five intermediate claims models lack uniqueness tests

Only `int_ubo_claims_aave_daily` and `int_ubo_second_level_daily` carry `unique_combination_of_columns` tests. The five untested models are:

- `int_ubo_claims_balancer_v2_daily`
- `int_ubo_claims_uniswap_v3_daily`
- `int_ubo_claims_swapr_v3_daily`
- `int_ubo_claims_curve_daily`
- `int_ubo_claims_sdai_daily`

Cumsum or NPM-ownership join bugs producing duplicate `(date, ubo_address, token_address)` rows would silently propagate into the mart.

Fix: add tests at each model's declared unique_key grain in `models/execution/ubo/intermediate/schema.yml`.

### MEDIUM — Backfill append mode is non-idempotent for cumsum models and the resolved mart

When `start_month`/`end_month` (or `incremental_end_date`) vars are set, `incremental_strategy` switches to `'append'` (confirmed in Balancer/Uniswap line 4). Re-running the same backfill range appends duplicate partitions; deduplication requires `OPTIMIZE TABLE FINAL` after the fact. No operator runbook is referenced in-model, despite `feedback_refresh_state` project memory warning explicitly about this class of issue.

Fix: document a pre-backfill partition-cleanup step in a comment or linked runbook, or switch to dedupe-on-write.

Affected: `int_ubo_claims_balancer_v2_daily.sql`, `int_ubo_claims_uniswap_v3_daily.sql`, `int_ubo_claims_swapr_v3_daily.sql`, `fct_ubo_supply_claims_resolved_daily.sql`.

### LOW — fct_ubo_known_containers_daily uses ReplacingMergeTree but is materialized='table'

Confirmed: `materialized='table'` + `engine='ReplacingMergeTree()'`. A full `CREATE TABLE AS SELECT` each run never exercises the replacing-merge dedup semantic, making the engine choice misleading. Plain `MergeTree()` removes the ambiguity.

Affected: `models/execution/ubo/marts/fct_ubo_known_containers_daily.sql`.

### LOW — Swapr DecreaseLiquidity without prior IncreaseLiquidity silently dropped

Swapr V3 `DecreaseLiquidity` events carry no pool address field; pool is inferred from the `IncreaseLiquidity` tokenId-to-pool map. A tokenId that was only ever decreased (no `IncreaseLiquidity` in scanned history) has no pool mapping and that decrease is dropped from aggregation, understating the position's withdrawals. The share of affected tokenIds near history boundaries should be quantified.

Affected: `models/execution/ubo/intermediate/int_ubo_claims_swapr_v3_daily.sql`.

---

## Business-logic assessment

### HIGH — Only one level of container nesting is resolved; deeper nesting understates terminal coverage

`fct_ubo_supply_claims_resolved_daily` performs exactly one unroll pass. The canonical aGnosDAI case — Aave lenders who deposited sDAI — is attributed to the aToken container address rather than individual lenders. As a result, `pct_known_container` is overstated and `pct_unwound_terminal` understated in sDAI/EURe-adjacent reporting. This is an accepted Phase-2 design limit, but the one-pass ceiling is not surfaced in the coverage view description.

Fix: add a caveat to the `fct_execution_tokens_ubo_coverage_latest` schema description and any downstream reporting that reads the coverage KPI.

Affected: `models/execution/ubo/marts/fct_ubo_supply_claims_resolved_daily.sql`, `int_ubo_second_level_daily.sql`, `models/execution/tokens/marts/fct_execution_tokens_ubo_coverage_latest.sql`.

### MEDIUM — NPM Track B attributes all historical deltas to the CURRENT NFT owner (lookahead bias)

Uniswap V3 and Swapr V3 Track B resolve the LP via `argMax` NFT owner, attributing pre-transfer liquidity deltas to the post-transfer owner. This is justified in code comments as "standard simplification since position transfers are rare on Gnosis Chain," but the bias is not present in `schema.yml` `ubo_address` descriptions for those models, and the rare-transfer share is unquantified. For external or quarterly holder-identity reporting this is a stated-but-unbounded measurement bias.

Fix: document the attribution simplification in `schema.yml` for both models and quantify the share of NPM positions ever transferred on Gnosis Chain to bound the error.

Affected: `models/execution/ubo/intermediate/int_ubo_claims_uniswap_v3_daily.sql`, `int_ubo_claims_swapr_v3_daily.sql`, `schema.yml`.

### MEDIUM — Balancer collapses all pools under one Vault container, losing per-pool venue granularity

`int_ubo_claims_balancer_v2_daily` groups by `(date, ubo_address, symbol)` and hardcodes a single Vault `container_address`. The venue-breakdown view can distinguish "Balancer V2" from other protocols but cannot break down individual Balancer pools, unlike Uniswap/Swapr which retain `pool_address`. Consumers who interpret venue breakdown at pool granularity for Balancer will not get it, and this difference is not documented in the venue-breakdown view.

Fix: document the Balancer-only single-Vault grain difference in the venue view schema.

Affected: `models/execution/ubo/intermediate/int_ubo_claims_balancer_v2_daily.sql`, `models/execution/tokens/marts/fct_execution_tokens_ubo_venue_breakdown_latest.sql`.

### MEDIUM — tier1 venue_breakdown has zero schema.yml entry; all three tier1 UBO endpoints absent from public API docs

Confirmed by grep: `fct_execution_tokens_ubo_venue_breakdown_latest` and `api_execution_tokens_ubo_venue_breakdown_latest` carry `tier1` + `production` tags but have no entry in `models/execution/tokens/marts/schema.yml`. All three tier1 UBO endpoints (`top_holders`, `ubo_coverage`, `venue_breakdown`) are absent from the `cerebro-docs` public API index. For externally-served surfaces this is a governance gap: no documented column contracts and unindexed public endpoints.

Fix: add `schema.yml` entries for the venue_breakdown models and add all three UBO endpoints to the docs-site index, or downgrade the tier to `internal` if they are not publicly served.

Affected: `models/execution/tokens/marts/fct_execution_tokens_ubo_venue_breakdown_latest.sql`, `api_execution_tokens_ubo_venue_breakdown_latest.sql`, `schema.yml`.

### MEDIUM — venue_breakdown and coverage views recently failed to build (Code 60); production presence unverified

Per context report: `fct_execution_tokens_ubo_venue_breakdown_latest` and `fct_execution_tokens_ubo_coverage_latest` were never successfully built (Code 60) until a bootstrap in June 2026. The inspector did not run a live presence/freshness check on these specific views. Any external or quarterly report citing these views should first confirm they exist, are populated, and are fresh in the warehouse.

Affected: `models/execution/tokens/marts/fct_execution_tokens_ubo_coverage_latest.sql`, `fct_execution_tokens_ubo_venue_breakdown_latest.sql`.

### LOW — Unlisted tokens bypass UBO decomposition entirely

The pipeline is scoped to `tokens_whitelist` symbols only. For any unlisted token, container contracts appear as raw top-holders without decomposition. This is correct by design but should be stated as an explicit scoping caveat in consumer-facing documentation.

Affected: `models/execution/ubo/marts/fct_ubo_supply_claims_daily.sql`.

### LOW — Hardcoded Curve 3pool and Balancer Vault addresses not cross-referenced to a seed registry

The Balancer V2 Vault (`0xBA12...F2C8`) and Curve 3pool (`0x7f90...f353`) appear only in model SQL and schema descriptions. The Curve LP token and gauge addresses ARE in seeds, but the Curve AMM pool contract is not. For an audited reporting surface a seed cross-check would harden against silent wrong-deployment errors.

Affected: `models/execution/ubo/intermediate/int_ubo_claims_curve_daily.sql`, `int_ubo_claims_balancer_v2_daily.sql`.

---

## Data findings

All queries run against the warehouse with `FINAL` to force deduplication.

| Metric | Value |
|---|---|
| `fct_ubo_supply_claims_daily` row count | 52,705,784 |
| `fct_ubo_supply_claims_resolved_daily` row count | 53,459,421 |
| Net redistribution delta (resolved minus claims) | +753,637 |
| Second-level containers leaking into resolved | 0 (verified by anti-join) |
| `int_ubo_second_level_daily` rows | 7,664 |
| Distinct second-level `ubo_address` values | 11 |
| Protocols present in second-level | sDAI only (Balancer, Aave: 0 rows) |
| Max date, incremental protocols (Aave/Balancer/Uni/Swapr) | 2026-06-09 |
| Max date, table-materialized protocols (sDAI, Curve) | 2026-06-07 |
| Upstream staleness gap | 2 days (inherited from `int_execution_tokens_balances_daily`) |

The sDAI EURe migration is handled correctly via `tokens_whitelist` date-range joins: Swapr tracks old EURe (`0xcb444e`) through 2024-08-24 (406 rows) and new EURe from 2024-08-25 (1,350 rows) with no data gap.

The 2-day staleness gap for sDAI/Curve relative to incremental models is an upstream issue in `int_execution_tokens_balances_daily`, not a UBO code bug. It does leave `fct_ubo_supply_claims_resolved_daily` missing sDAI and Curve 3pool claim rows for 2026-06-08 and 2026-06-09.

---

## Pros / Cons

**Strengths:**

- Conceptually sound and ambitious product: traces pooled DeFi supply across five protocols down to end-wallet UBOs, answering a real "who holds this token?" question.
- Second-level container resolution verified correct in-warehouse: 53.4M resolved rows, zero second-level containers leaking through, no double-counting detected.
- Coverage diagnostic (pct_direct_terminal / pct_unwound_terminal / pct_known_container / pct_unclassified) is a self-describing data-quality KPI that makes the pipeline's own completeness measurable.
- Token canonicalization via `tokens_whitelist` date ranges correctly collapses migrated tokens; warehouse confirmed clean Swapr EURe handling.
- Container addresses are individually documented in context with seed cross-references where available.
- Mart grain on `fct_ubo_supply_claims_daily` is well-defined and carries a `unique_combination_of_columns` test.
- OOM remediation already applied (memory pre/post hooks, external spill) following the prior 16-month staleness incident.

**Weaknesses:**

- Five intermediate cumsum models and the critical resolved mart lack uniqueness tests; grain/duplicate regressions propagate to API consumers silently.
- Regular incremental `prev_balances` reads `{{ this }}` without `FINAL` while the batch path uses `FINAL`; unmerged boundary duplicates can corrupt cumulative balances.
- Resolved mart container-strip depends on a `Nullable(String)` type side-effect rather than an explicit `join_use_nulls` pre_hook, violating project convention.
- Only one level of container unrolling: nested positions (aGnosDAI = Aave lenders holding sDAI) are attributed to the aToken address, understating true terminal coverage.
- Balancer collapses all pools under a single Vault container, losing per-pool venue granularity.
- NPM Track B attributes all historical liquidity deltas to the current NFT owner — a measurement bias undocumented in schema for the affected models.
- tier1 production `api_execution_tokens_ubo_venue_breakdown` has zero `schema.yml` entry; all three tier1 UBO endpoints are absent from the public docs index.
- sDAI and Curve are 4 days stale vs. 2 days for incremental protocols (upstream-inherited); full-rebuild table materialization for those two protocols is unprofiled cost-wise.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 | Add explicit `join_use_nulls = 1` pre_hook/post_hook to `fct_ubo_supply_claims_resolved_daily` so the container-strip anti-join no longer depends on Nullable column type | `fct_ubo_supply_claims_resolved_daily.sql` |
| P0 | Make the regular incremental `prev_balances` CTE read `FROM {{ this }} FINAL` in all three cumsum models (or justify asymmetry with the batch path in a comment) | `int_ubo_claims_balancer_v2_daily.sql`, `int_ubo_claims_uniswap_v3_daily.sql`, `int_ubo_claims_swapr_v3_daily.sql` |
| P1 | Add `dbt_utils.unique_combination_of_columns` to `fct_ubo_supply_claims_resolved_daily` at its redistribution output grain | `schema.yml` (marts) |
| P1 | Add uniqueness tests to the five untested intermediate models (Balancer, Uniswap V3, Swapr V3, Curve, sDAI) at their declared unique_key grains | `schema.yml` (intermediate) |
| P1 | Run a live warehouse check confirming `venue_breakdown` and `coverage` views exist, are populated, and are fresh before any external or quarterly use | `fct_execution_tokens_ubo_coverage_latest.sql`, `fct_execution_tokens_ubo_venue_breakdown_latest.sql` |
| P1 | Add a `schema.yml` entry for `fct/api_execution_tokens_ubo_venue_breakdown_latest` and surface all three tier1 UBO endpoints in the public API docs index (or downgrade tier to `internal`) | `schema.yml` (tokens/marts) |
| P2 | Document the one-pass nesting ceiling (aGnosDAI attributed to aToken, not lenders) in the coverage view description so `pct_known_container` is not misread as fully unresolvable | `fct_execution_tokens_ubo_coverage_latest.sql`, `schema.yml` |
| P2 | Document the NPM current-owner attribution bias in `schema.yml` for Uniswap V3 and Swapr V3 models; quantify the share of NPM positions ever transferred on Gnosis Chain to bound the error | `int_ubo_claims_uniswap_v3_daily.sql`, `int_ubo_claims_swapr_v3_daily.sql`, `schema.yml` |
| P2 | Add a backfill runbook step (pre-clean target partitions) for the append-mode cumsum/resolved backfills and reference it in model configs | `int_ubo_claims_balancer_v2_daily.sql`, `int_ubo_claims_uniswap_v3_daily.sql`, `int_ubo_claims_swapr_v3_daily.sql`, `fct_ubo_supply_claims_resolved_daily.sql` |
| P2 | Document the Balancer-only single-Vault grain (no per-pool granularity) in the venue-breakdown view schema | `fct_execution_tokens_ubo_venue_breakdown_latest.sql`, `schema.yml` |
| P3 | Switch `fct_ubo_known_containers_daily` from `ReplacingMergeTree()` to plain `MergeTree()` since it is `materialized='table'` and the dedup semantic is never exercised | `fct_ubo_known_containers_daily.sql` |
| P3 | Quantify Swapr tokenIds that only have `DecreaseLiquidity` (no prior `IncreaseLiquidity`) to bound the silent-drop exposure near history boundaries | `int_ubo_claims_swapr_v3_daily.sql` |
| P3 | Add the Curve 3pool AMM address (`0x7f90...f353`) and Balancer Vault address (`0xBA12...F2C8`) to a seed registry to harden against silent wrong-deployment errors | `int_ubo_claims_curve_daily.sql`, `int_ubo_claims_balancer_v2_daily.sql` |

---

## Open disagreements

None. The review converged in one round with no contradictions between inspector and context reports.

---

## Review log

| Round | Agent | Challenge issued | Resolution |
|---|---|---|---|
| 1 | Inspector | Verified LEFT ANTI JOIN via Nullable type, FINAL inconsistency in cumsum prev_balances, missing uniqueness tests, append-mode non-idempotency, ReplacingMergeTree on table-materialized model | All confirmed by direct file reads and 8 warehouse queries; no rebuttal required |
| 1 | Context | Identified one-pass nesting limit, NPM attribution bias, Balancer Vault grain collapse, venue_breakdown schema.yml gap, Code 60 build history, sDAI/Curve staleness as upstream-inherited | Consistent with inspector findings; no contradictions; convergence declared after round 1 |
