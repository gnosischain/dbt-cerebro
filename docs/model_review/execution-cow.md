# Model review: execution/cow

**Convergence:** converged in 1 round — both inspector and context reports identified the same core defects independently, with no open code unknowns; remaining open items are team-answerable business validations.

---

## Scope and inventory

The `execution/cow` unit covers CoW Protocol (formerly CowSwap) trading activity on Gnosis Chain from genesis (April 2021) to the present. It decodes on-chain events from the `GPv2Settlement` contract and enriches them with off-chain surplus-based fee data from the CoW Protocol API.

| Layer | Count | Key models |
|---|---|---|
| Staging (views) | 4 | `stg_cow__trades`, `stg_cow__settlements`, `stg_cow__solvers`, `stg_cow__interactions` |
| Intermediate (incremental) | 2 | `int_execution_cow_trades`, `int_execution_cow_batches` |
| Fact marts | 4 | `fct_execution_cow_trades`, `fct_execution_cow_daily`, `fct_execution_cow_solvers`, `fct_execution_cow_solvers_daily` |
| API/KPI marts | 14 | `api_execution_cow_kpi_*` (6), `api_execution_cow_*_ts` (5), `api_execution_cow_batch_*` (2), `api_execution_cow_top_pairs_weekly` |
| Seeds | 2 | `cow_solvers.csv` (8 named solvers), `function_signatures.csv` |
| Semantic models | 2 | `execution_cow_daily`, `execution_cow_top_pairs_weekly` |
| Total SQL files | 27 | |

The unit answers five business questions: volume flowing through CoW on Gnosis Chain; protocol revenue (Sep 2024+); solver-ecosystem competitiveness; peer-to-peer CoW-ratio health; and downstream Gnosis App WAU swap-filled conversion signals via `int_execution_gnosis_app_swaps`.

---

## Business context

**What it measures.** Each row in the fact layer is one Trade event fill from `GPv2Settlement` (`0x9008D19f58AAbD9eD0D60971565AA8510560ab41`), identified by `(block_timestamp, transaction_hash, log_index)` / `order_uid`. A batch/settlement is a single on-chain transaction settling one or more trades; the winning solver is recorded from the co-emitted Settlement event. Active solvers are registered via `GPv2AllowListAuthentication` (`0x2c4c28DDBdAc9C5E7055b4C863b72eA0149D8aFE`).

**Critical fee-model discontinuity.** Before Sep 2024, the on-chain `feeAmount` in the Trade event was the user's signed-maximum fee ceiling under CIP-12's fee-subsidy model, not executed protocol revenue. Summing `fee_usd` with `fee_source='onchain'` massively overstates historical revenue. Only `fee_source='api'` (off-chain CoW Protocol API, Sep 2024+) correctly measures surplus-based protocol revenue. The daily mart correctly gates on `sumIf(fee_usd, fee_source='api')`; the schema.yml documents this clearly. A mixed transition window (mid-2023 to early-2024) has both source types coexisting.

**Key canonical definitions.**

- `is_cow` (Pure CoW): `num_trades > 1 AND num_interactions = 0` — no AMM calls needed, full peer-to-peer matching. Peaked ~34% Sep 2021; near zero since mid-2022.
- `cow_ratio`: `num_cow_batches / num_batches` per day; computed in `fct_execution_cow_daily`.
- `volume_usd`: `amount_bought * buy_token_price_usd`, fallback to `amount_sold * sell_token_price_usd` via ASOF daily price join on token symbol (not address).
- `fee_usd` (Sep 2024+): surplus-based revenue from `crawlers_data.cow_api_trade_fees` (three policy types: priceImprovement, surplus, volume).
- `solver_value_usd`: gross value found by solver before CoW takes its cut; NULL for pre-2024 trades and volume-only policies.
- `unique_traders`: `countDistinct(taker)` per day — non-additive; the 7d KPI correctly bypasses this by running `uniqExact` on `fct_execution_cow_trades` directly.
- Batch routing categories: Pure CoW, Partial CoW (`num_trades > 1 AND num_interactions > 0 AND num_interactions < num_trades`), Pure DEX (all others).
- Solver labels: 8 named entries in `seeds/cow_solvers.csv`; unlabeled solvers fall back to truncated address.

**Contract addresses** are cross-verified across `seeds/function_signatures.csv`, the contract SQL, and public cerebro-docs. Minor spelling discrepancy: the seed uses `GPv2AllowListAuthentication` (capital L) while public docs use `GPv2AllowlistAuthentication` (lowercase l) — no functional impact.

---

## Implementation assessment

### Critical

**cow_api_trade_fees ingestor 42 days stale — fee and solver-value KPIs serve NULL**
`models/execution/cow/marts/api_execution_cow_kpi_fees_7d.sql`, `api_execution_cow_kpi_solver_value_7d.sql`, `fct_execution_cow_daily.sql`

Confirmed via warehouse query: `crawlers_data.cow_api_trade_fees` `max(ingested_at) = 2026-04-30`, 42 days behind today. Only 75 of 42,251 trades (0.18%) in the last 30 days have `fee_source='api'`. `fct_execution_cow_daily.fees_usd` and `solver_value_usd` are NULL for every day from 2026-06-07 through 2026-06-11. Both `api_execution_cow_kpi_fees_7d` and `api_execution_cow_kpi_solver_value_7d` return NULL to dashboard consumers. This is an operational crawler failure, not a dbt logic bug, but neither model has a freshness test or alert to surface it.

---

### High

**No dbt source freshness test on cow_api_trade_fees**
`models/crawlers_data/staging/stg_crawlers_data__cow_api_trade_fees.sql`

`sources.yml` defines `loaded_at_field: ingested_at` on `crawlers_data.cow_api_trade_fees` but configures no `warn_after`/`error_after` threshold. The current 42-day staleness is completely invisible to `dbt test` runs; consumers must discover it themselves through NULL dashboard panels.

---

**All 14 api_* marts (plus fct_ marts) missing `production` tag — `check_api_tags.py` never validates them**
`models/execution/cow/marts/fct_execution_cow_daily.sql`, `fct_execution_cow_trades.sql`, all `api_execution_cow_*` marts; `models/execution/cow/staging/stg_cow__solvers.sql`, `stg_cow__interactions.sql`

`check_api_tags.py` only validates production-tagged models. The entire cow API surface lacks the tag, so tier/granularity/window/column-schema convention is never CI-enforced. Only `stg_cow__trades`, `stg_cow__settlements`, and `int_execution_cow_trades` carry `production` — an inconsistent subset. `stg_cow__solvers` and `stg_cow__interactions` also lack it, meaning their downstream `fct_execution_cow_solvers` and `fct_execution_cow_solvers_daily` may be silently skipped by `+tag:production` dbt selectors.

---

**Incremental lookback mismatch in `int_execution_cow_trades` (3-day settlements vs full-month trades)**
`models/execution/cow/intermediate/int_execution_cow_trades.sql`

Confirmed in code: the trades CTE uses `apply_monthly_incremental_filter` (whole-partition, full-month recompute), while the settlements sub-query uses a raw `block_timestamp >= addDays(max(block_timestamp), -3)` filter. On a partition-replace rerun early in a month, settlements older than 3 days relative to the table's current `max(block_timestamp)` are absent, producing NULL solver attribution on those trades for the remainder of the month until the next full rerun.

---

**Incremental lookback mismatch in `int_execution_cow_batches` (3-day interactions vs full-month batches)**
`models/execution/cow/intermediate/int_execution_cow_batches.sql`

Symmetric issue: the batches CTE uses the monthly partition filter while the interactions sub-query uses `addDays(max(block_timestamp), -3)`. On an early-month partition-replace rerun, interaction counts for older days in the month default to 0 via `coalesce`, misclassifying multi-trade DEX batches as `is_cow=TRUE` (Pure CoW) and inflating `cow_ratio` until the next full rerun.

---

### Medium

**`fct_execution_cow_daily.cow_ratio` returns 0, not NULL, on unmatched batch_daily LEFT JOIN**
`models/execution/cow/marts/fct_execution_cow_daily.sql`

The expression `if(b.num_batches > 0, ..., 0)` returns the false branch when `b.num_batches` is NULL (LEFT JOIN miss) because ClickHouse evaluates `if(NULL > 0, ...)` as falsy. Days with trade data but no matching batch row appear as 0% CoW ratio rather than missing/NULL, understating the metric silently.

---

**Two computation paths for fees and solver-value can diverge on negative corrections**
`models/execution/cow/marts/api_execution_cow_fees_ts.sql`, `api_execution_cow_solver_value_ts.sql`, `fct_execution_cow_daily.sql`

`api_execution_cow_fees_ts` and `api_execution_cow_solver_value_ts` query `fct_execution_cow_trades` directly with a `fee_usd > 0` / `solver_value_usd > 0` positivity filter. `fct_execution_cow_daily` uses `sumIf(fee_usd, fee_source='api')` without any positivity filter. If the CoW API emits negative fee corrections (which is plausible for adjustments), the ts endpoints and the daily rollup will report different totals for the same metric and date.

---

**`api_execution_cow_kpi_active_solvers` missing `window:7d` tag**
`models/execution/cow/marts/api_execution_cow_kpi_active_solvers.sql`

All other 7-day KPI models carry both `granularity:last_7d` and `window:7d`. This model carries only `granularity:last_7d`. Harmless while the `production` tag is absent, but will fail the convention check once that tag is added.

---

### Low

**16,252 all-time trades (0.6%) have NULL amount_usd from no price match**
`models/execution/cow/intermediate/int_execution_cow_trades.sql`

2,686,285 total rows; 16,252 have `NULL amount_usd`, concentrated on unlisted/symbol-less historical tokens. Recent coverage is excellent (1 of 42,251 in last 30 days). These rows contribute 0 to `volume_usd` aggregates, slightly underreporting all-time cumulative volume.

---

**Symbol-based price join is vulnerable to symbol collisions across token addresses or rebuilds**
`models/execution/cow/intermediate/int_execution_cow_trades.sql`

The ASOF join to `int_execution_token_prices_daily` is keyed on token symbol (not address). Tokens sharing a symbol across different contract addresses, or a token whose symbol changes between rebuild windows, can select an incorrect price row. Impact is low for recent data but represents a latent valuation risk without a documented guardrail.

---

## Business-logic assessment

### High

**Semantic model `execution_cow_top_pairs_weekly` references columns that do not exist — `cow_top_pairs_volume` fails at query time**
`semantic/authoring/execution/cow/semantic_models.yml`, `models/execution/cow/marts/api_execution_cow_top_pairs_weekly.sql`

Confirmed in code. The mart outputs columns `date`, `label`, and `value`. The semantic model's entity, dimension, and measure expressions reference `week`, `pair`, `volume_usd`, and `num_trades` — none of which exist in the mart. The promoted approved metric `cow_top_pairs_volume` and the candidate metric `execution_cow_pair_trades_value` both bind to these column names. Any MCP or semantic-layer consumer querying top CoW pairs will receive a runtime column-not-found error rather than data.

---

**Pre-Sep-2024 `fee_usd` (fee_source='onchain') massively overstates protocol revenue — standing business caveat**
`models/execution/cow/marts/fct_execution_cow_trades.sql`, `api_execution_cow_fees_ts.sql`

Documented in schema.yml and SQL comments; the daily mart correctly gates on `fee_source='api'`. Recorded here as a formal caveat: any revenue analysis or semantic metric must hard-filter `fee_source='api'`, and the mixed mid-2023 to early-2024 transition window must not be summed without filtering. The exact cutover date for Gnosis Chain's switch to surplus-based fees has not been validated against the `cow_api_trade_fees` coverage boundary.

---

### Medium

**Four auto-generated candidate metrics are marked `quality_tier: approved` despite their own review warnings**
`semantic/authoring/execution/cow/semantic_models.yml`

`execution_cow_batches_value`, `execution_cow_cow_batches_value`, `execution_cow_gas_native_value`, and `execution_cow_pair_trades_value` all carry descriptions containing "Auto-generated candidate metric; review and promote before relying on it," yet each has `quality_tier: approved`. The approved tier is therefore not a reliable trust signal for this unit.

---

**`cow_active_solvers` semantic measure uses `agg: avg` — averages daily distinct counts, not distinct solvers over a period**
`semantic/authoring/execution/cow/semantic_models.yml`, `models/execution/cow/marts/fct_execution_cow_daily.sql`

Averaging daily `countDistinct(solver)` across a month yields mean daily active solvers, not the distinct solver count over that period (which the 7-day KPI correctly computes via `uniqExact` on `fct_execution_cow_trades`). May be intentional but is undocumented and easily misread.

---

**`solver_value_usd` has marts and a 7d KPI but no promoted semantic metric**
`semantic/authoring/execution/cow/semantic_models.yml`, `models/execution/cow/marts/api_execution_cow_solver_value_ts.sql`

`api_execution_cow_solver_value_ts` and `api_execution_cow_kpi_solver_value_7d` exist and are operational, but no corresponding semantic metric is registered. MCP consumers cannot answer solver-value questions through the semantic layer.

---

### Low

**Partial CoW label may under-credit peer matching**
`models/execution/cow/marts/api_execution_cow_batch_routing_ts.sql`

The Partial CoW label requires `num_interactions < num_trades`. A batch with 3 trades and 3 interactions is labelled Pure DEX even if one pair was CoW-matched. Should be confirmed against the intended business definition before the routing breakdown is published externally.

---

**ETH-flow orders not explicitly accounted for or excluded**
`models/execution/cow/intermediate/int_execution_cow_trades.sql`

`contracts_CowProtocol_CoWSwapEthFlow_events` is decoded but unreferenced by this unit. ETH-flow trades emit Trade events on `GPv2Settlement` when settled, so they are likely captured by the existing pipeline, but this is neither asserted nor tested. Volume is likely negligible on Gnosis Chain; the treatment should be documented.

---

## Data findings

Warehouse queries run during the review (8 total):

| Query | Result |
|---|---|
| Grain uniqueness (last 14 days, `int_execution_cow_trades`) | 0 duplicate `(block_timestamp, transaction_hash, log_index)` keys across 16,610 rows |
| Daily cross-count comparison (`fct_execution_cow_daily` vs `fct_execution_cow_trades`) | diff = 0 for all 5 days tested |
| `fct_execution_cow_daily` max date | 2026-06-11 (current to today) |
| `cow_api_trade_fees` max `ingested_at` | 2026-04-30 (42 days stale; numeric date 20573 confirmed) |
| Recent `fee_source` distribution (last 30 days) | 75 of 42,251 trades (0.18%) have `fee_source='api'` |
| `api_execution_cow_kpi_fees_7d` value | NULL |
| `fct_execution_cow_daily` `fees_usd` (2026-06-07 to 2026-06-11) | NULL for every day |
| NULL `amount_usd` count (all-time vs last 30d) | 16,252 of 2,686,285 all-time (0.6%); 1 of 42,251 last 30 days |

Core trade and volume metrics are current and internally consistent. The only active data failure is the fee/solver-value ingestor outage.

---

## Pros / Cons

**Pros**

- Clean event-decoded lineage from staging views through documented incremental intermediates to fact layer and 14 api_* marts; no structural surprises across all 27 SQL files.
- Grain integrity verified: zero duplicates on `(block_timestamp, transaction_hash, log_index)` over 14 days; `fct_execution_cow_daily.num_trades` reconciles exactly to `fct_execution_cow_trades` row counts for all 5 days tested (diff = 0).
- Fee-model discontinuity (CoW v1 signed-maximum ceiling vs v2 surplus-based revenue) is clearly documented in schema.yml and SQL comments; `fee_source` provides a reliable revenue filter.
- Non-additive `unique_traders` is correctly handled: the 7d KPI uses `uniqExact` on the fact table directly rather than summing daily aggregates.
- Recent price coverage is excellent: only 1 of 42,251 trades in the last 30 days has `NULL amount_usd`.
- Both contract addresses cross-verified across seeds, SQL, and public docs.
- Core trade, volume, and batch metrics are current to 2026-06-11.

**Cons**

- Fee and solver-value KPIs have served NULL to consumers for 42 days; the crawler outage is undetected by CI and invisible without manual inspection.
- The entire cow API surface (14 api_* marts, fct_ marts) lacks the `production` tag, bypassing all `check_api_tags.py` convention enforcement.
- The `execution_cow_top_pairs_weekly` semantic model is broken against its own mart (column names diverged: `week`/`pair`/`volume_usd`/`num_trades` vs actual `date`/`label`/`value`), causing `cow_top_pairs_volume` to fail at query time.
- Two incremental lookback mismatches (3-day vs full-month) create latent solver-attribution NULLs and `cow_ratio` inflation on partition-boundary reruns.
- No source freshness test despite `loaded_at_field` being defined; ingestor staleness is invisible to `dbt test`.
- Four auto-generated candidate metrics carry `quality_tier: approved` despite their own descriptions recommending review first.
- Two separate computation paths for the same fee/solver-value metric (ts endpoints filter `> 0`; daily mart does not) can diverge silently on negative API corrections.
- `solver_value_usd` has dedicated marts but no promoted semantic metric.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 | Escalate the `cow_api_trade_fees` crawler outage as an active incident (42 days stale, NULL fee/solver-value KPIs in production). File or confirm a tracking ticket; backfill once the crawler is restored. | `crawlers_data.cow_api_trade_fees`, `api_execution_cow_kpi_fees_7d`, `api_execution_cow_kpi_solver_value_7d` |
| P0 | Fix the `execution_cow_top_pairs_weekly` semantic model: rename expression references to match the mart's actual column names (`week` -> `date`, `pair` -> `label`, `volume_usd` -> `value`); drop the non-existent `num_trades` measure (or align the mart), then reload the semantic registry and smoke-test `cow_top_pairs_volume`. | `semantic/authoring/execution/cow/semantic_models.yml`, `api_execution_cow_top_pairs_weekly.sql` |
| P1 | Add a source freshness block (`warn_after: 2d`, `error_after: 4d`) on `crawlers_data.cow_api_trade_fees` using the existing `loaded_at_field: ingested_at`. | `sources.yml` (crawlers_data layer) |
| P1 | Add the `production` tag to all 14 `api_*` marts and the `fct_*` marts; add it to `stg_cow__solvers` and `stg_cow__interactions` to keep `+tag:production` run selectors complete. | All `api_execution_cow_*` and `fct_execution_cow_*` mart files; `stg_cow__solvers.sql`, `stg_cow__interactions.sql` |
| P1 | Align incremental lookbacks in both intermediates: change the settlements sub-query in `int_execution_cow_trades` and the interactions sub-query in `int_execution_cow_batches` to use the same whole-month partition filter as their primary CTEs, eliminating partition-boundary solver-attribution NULLs and `cow_ratio` inflation. | `int_execution_cow_trades.sql`, `int_execution_cow_batches.sql` |
| P2 | Change `fct_execution_cow_daily.cow_ratio` to return NULL (not 0) when `b.num_batches` is NULL/0 so days without matched batches surface as missing rather than falsely 0%. | `fct_execution_cow_daily.sql` |
| P2 | Unify the fee/solver-value computation path: either add the `> 0` positivity filter to the daily mart or remove it from the ts endpoints so they cannot diverge on negative API corrections. | `api_execution_cow_fees_ts.sql`, `api_execution_cow_solver_value_ts.sql`, `fct_execution_cow_daily.sql` |
| P2 | Resolve the auto-generated metric quality tier: demote the four candidate metrics (`execution_cow_batches_value`, `execution_cow_cow_batches_value`, `execution_cow_gas_native_value`, `execution_cow_pair_trades_value`) to a non-approved tier, or formally review them and strip the "review before relying" warning. | `semantic/authoring/execution/cow/semantic_models.yml` |
| P3 | Add the `window:7d` tag to `api_execution_cow_kpi_active_solvers` for 7d-KPI convention consistency. | `api_execution_cow_kpi_active_solvers.sql` |
| P3 | Decide whether to promote `solver_value_usd` as a semantic metric (data is available post-Sep 2024); document the decision either way. | `semantic/authoring/execution/cow/semantic_models.yml` |
| P3 | Document the symbol-based price-join risk and the ETH-flow treatment in schema.yml; confirm the Partial CoW routing label definition with the business team before external publishing. | `int_execution_cow_trades.sql`, `api_execution_cow_batch_routing_ts.sql`, `schema.yml` (marts) |

---

## Open disagreements

None. The two agent reports converged fully in round 1.

---

## Review log

**Round 1**

| Action | Outcome |
|---|---|
| Inspector: full read of all 27 SQL files + schema.yml across all three layers; 8 warehouse queries run (freshness, grain uniqueness, daily cross-count, ingestor staleness, fee_source distribution, KPI value check) | All findings confirmed by data |
| Context agent: read dbt_project.yml, semantic models, check_api_tags.py, public docs, contract seeds | Independently surfaced same core defects; added semantic column-mismatch finding and fee-model discontinuity business caveats |
| Arbitrator cross-check: verified semantic model column mismatch in code; confirmed both incremental mismatches, cow_ratio LEFT-JOIN-returns-0 bug, ts-vs-daily positivity divergence, missing production tags | No open code unknowns after verification; convergence declared |
