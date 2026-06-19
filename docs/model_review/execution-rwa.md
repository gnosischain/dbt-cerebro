# Model review: execution/rwa

**Convergence:** Converged in 1 round — inspector and context reports are mutually consistent; all load-bearing claims independently verified against the warehouse; no unresolved disagreements.

---

## Scope and inventory

The `execution/rwa` sector is a narrow, purpose-built price-oracle pipeline for 9 Backed Finance (BackedFi) tokenized real-world asset tokens deployed on Gnosis Chain. It has no user-activity or volume metrics — it is purely a price reference feed.

| Layer | Count | Models |
|---|---|---|
| Intermediate | 1 | `int_execution_rwa_backedfi_prices` (incremental, insert_overwrite) |
| Fact mart | 1 | `fct_execution_rwa_backedfi_prices_daily` (view, forward-fill) |
| API mart | 1 | `api_execution_rwa_backedfi_prices_daily` (view, thin select from fct) |
| Contract sources | 9 | `contracts_backedfi_b*_Oracle_events` (one per ticker) |

Total: 4 SQL model files reviewed. The unit also registers 3 semantic models in `semantic/authoring/execution/rwa/semantic_models.yml`.

---

## Business context

**Intended purpose.** The unit answers: "What was the NAV-based price of each BackedFi RWA token on a given date?" It serves four downstream use cases: (1) the REST API endpoint `GET /v1/execution/rwa_backedfi_prices/daily` (tier1, partner access); (2) the shared token-price hub `int_execution_token_prices_daily` at priority 2, feeding all USD-denominated downstream models across token metrics, GPay balances, UBO TVL, and lending; (3) the MCP semantic layer (`quality_tier: candidate`) for ad-hoc queries; (4) the canonical reference implementation pattern for Chainlink oracle decoding on Gnosis Chain (documented in `docs/native_token_prices_build_plan.md`).

**Canonical definitions.**

- **bticker:** BackedFi instrument identifier and primary key. Hardcoded Jinja loop values: `bC3M` (3-month French T-bills), `bCOIN` (Coinbase equity), `bCSPX` (S&P 500 ETF), `bHIGH` (iShares USD HY Bond ETF), `bIB01` (short-duration USD T-bill ETF), `bIBTA` (US Treasury Bond ETF), `bMSTR` (MicroStrategy equity), `bNVDA` (Nvidia equity), `TSLAx` (Tesla equity).
- **Daily closing price:** `argMax(toUInt256OrNull(decoded_params['current']) / 1e8, block_timestamp)` grouped by `toStartOfDay(block_timestamp)` — the oracle answer emitted latest within each UTC calendar day, with 8-decimal-place precision inherited from Chainlink's standard `int256` answer field.
- **Forward-fill:** The fct model uses per-ticker ClickHouse `ORDER BY date WITH FILL` (looped via Jinja to avoid cross-ticker contamination) plus `last_value(price) IGNORE NULLS OVER (UNBOUNDED PRECEDING)` to produce a continuous calendar series through `today()-1`. Rows before each ticker's first oracle event are dropped via `WHERE price IS NOT NULL`.
- **AnswerUpdated event:** Chainlink standard aggregator event carrying `int256 current` (raw price), `uint256 roundId`, and `uint256 updatedAt`. Decoded via `decode_logs` into a `Map(String,Nullable(String))` `decoded_params` column; filtered on `event_name = 'AnswerUpdated'`.

**Contract context.** Nine per-token oracle contracts on Gnosis Chain, each a Chainlink-compatible price aggregator deployed by BackedFi. Oracle addresses are hardcoded in the individual contract models and do not appear in any registry or seed file. All 9 token addresses appear in `seeds/tokens_whitelist.csv` with `token_class=RWA`. `TSLAx`/`TSLAX` mixed-case is correctly resolved via `upper()` on both sides of the token-price hub join.

---

## Implementation assessment

### High severity

**Unbounded forward-fill serves stale prices as current; no staleness cap or valid-through column.**
`models/execution/rwa/marts/fct_execution_rwa_backedfi_prices_daily.sql` forward-fills each ticker via `last_value IGNORE NULLS` to `today()-1` with no staleness cap and no column recording the last real oracle date. Verified in warehouse: bC3M flatlines at 126.2 for 12+ consecutive days in both the fct view and the central price hub. A consumer cannot distinguish a live price from a 49-day-old one. This is the structural root of the most material data defect in this unit.

**`freshness_anomalies` test is structurally blind to per-ticker oracle staleness.**
`models/execution/rwa/marts/schema.yml` places `elementary.freshness_anomalies` on the mart views. Because the forward-fill guarantees all 9 tickers emit a row every day regardless of true oracle activity, the test sees healthy daily volume and can never fire — not even for a completely stalled feed. Freshness must be asserted against `int_execution_rwa_backedfi_prices` (real oracle events) per-ticker, not against the filled mart.

### Medium severity

**No grain uniqueness test on `(bticker, date)` in fct or api marts.**
`models/execution/rwa/intermediate/schema.yml` has `dbt_utils.unique_combination_of_columns(date, bticker)` on the int_ source; neither `fct_` nor `api_` mart schema.yml does. Grain is clean today (warehouse verified: `total_rows == unique_dates` for all 9 tickers, zero nulls), but a future model change duplicating a ticker would silently inflate consumer data with no test failure.

**Global incremental watermark blocks targeted single-ticker gap-fill.**
`models/execution/rwa/intermediate/int_execution_rwa_backedfi_prices.sql` reads `max(toDate(date))` from `{{ this }}` across ALL tickers as its incremental watermark (via `macros/db/get_incremental_filter.sql`). A stalled ticker cannot be re-pulled without a manual `start_month`/`end_month` var pass or a per-partition full-refresh. This is also the lever needed to recover bC3M if its oracle resumes.

**`models/contracts/backedfi/schema.yml` column docs are fabricated across all 9 oracle models.**
Actual decode output confirmed via `describe_table`: `block_number`, `block_timestamp`, `transaction_hash`, `transaction_index`, `log_index`, `contract_address`, `event_name`, `decoded_params` (`Map(String,Nullable(String))`). The schema.yml lists non-existent columns: bC3M documents `answer`/`oracle_id`; bCOIN documents `amount_wei`/`token_address`; TSLAx documents `event_type`/`event_data`. Schema-driven tooling and new contributor onboarding are actively misled.

### Low severity

**api mart schema.yml declares `price` as `UInt64`; warehouse type is `Nullable(Float64)`.**
`models/execution/rwa/marts/schema.yml` line 21 sets `data_type: UInt64` for `api_execution_rwa_backedfi_prices_daily.price`. Confirmed via `describe_table`: actual type is `Nullable(Float64)`. The CI api-tag guard checks only for the presence of `data_type`, not correctness, so it passes. The fct_ schema correctly documents `Float64`. MCP/cerebro-api consumers of this tier1 endpoint may expect integers.

**int_ schema.yml describes a Float column as "unsigned integer with 8 decimal places".**
`models/execution/rwa/intermediate/schema.yml` price column description says "expressed as an unsigned integer with 8 decimal places precision." The `/POWER(10,8)` division yields `Nullable(Float64)` — unsigned integers cannot carry decimal precision. The description is self-contradictory and misleading.

**fct view reads `ReplacingMergeTree` source without `FINAL`.**
`models/execution/rwa/marts/fct_execution_rwa_backedfi_prices_daily.sql` selects from `int_execution_rwa_backedfi_prices` (RMT, `order_by=(date,bticker)`) without `FINAL`. Structurally safe: `insert_overwrite` atomically replaces partitions and `argMax` yields one row per `(bticker,date)` per run; grain verified clean. A concurrent background merge could in theory expose transient duplicates — `FINAL` would eliminate this at modest cost on this small table.

**`fct` `fill_start` hardcoded to `2020-01-01`, years before any token existed.**
The earliest oracle data is 2023-04-01 (bIB01/bIBTA). `WITH FILL` generates approximately 3 years of `NULL` calendar rows per ticker that are pruned by `WHERE price IS NOT NULL` — correct output but wasted computation. Anchoring to 2023-04-01 (the int_ `full_refresh start_date` from schema meta) would remove the overhead.

---

## Business-logic assessment

### High severity

**Stale forward-filled price is indistinguishable from a live NAV for an external consumer.**
For bC3M, the honest answer since 2026-04-23 is "oracle silent / price unknown," but the tier1 API and the central price hub both return 126.2 as if current. Any external partner or quarterly report using bC3M (or any future stalled ticker) receives a fabricated current price. This is the trust-defining defect: the model does not represent "we have no fresh price" — it relabels last-known price as today's price. The unit affects `models/execution/rwa/marts/fct_execution_rwa_backedfi_prices_daily.sql`, `models/execution/rwa/marts/api_execution_rwa_backedfi_prices_daily.sql`, and `models/execution/prices/intermediate/int_execution_token_prices_daily.sql`.

### Medium severity

**Historical coverage gaps yield null/zero USD valuations with no fallback, undisclosed to consumers.**
bCOIN listed 2023-02-19 but oracle data starts approximately 2025-01-01 (~23-month gap); bCSPX ~21-month gap; bMSTR/bNVDA ~4-month gap. During these periods the tokens have no price in the hub and the Dune fallback has no coverage either, confirmed by warehouse queries on `int_execution_token_prices_daily`. Downstream USD-denominated transfers and balances silently produce null or zero with no consumer warning.

**Semantic layer triple-registers the same average-price metric with shared synonyms.**
`semantic/authoring/execution/rwa/semantic_models.yml` registers `int_`, `fct_`, and `api_` each with the same single `average(price)` measure and overlapping `question_synonyms` (`'price'`, `'execution rwa backedfi prices'`). The MCP dispatcher cannot reliably disambiguate them. `api_execution_rwa_backedfi_prices_daily` is a thin `SELECT` from `fct_` — registering both adds no analytical value and guarantees ambiguous resolution.

### Low severity

**USD denomination is never stated in any schema.yml description.**
Prices are assumed USD (Chainlink/BackedFi NAV convention) but no column description asserts this. For a tier1 external endpoint, currency must be explicit. Note that bC3M is a EUR-denominated instrument (3-month French T-bills) — the oracle answer currency should be verified before asserting a blanket USD label, to avoid a unit-mislabeling error for that ticker specifically.

---

## Data findings

Seven warehouse queries were run by the inspector. Key results:

| Finding | Result |
|---|---|
| int_ grain (`total_rows == unique_dates`) | Clean for all 9 tickers; zero null prices |
| bC3M `max_date` in int_ (FINAL) | 2026-04-23 — 49 days before review date |
| bC3M price in fct and price hub | 126.2, flatlined from 2026-04-23 through latest hub date (2026-06-08) |
| bHIGH / bIB01 / bIBTA `max_date` | 2026-06-07 (4 days) — expected weekday gap |
| Remaining 6 tickers `max_date` | 2026-06-05 (6 days) — expected weekend gap |
| fct mart grain | No duplicate `(bticker, date)` rows |
| `int_execution_token_prices_daily` bC3M rows | Present at 126.2 through 2026-06-08 (stale price confirmed in hub) |

The grain is healthy for all tickers except bC3M, whose 49-day silence is unambiguously beyond normal tolerance (bC3M is not an equity token with weekday trading gaps — it tracks French T-bill NAV and updates continuously). All 4-6 day gaps for other tickers are consistent with weekday-only oracle update cadence.

---

## Pros / Cons

**Pros**

- Core SQL logic is correct: per-day `argMax` dedup, `(date, bticker)` grain holds in the source-of-truth int_ model, `insert_overwrite` + `ReplacingMergeTree` partitioning sound.
- int_ model carries a `dbt_utils.unique_combination_of_columns(date, bticker)` grain test guarding the incremental source.
- Per-ticker `WITH FILL` Jinja loop correctly avoids ClickHouse cross-ticker fill contamination (a known CH limitation).
- Clean priority layering in the price hub: backedfi at priority 2, above Dune fallback (3), below native Chainlink (1).
- `TSLAx`/`TSLAX` mixed-case correctly resolved via `upper()` on both sides of the hub join.
- Tier1 api endpoint is api-tag-convention compliant (single `api:` resource, `granularity:`, `tier:` present, typed columns, exposes `date`).
- Documented reference implementation for Chainlink oracle decoding on Gnosis Chain.
- Memory-pressure remediation (`start_month`/`end_month` branching + pre/post hooks) already applied after the June 2026 OOM during the cron_preview run.

**Cons**

- No staleness metadata: the pipeline cannot represent "no data yet" or "data went stale" — it silently forward-fills a constant price to `today()-1` for any stalled ticker.
- bC3M oracle silent for 49 days; stale 126.2 propagates into the central price hub and every USD-denominated downstream (portfolio valuation, UBO TVL, GPay balances, lending).
- `elementary.freshness_anomalies` is structurally blind — all 9 tickers emit a row every day by design, so the test can never fire regardless of true oracle health.
- Historical coverage gaps (bCOIN ~23 months, bCSPX ~21 months, bMSTR/bNVDA ~4 months) leave downstream USD models null/zero for those periods with no Dune fallback coverage.
- Documentation defects reaching external consumers: api mart `price` typed `UInt64` (actual `Nullable(Float64)`); int_ description calls a Float an "unsigned integer."
- `contracts/backedfi/schema.yml` column docs are fabricated across all 9 oracle models — actively misleads schema-driven tooling and onboarding.
- Marts (fct + api) carry no grain uniqueness test; a future regression duplicating `(bticker, date)` would silently inflate consumer data.
- Semantic layer triple-registers the same average-price metric (int/fct/api) with shared `question_synonyms` — guarantees ambiguous MCP resolution with no added analytical value.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 | Triage bC3M immediately: determine whether oracle `0x83Ec02059F686E747392A22ddfED7833bA0d7cE3` was migrated/delisted or the pipeline broke. If migrated, add the new oracle address and recover via `start_month`/`end_month` backfill. If delisted, suppress the ticker from the forward-fill. | `models/contracts/backedfi/`, `int_execution_rwa_backedfi_prices.sql` |
| P1 | Add per-ticker staleness guard on the int_ source: assert `elementary.freshness_anomalies` (or a custom `not_null_proportion` / `recency` test) against `int_execution_rwa_backedfi_prices` per `bticker`, not against the forward-filled mart views. | `models/execution/rwa/intermediate/schema.yml` |
| P1 | Add a `last_oracle_date` / `valid_through` column to fct (and surface it in the api mart and price hub) so consumers can filter or warn on forward-filled vs live prices. Consider capping forward-fill to a maximum of N days (e.g. 7) past the last real observation. | `fct_execution_rwa_backedfi_prices_daily.sql`, `api_execution_rwa_backedfi_prices_daily.sql`, `int_execution_token_prices_daily.sql` |
| P2 | Fix the api mart schema.yml `price` `data_type` from `UInt64` to `Nullable(Float64)` — this is a tier1 external type contract. | `models/execution/rwa/marts/schema.yml` |
| P2 | Add `dbt_utils.unique_combination_of_columns(bticker, date)` tests to both `fct_` and `api_` mart schema.yml to match the int_ grain guard. | `models/execution/rwa/marts/schema.yml` |
| P2 | Regenerate `models/contracts/backedfi/schema.yml` for all 9 oracle models from the catalog. Actual decode output is `decoded_params Map(String,Nullable(String))` — the current column docs are fabricated. | `models/contracts/backedfi/schema.yml` |
| P3 | Collapse the three semantic models to one canonical price metric (api or fct) with distinct `question_synonyms` to remove MCP ambiguity. | `semantic/authoring/execution/rwa/semantic_models.yml` |
| P3 | Document USD denomination explicitly in all schema.yml price column descriptions; verify bC3M oracle currency (EUR vs USD) before asserting blanket USD labeling. | `models/execution/rwa/marts/schema.yml`, `models/execution/rwa/intermediate/schema.yml` |
| P4 | Fix int_ schema.yml price column description — remove the self-contradictory "unsigned integer with 8 decimal places" wording; the actual type is `Nullable(Float64)`. | `models/execution/rwa/intermediate/schema.yml` |
| P4 | Anchor `fct` `fill_start` to `2023-04-01` (or per-ticker min date) instead of `2020-01-01` to cut ~3 years of wasted `WITH FILL` computation per ticker. | `fct_execution_rwa_backedfi_prices_daily.sql` |
| P4 | Move oracle addresses into a registry seed (e.g. `backedfi_oracle_registry.csv`) so address rotations are detectable and adding a ticker is data-driven rather than a Jinja loop code change. | `models/execution/rwa/intermediate/int_execution_rwa_backedfi_prices.sql` |
| P4 | Add `FINAL` to the fct view's select from the `ReplacingMergeTree` int_ source to eliminate theoretical transient duplicates during background merges. | `fct_execution_rwa_backedfi_prices_daily.sql` |

---

## Open disagreements

None. The review converged in 1 round.

---

## Review log

**Round 1**

- Inspector issued no challenges to the context report.
- Context agent issued no challenges to the inspector report.
- All load-bearing claims verified against the warehouse (7 queries). bC3M 49-day staleness, forward-fill propagation to price hub, api mart `UInt64` type mismatch, fabricated contracts schema.yml columns, and missing mart grain tests all confirmed independently.
- Final verdict: converged; no open items.
