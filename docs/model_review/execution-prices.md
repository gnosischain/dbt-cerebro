# Model review: execution/prices

**Convergence:** Converged in 1 round — inspector and context reports were mutually consistent; all load-bearing claims (SAFE 3.1x overstatement, hub uniqueness-test gap, WxDAI dual-priority non-determinism, schema drift) were verified directly in the warehouse and SQL source before the verdict was issued.

---

## Scope and inventory

The `execution/prices` unit is a pure infrastructure layer: four intermediate models that assemble the single authoritative `(date, symbol, price_usd)` table consumed by all downstream USD valuations on Gnosis Chain. There are no mart- or API-layer models in this unit; its sole output is a SQL `ref()` target.

| Layer | Count | Models |
|---|---|---|
| Intermediate | 4 | `int_execution_prices_oracle_daily`, `int_execution_prices_dex_ratios`, `int_execution_prices_native_daily`, `int_execution_token_prices_daily` |
| Fact / Mart / API | 0 | — |

All four models live under `models/execution/prices/intermediate/`. The unit feeds ~26 downstream intermediate and fact models across pools, CoW, GPay, lending, tokens, transfers, yields, revenue, and the MMM econometric model. One semantic model exists at quality tier `candidate`; there is no REST/API endpoint for raw price series.

---

## Business context

**Intended purpose.** The unit answers: "What was the USD price of whitelisted Gnosis Chain token X on date D?" Consumers JOIN on `(date, symbol)` to convert native token amounts to USD for TVL, swap volume, fee revenue, wallet balance snapshots, lending collateral values, and GNO/ETH/EURe control variables in econometric models. The hub's output shape is the stable contract; migration from Dune to fully on-chain native pricing is hub-internal and requires no downstream model changes at cutover.

**Priority stack in the hub (`int_execution_token_prices_daily`).**

| Priority | Source | Scope |
|---|---|---|
| 1 | Native on-chain (Chainlink oracle + DEX-derived + vault rates) | All oracle-covered and DEX-priced tokens |
| 2 | BackedFi RWA oracle (`fct_execution_rwa_backedfi_prices_daily`) | bCSPX, bTSLA, bMSTR, bIBTA, bNVDA, bCOIN, bC3M, bIB01, bHIGH, TSLAX, etc. |
| 3 | Dune external feed (`stg_crawlers_data__dune_prices`) | Universal fallback; pre-2021 history; SAFE and any native gap |
| 4 | Hardcoded $1.00 peg | USDC, USDT, USDC.e catch-all when native is absent |

**Canonical definitions (from schema.yml and SQL).**

- **price (USD):** Daily USD price of a whitelisted Gnosis Chain token, priority-selected across the four source tiers above.
- **symbol:** Preferred presentation symbol from `seeds/tokens_whitelist.csv` (`argMax` over `date_start`), upper-cased internally.
- **Chainlink oracle price:** `argMax(answer_raw / 10^decimals, block_timestamp)` per `(feed, calendar_day)` from AnswerUpdated events. USD feeds use 8 decimals; `wstETH` is a two-step multiply (`wstETH-ETH` 18-decimal rate * same-day `ETH/USD`). Source: `int_execution_prices_oracle_daily`.
- **DEX-derived price:** Daily median implied USD price across qualifying trades (>= $1,000 notional AND >= 5 trades/day) where the target token is swapped against an oracle-priced anchor. Outliers nulled by 30-day rolling MAD (|price - 30d_median| > 3 * 30d_MAD). Covers: GBPe, BRLA, BRZ, COW, SAFE, sGNO. Source: `int_execution_prices_dex_ratios`.
- **sDAI price:** `vault share_price` from `int_yields_savings_xdai_rate_daily` multiplied by `xDAI/USD`; reflects actual ERC4626 redemption rate, not a DEX spot.
- **aToken / spToken price:** 1:1 from reserve token (e.g., `aGnoGNO = GNO`). Mapping via `seeds/lending_market_mapping.csv`.
- **WxDAI price:** `xDAI` price passed through 1:1 via `wxdai_from_xdai` CTE.
- **BRZ <- BRLA fallback:** When no qualifying BRZ DEX price exists, BRZ inherits from BRLA (treated as equivalent BRL stablecoin proxy).
- **sGNO <- GNO fallback:** sGNO inherits GNO oracle price as approximation when no sGNO DEX observation exists.
- **Forex-peg approximation:** EURe = EUR/USD oracle; ZCHF/svZCHF = CHF/USD oracle. Stablecoin depeg events are not captured.

---

## Implementation assessment

### Critical

**SAFE price served 3.1x overstated due to uncapped forward-fill always beating live Dune fallback**
`models/execution/prices/intermediate/int_execution_prices_native_daily.sql`, `int_execution_token_prices_daily.sql`, `int_execution_prices_dex_ratios.sql`

`int_execution_prices_native_daily` forward-fills via `last_value(price) IGNORE NULLS OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` with no age cap; the spine is extended to `today()`. SAFE's last DEX observation is 2025-11-18 (day 20395), so 0.367 propagates ~220 days. The hub assigns native priority=1 and Dune priority=3, so the stale fill always wins. Warehouse-confirmed: hub `SAFE = 0.3667`, Dune `SAFE = 0.1175` for 2026-06-11 — a 3.1x overstatement. All downstream USD valuations (pools TVL/fees, balances, MMM controls) inherit this error.

---

### High

**No forward-fill staleness guard — any DEX-priced token losing liquidity will serve a frozen price indefinitely**
`int_execution_prices_native_daily.sql`, `int_execution_token_prices_daily.sql`

There is no cap on forward-fill age and no mechanism to demote a stale native price below Dune. SAFE is the currently-triggered instance; any future token that loses DEX liquidity will exhibit the same failure mode silently. The defect is structural.

**Price hub lacks `unique_combination_of_columns` test on `(date, symbol)`**
`int_execution_token_prices_daily.sql`, `schema.yml`

Confirmed by schema.yml line-count: `int_execution_prices_oracle_daily` (line 125), `int_execution_prices_dex_ratios` (line 174), and `int_execution_prices_native_daily` (line 245) all carry `dbt_utils.unique_combination_of_columns`; the hub at lines 4-56 has only Elementary volume/freshness/schema anomaly tests. A `row_number()` dedup regression in the hub would propagate duplicates silently to all ~26 consumers.

**sGNO scope documented incorrectly — schema.yml omits sGNO from dex_ratios coverage**
`int_execution_prices_dex_ratios.sql`, `schema.yml`

The SQL WHERE clause and `legs` CTE include `'SGNO'`; the warehouse confirms 4 sGNO rows (2023-10-21 to 2024-03-09). The schema.yml model description and symbol-column description list only `'GBPe, BRLA, BRZ, COW, SAFE'`. A maintainer auditing sGNO pricing would not learn from the docs that a DEX price path exists before the GNO-oracle fallback kicks in.

---

### Medium

**WxDAI dual-sourced at equal priority=1 makes dedup non-deterministic**
`int_execution_token_prices_daily.sql`

`all_prices` unions: (a) native (which already contains oracle-derived `WXDAI` from the oracle's `feed_symbols` DAI->WxDAI mapping) at priority=1, and (b) `wxdai_from_xdai` CTE (re-aliasing `XDAI` rows as `WXDAI`) also at priority=1. On days where both exist with diverging values (historically up to ~0.4% before 2021-06-23), `row_number()` picks one non-deterministically. The `wxdai_from_xdai` CTE appears redundant given the oracle already emits `WxDAI`.

**sDAI INNER JOIN on xDAI silently drops sDAI on any xDAI oracle gap day**
`int_execution_prices_native_daily.sql`

The `sDAI` CTE uses `INNER JOIN xdai x ON x.date = r.date`. A missing DAI/USD AnswerUpdated event for any calendar day drops sDAI for that day; the spine's forward-fill then masks the gap. The oracle xDAI series is dense today (1,917 rows), so the risk is low in practice, but there is no defensive fallback.

**Oracle reads append+ReplacingMergeTree source without FINAL — argMax correctness is a latent risk**
`int_execution_prices_oracle_daily.sql`, `models/contracts/chainlink/contracts_chainlink_feeds_events.sql`

`contracts_chainlink_feeds_events` uses `incremental_strategy='append'` + `ReplacingMergeTree`. Without `FINAL`, pre-merge duplicate rows may exist. `argMax(answer_raw, block_timestamp)` is idempotent for exact duplicates, but if a re-decode emits two rows at the same timestamp with different `decoded_params['current']` values (e.g., after an ABI change), `argMax` resolves non-deterministically. Not a confirmed bug; latent risk.

**CoW LEFT JOINs in dex_ratios lack `join_use_nulls` per project convention**
`int_execution_prices_dex_ratios.sql`

The CoW trades subquery LEFT JOINs `stg_pools__tokens_meta` twice. Without `join_use_nulls`, unmatched rows return ClickHouse defaults (`decimals=0`, `token=''`). The `if(decimals>0,...,18)` guard handles the decimals case; empty-symbol tokens are filtered by the whitelist `IN` clause, so impact is contained — but the model deviates from the documented project convention without explanation.

---

### Low

**Hub schema.yml description mislabels a ReplacingMergeTree table as a 'view'**
`schema.yml`

Line 5 reads `"...view consolidates daily price data..."`. The model config is `materialized='table'` with `engine=ReplacingMergeTree`. Misleads developers about storage characteristics and whether `FINAL` is needed on reads.

**Redundant ORDER BY in hub final SELECT**
`int_execution_token_prices_daily.sql`

The final `SELECT` carries `ORDER BY d.date, symbol`. For a full-rebuild `ReplacingMergeTree`, the storage order is determined by `order_by=(date,symbol)` in the engine config; the SELECT-level sort adds rebuild cost with no correctness or query-performance benefit.

**sGNO DEX price series (4 observations, 2023-2024) is effectively dead code**
`int_execution_prices_dex_ratios.sql`, `int_execution_prices_native_daily.sql`

After 2024-03-09, sGNO falls back to GNO oracle price via the priority-2 fallback. The `'SGNO'` filter in `dex_ratios` is consuming pipeline resources for a path that almost never fires. Consider removing sGNO from the DEX whitelist or documenting the intended market-price verification purpose.

---

## Business-logic assessment

### High

**USDC/USDT serve oracle market prices including the 2023 depeg — intent undocumented**
`int_execution_token_prices_daily.sql`

`usd_pegs` is priority=4; native oracle is priority=1. For USDC and USDT, the oracle prices win and the $1 peg only fires as a catch-all when oracle data is absent. Result: 229 historical hub rows deviate >0.5% from $1 (min 0.9689, observed on Dune-era data including the 2023 USDC depeg to 0.969). This may be more historically accurate, but the build plan implies pegs are canonical for these tokens. Downstream stablecoin-collateral USD counts in early history see up to ~3% discrepancies. The intent is not documented anywhere in schema.yml or the SQL.

---

### Medium

**USDC/USDT hub coverage: 229 historical off-peg rows (confirmed data)**
See Business-logic finding above; the 229-row count is a warehouse-confirmed data observation supporting the undocumented-intent finding.

---

### Low

**sGNO priced as GNO for ~99% of its history — staking discount divergence is unacknowledged to consumers**
`int_execution_prices_native_daily.sql`, `int_execution_prices_dex_ratios.sql`

With only 4 DEX observations, sGNO inherits GNO oracle price for nearly all days. The staked-GNO vs GNO-spot gap (Gnosis Beacon Chain rewards accrual) could be material for sGNO TVL or yield calculations. The SQL comment documents the approximation, but schema.yml does not surface it as a consumer caveat.

**BRZ relies entirely on BRLA proxy with no availability monitor**
`int_execution_prices_native_daily.sql`, `int_execution_prices_dex_ratios.sql`

344 hub BRZ rows come from the `BRZ <- BRLA` fallback; the single direct BRZ observation (0.192) matches BRLA, making the proxy reasonable. However, BRZ and BRLA are distinct issuers with no guaranteed 1:1 peg, and there is no alert if BRLA pricing becomes unavailable.

**Forex-peg approximations do not capture stablecoin depeg events**
`int_execution_prices_oracle_daily.sql`

EURe = EUR/USD oracle; ZCHF/svZCHF = CHF/USD oracle. Documented in the build plan but not surfaced as an explicit caveat in schema.yml for downstream consumers.

**Only semantic metric is candidate-tier unfiltered average-of-price**
`int_execution_token_prices_daily.sql`

`execution_token_prices_daily__price_value` averages `price` without symbol filtering and is quality_tier `candidate`. The hub carries tag `production` but no `api:` tag; it is effectively SQL-ref-only. If per-token price series are ever exposed via MCP/API, a validated semantic model or `api_` model is needed.

---

## Data findings

Queries confirmed directly in the warehouse during the review:

| Finding | Value |
|---|---|
| Hub SAFE price (2026-06-11) | 0.3667 USD (forward-filled from 2025-11-18) |
| Dune SAFE price (2026-06-11) | 0.1175 USD |
| SAFE staleness | 220 days since last DEX observation |
| Hub max date | 2026-06-08 (3 days behind today) |
| dex_ratios max date | 2026-06-07 |
| Hub duplicate-grain check (last 30d) | 0 duplicates detected |
| USDC off-peg rows (>0.5% from $1) | 229 out of 2,786 total hub USDC rows; all pre-2023-03 |
| USDC historical min price in hub | 0.9689 |
| sGNO dex_ratios rows | 4 (2023-10-21 to 2024-03-09) |
| BRZ dex_ratios rows | 1 (2025-12-21) |
| BRZ hub rows (BRLA-sourced) | 344 |
| wstETH oracle rows vs expected days | 1,117 vs 1,119 (2-day gap, forward-fill closes it) |
| Oracle symbol count | 12 covered symbols |

Eight primary queries were run plus supplementary queries for per-symbol max dates, SAFE stale duration, oracle symbol enumeration, native_daily symbol list, and BRZ/BRLA cross-check.

---

## Pros / Cons

**Pros**

- Single authoritative price hub with a stable `(date, symbol, price)` JOIN contract; migration to fully native pricing is hub-internal with no downstream impact at cutover.
- Layered priority design (native > RWA/aToken > Dune > $1 peg) is sensible and keeps Dune as a safety net for pre-2021 history.
- Native sub-layers all carry grain-uniqueness tests; oracle-covered symbols are dense and unique.
- DEX-ratio guardrails (>= $1,000 notional, >= 5 trades/day, 30-day rolling-MAD outlier nulling) prevent thin-market noise from entering the hub.
- sDAI uses the actual ERC4626 vault redemption rate rather than a DEX spot — economically correct.
- Partitioning strategy is deliberate (yearly for the wide-history hub, monthly for incrementals) and avoids the CH Cloud 100-partition-per-insert cap.
- Aggregator addresses and migration gates are documented in `docs/native_token_prices_build_plan.md`, providing traceability for contract-level claims.

**Cons**

- Forward-fill has no staleness ceiling and always outranks Dune: any token that loses DEX liquidity silently serves a frozen price forever. SAFE is the live instance; the defect is structural.
- The hub — the model every consumer reads — has no grain-uniqueness test; only the upstream sub-layers do.
- WxDAI is dual-sourced at equal priority=1, making the dedup pick non-deterministic on overlapping days.
- Documentation drift: schema.yml omits sGNO from dex_ratios scope and mislabels the hub as a `view`.
- Stablecoin pricing intent (oracle market prices vs. $1 peg) is ambiguous and undocumented; 229 historical rows deviate from the peg.
- Only one semantic metric exists and it is quality_tier `candidate` (an unfiltered average-of-price); the unit is effectively SQL-ref-only.
- Forex-peg approximations (EURe, ZCHF, svZCHF) do not capture depeg events; this is a documented design caveat but not surfaced in schema.yml.
- Build-plan gaps remain open: osETH feed not wired, pre-2023 phase aggregators for GNO/WETH/WBTC not added.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 — Fix now | Cap the forward-fill or demote stale native prices. Either limit the window to N days (7–30) so fills older than N become NULL and Dune (priority 3) takes over, or add a `native_is_forward_filled AND age > N` demotion path. Re-run `int_execution_prices_native_daily` + hub and confirm SAFE returns ~0.117. | `int_execution_prices_native_daily.sql`, `int_execution_token_prices_daily.sql` |
| P1 | Add `dbt_utils.unique_combination_of_columns` on `(date, symbol)` to `int_execution_token_prices_daily` in schema.yml, matching the three sub-layers. | `schema.yml` |
| P1 | Resolve the WxDAI dual-source: drop the `wxdai_from_xdai` CTE if the oracle already emits `WXDAI`, or assign the two sources distinct priorities so row selection is deterministic. | `int_execution_token_prices_daily.sql` |
| P1 | Investigate the SAFE DEX gap: determine whether SAFE/anchor DEX liquidity genuinely ceased on 2025-11-18 or whether the `>=$1,000` / `>=5-trades` guardrails are too strict for a low-liquidity token. Adjust thresholds or formally accept Dune-only fallback for SAFE. | `int_execution_prices_dex_ratios.sql` |
| P2 | Add a forward-fill staleness monitor (e.g., a dbt test or Elementary alert flagging any symbol whose price has been unchanged for more than N days) so the next SAFE-style stall is caught proactively. | `schema.yml` |
| P2 | Decide and document the USDC/USDT peg-vs-oracle policy: either raise the $1 peg priority above native for hardcoded-stable tokens, or formally adopt oracle market prices and note historical depeg behavior in schema.yml. | `int_execution_token_prices_daily.sql`, `schema.yml` |
| P2 | Update schema.yml: add sGNO to the `int_execution_prices_dex_ratios` scope description; change the hub description from 'view' to 'table (ReplacingMergeTree)'. | `schema.yml` |
| P3 | Add a defensive fallback for sDAI's INNER JOIN on xDAI (e.g., LEFT JOIN with forward-filled xDAI) to prevent a single oracle gap day from dropping sDAI. | `int_execution_prices_native_daily.sql` |
| P3 | Document sGNO~=GNO, BRZ<-BRLA, and forex-peg approximations as explicit consumer caveats in schema.yml; add monitoring for BRLA availability. | `schema.yml`, `int_execution_prices_native_daily.sql` |
| P4 | Remove the redundant `ORDER BY` in the hub final `SELECT` and apply the project `join_use_nulls` convention to the dex_ratios CoW LEFT JOINs for consistency. | `int_execution_token_prices_daily.sql`, `int_execution_prices_dex_ratios.sql` |

---

## Open disagreements

None. Review converged in 1 round with no unresolved challenges.

---

## Review log

| Round | Challenge | Resolution |
|---|---|---|
| 1 | Inspector claim: SAFE price 3.1x overstated. Challenged by requiring warehouse confirmation of hub vs Dune values for same date. | Resolved — warehouse query confirmed hub SAFE=0.3667, Dune SAFE=0.1175 for 2026-06-11; code path traced through `dex_ratios` max date, `native_daily` unbounded forward-fill, and hub priority=1 assignment. |
| 1 | Inspector claim: hub uniqueness test absent. Challenged by requiring schema.yml line-level verification. | Resolved — confirmed tests at lines 125, 174, 245 for sub-layers; hub section (lines 4-56) has no `unique_combination_of_columns` test. |
| 1 | Inspector claim: WxDAI dual priority=1 non-determinism. Challenged by requiring evidence of actual value divergence. | Resolved — historical price divergence of up to 0.4% (before day 18695) confirmed in warehouse query; oracle and CTE both confirmed to enter `all_prices` at priority=1. |
| 1 | Context open question: USDC/USDT peg-vs-oracle priority. Re-examined in verdict. | Resolved as a recorded undocumented-intent finding (not a factual conflict between agents). Priority ordering confirmed: native priority=1 wins over usd_pegs priority=4 when oracle data exists, contradicting the build plan's statement that pegs are canonical. |
