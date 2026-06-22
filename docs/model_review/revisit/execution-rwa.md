# Model review (revisit 2026-06-21): execution/rwa

Baseline: `docs/model_review/execution-rwa.md` (dated `2026-06-11`); `14` cases re-verified over `3` rounds on `2026-06-21`. Headline: all `14` cases remain CONFIRMED with zero resolved/changed — the dominant systemic risk (unbounded forward-fill serving a `bC3M` price now `59` days stale as if live) has only gotten worse since baseline.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONRWA-C01 | — | Unbounded forward-fill serves stale prices as current (no staleness cap, no last_oracle_date col) | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONRWA-C02 | — | `freshness_anomalies` on filled marts is structurally blind to per-ticker oracle staleness | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONRWA-C03 | — | No `(bticker, date)` grain uniqueness test on `fct_`/`api_` marts | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONRWA-C04 | — | Global incremental watermark blocks targeted single-ticker gap-fill | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONRWA-C05 | — | `contracts/backedfi/schema.yml` column docs fabricated across all 9 oracle models | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONRWA-C06 | — | `api_` price declared `UInt64` but warehouse type is `Nullable(Float64)` | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONRWA-C07 | — | `int_` schema describes a Float column as 'unsigned integer with 8 decimal places' | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONRWA-C08 | — | `fct` reads ReplacingMergeTree source without `FINAL` | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONRWA-C09 | — | `fct` `fill_start` hardcoded `2020-01-01`, ~3yr before earliest data | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONRWA-C10 | — | Stale forward-filled price indistinguishable from live NAV for external consumer | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONRWA-C11 | — | Historical coverage gaps yield null/zero USD valuations, no fallback, undisclosed | medium | CONFIRMED | medium | medium | none | 3 |
| EXECUTIONRWA-C12 | — | Semantic layer triple-registers same average-price metric with shared synonyms | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONRWA-C13 | — | USD denomination never asserted; `bC3M` plausibly EUR-denominated | low | CONFIRMED | low | medium | none | 3 |
| EXECUTIONRWA-C14 | — | Warehouse: `bC3M` max_date `2026-04-23`, now 59 days stale; propagates into hub | high | CONFIRMED | high | high | none | 3 |

Rollup: confirmed `14`, resolved `0`, changed `0`, unverifiable `0`, new `0`, still_open `0` (all evidentially sufficient after 3 rounds). No status disputes across any round. No incidents implicated.

## Delta vs baseline

### RESOLVED (0)
None.

### CHANGED (0)
No status or severity changes. Two cases are materially WORSE in magnitude (severity unchanged, defect deeper):

- **EXECUTIONRWA-C01** — `bC3M` flatline grew from baseline `12+ days` to `60` consecutive rows at `126.2` (`2026-04-21`..`2026-06-20`), ~`59` days past the last real print on `2026-04-23`. Code unchanged (`fct` lines 28-32). Incident attribution: `none`.
- **EXECUTIONRWA-C14** — `bC3M` staleness grew from baseline `49` days (`2026-06-11`) to `59` days (`2026-06-21`); hub propagation extended from `2026-06-08` to `2026-06-20` (today-1). Incident attribution: `none` — genuine continuous oracle silence, explicitly excluded from incident-A (month-collapse) and incident-B (decode-window outage).
- **EXECUTIONRWA-C10** — hub stale `126.2` propagation extended from baseline through `2026-06-08` to through `2026-06-20` (today-1). Incident attribution: `none`.

### STILL CONFIRMED (14)
All 14 cases. Proving numbers:

- **C01** `fct` lines 28-32 still `last_value(price) IGNORE NULLS OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`; `bC3M` run-length `60` rows at `126.2`. (`none`)
- **C02** `api_` emits `n_tickers=9` every day over the last 15 days including the 59-day-stalled `bC3M`; `freshness_anomalies` only on `api_` (lines 48-55) / `fct_` (lines 105-112), each with only `timestamp_column: date` and NO `dimensions`/`anomaly_grouping`; `int_` has none. (`none`)
- **C03** `api_`: `rows=7249`, `uniqExact((bticker,date))=7249`, `dupes=0`; `unique_combination_of_columns` exists only on `intermediate/schema.yml` lines 35-40, none on either serving mart. (`none`)
- **C04** `get_incremental_filter.sql` lines 43-47 (insert_overwrite branch) render `toStartOfMonth(toDate(block_timestamp)) >= (SELECT toStartOfMonth(addDays(max(toDate(x1.date)),-N)) FROM {{this}} AS x1 WHERE 1=1)`; `filters_sql=''`, no per-`bticker` `WHERE`/`GROUP BY`. (`none`)
- **C05** `describe_table` of `bC3M`/`bCOIN`/`TSLAx` oracle events all = `decoded_params Map(String,Nullable(String))` + 8 standard log cols; real keys `current`/`roundId`/`updatedAt`; documented `answer`/`oracle_id` (bC3M), `amount_wei`/`token_address` (bCOIN), `event_type`/`event_data` (TSLAx) all absent. (`none`)
- **C06** `marts/schema.yml` line 21 `data_type: UInt64`; warehouse `Nullable(Float64)`; `fct_` line 78 correctly `Float64`; `api_` SQL line 11 is a bare `price` projection (no cast); `check_api_tags.py` lines 86-88 check presence only. (`none`)
- **C07** `int_` schema.yml lines 19-20 `data_type: Float64` with description 'expressed as an unsigned integer with 8 decimal places precision'; model line 41 `argMax(toUInt256OrNull(decoded_params['current'])/POWER(10,8), block_timestamp) AS price`. (`none`)
- **C08** `fct` line 38 reads `int_execution_rwa_backedfi_prices` (RMT, `order_by=(date,bticker)`, `insert_overwrite`) with no `FINAL`; inner `max(price) GROUP BY toDate(date)` collapses transient dupes; grain clean (`7249==uniqExact`). (`none`)
- **C09** `fct` line 13 `fill_start='2020-01-01'`; `WITH FILL FROM toDate('2020-01-01') TO today() STEP 1`; earliest real oracle data `2023-04-01`; `count() WHERE date<'2023-04-01' = 0`; `fct` is `materialized='view'` (compute-only waste). (`none`)
- **C10** hub `int_execution_token_prices_daily` `bC3M` max_date `2026-06-20` (today-1), price `126.2`, `59` rows at `126.2` since `2026-04-23`; schema is only `(date, symbol, price)` — no `last_oracle_date`/`is_stale`/`valid_through`. (`none`)
- **C11** hub min(date): `bCOIN`/`bMSTR`/`bNVDA` `2025-01-01`, `bCSPX` `2024-11-27`; zero rows before oracle start; no `source`/`provider` column; anchored to contract `full_refresh` start_dates. (`none`)
- **C12** `semantic_models.yml` triple-registers `int_`/`api_`/`fct_`, each `average(price)`; metric synonyms all share bare `price` (lines 102,127,152); `api_`/`fct_` share `execution rwa backedfi prices daily` (lines 50,75) and `execution rwa backedfi prices daily price` (lines 126,151); all `quality_tier: candidate`; live `discover_metrics` returned ZERO of the three (silently non-discoverable as candidates). (`none`)
- **C13** no currency assertion in any RWA schema.yml (`api_` 'respective currency units', `fct_` 'numeric value', `int_` none); hub `price` description = 'recorded price of the token in USD' (blanket); `bC3M` is EUR-denominated 3-month French T-bill tracker. (`none`)
- **C14** `int_` FINAL `bC3M` max_date `2026-04-23` (59 days), `rows==uniq_days==914` (contiguous, NOT month-collapsed); `AnswerUpdated` one-per-day `2026-04-15`..`2026-04-23` then STOP, none on incident-B windows `2026-05-30`/`2026-06-14`; all other tickers fresh 1-3 days. (`none` — genuine oracle silence)

### NEW (0)
None.

### UNVERIFIABLE / UNRESOLVED (0)
None. Two cases carry `confidence: medium` rather than high (defect confirmed, one supporting fact not independently re-pinned within budget):

- **C11** — gap *existence* confirmed high; the exact `~23-month` gap *length* rests on baseline's asserted `bCOIN` listing date `2023-02-19` (not re-pinned from a token seed). Confidence medium on length, high on the no-fallback gap itself.
- **C13** — documentation-absence defect fully proven in-repo; the EUR-denomination of `bC3M` was not re-pinned via an external/on-chain anchor this round, so the mislabel-risk premise stays medium confidence. Severity kept low on the documentation-absence basis alone.

## Evidence appendix

**C01 / C10 / C14 (shared `bC3M` staleness queries)**
```sql
-- fct/api run-length
SELECT count(), max(toDate(date)), sum(price=126.2), sum(toDate(date)>='2026-04-23')
FROM api_execution_rwa_backedfi_prices_daily WHERE bticker='bC3M';
-- returns: 1075 rows, max_date epoch 20624 (2026-06-20), 60 rows at 126.2, 59 since 2026-04-23

-- int_ sparse pre-fill grain (FINAL)
SELECT count(DISTINCT toDate(date)) AS real_days, max(toDate(date)) AS last_real
FROM int_execution_rwa_backedfi_prices FINAL WHERE bticker='bC3M' AND toDate(date)>='2026-04-15';
-- returns: real_days 9, last_real 2026-04-23 (one real point on 2026-04-23 amplified into 59 filled rows)

-- hub
SELECT max(date), argMax(price,date), sum(price=126.2)
FROM int_execution_token_prices_daily WHERE symbol='bC3M' AND date>='2026-04-23';
-- returns: max_date 2026-06-20 (today-1), latest_price 126.2, 59 rows at 126.2

-- int_ FINAL per-bticker staleness + contiguity
SELECT bticker, max(toDate(date)) max_date, count(), uniqExact(toDate(date)),
       dateDiff('day', max(toDate(date)), today()) days_stale
FROM int_execution_rwa_backedfi_prices FINAL GROUP BY bticker;
-- bC3M: max 2026-04-23, 914 rows == 914 uniq_days, 59 days stale; others 1-3 days, rows==uniq_days

-- oracle silence
SELECT toDate(block_timestamp) d, count() FROM contracts_backedfi_bC3M_Oracle_events
WHERE event_name='AnswerUpdated' AND toDate(block_timestamp)>='2026-04-15' GROUP BY d ORDER BY d;
-- one event/day 2026-04-15..2026-04-23 then NONE; no event on 2026-05-30 or 2026-06-14
```
Code: `fct` lines 28-32 `last_value(price) IGNORE NULLS OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`, no staleness cap, no `last_oracle_date` column. Hub `describe_table` = `(date, symbol Nullable(String), price Nullable(Float64))` only.

**C02 (freshness blindness)**
```sql
SELECT toDate(date), count(DISTINCT bticker) FROM api_execution_rwa_backedfi_prices_daily
WHERE toDate(date)>=today()-15 GROUP BY 1 ORDER BY 1;
-- n_tickers=9 every day 2026-06-06..2026-06-20, including 59-day-stalled bC3M
```
Code: `marts/schema.yml` `elementary.freshness_anomalies` on `api_` lines 48-55 and `fct_` lines 105-112, each only `timestamp_column: date`, `time_bucket {period: day, count: 1}`, NO `dimensions`/`anomaly_grouping`/`group_by`. `intermediate/schema.yml` has only `schema_changes` + `unique_combination_of_columns`.

**C03 (grain test absence)**
```sql
SELECT count(), uniqExact((bticker,date)), count()-uniqExact((bticker,date))
FROM api_execution_rwa_backedfi_prices_daily;
-- 7249, 7249, 0 (grain-clean)
```
Code: `unique_combination_of_columns([date,bticker])` only on `intermediate/schema.yml` lines 35-40; absent from both `fct_` and `api_` blocks in `marts/schema.yml`.

**C04 (global watermark)** — code only. `int_` line 50 `apply_monthly_incremental_filter('block_timestamp','date','true')` (`'true'` is the `add_and` positional arg, so `filters_sql=''`). `macros/db/get_incremental_filter.sql` lines 43-47 (insert_overwrite branch) render a single global `max(toDate(x1.date))` over `{{this}}` with no per-`bticker` `WHERE`/`GROUP BY`. Compiled artifact was last rendered under the `start_month`/`end_month` var path (`2026-05-01`..`2026-06-01`), so the macro source is the authoritative evidence.

**C05 (fabricated oracle docs)**
```sql
SELECT DISTINCT arrayJoin(mapKeys(decoded_params)) FROM contracts_backedfi_bC3M_Oracle_events WHERE event_name='AnswerUpdated';
-- {current, roundId, updatedAt}
SELECT arrayJoin(mapKeys(decoded_params)) AS k, count() FROM contracts_backedfi_TSLAx_Oracle_events WHERE event_name='AnswerUpdated' GROUP BY k;
-- {current:1127, roundId:1127, updatedAt:1127}
```
`describe_table` for `bC3M`, `bCOIN`, `TSLAx` all = `decoded_params Map(String,Nullable(String))` + `block_number`/`block_timestamp`/`transaction_hash`/`transaction_index`/`log_index`/`contract_address`/`event_name`. Documented `answer`/`oracle_id` (bC3M), `amount_wei`/`token_address` (bCOIN), `event_type`/`event_data` (TSLAx) do not exist. Three models from different event families confirm systemic fabrication across all 9.

**C06 (type lie)** — `describe_table api_execution_rwa_backedfi_prices_daily.price = Nullable(Float64)`; `marts/schema.yml` line 21 `data_type: UInt64`; `fct_` line 78 `Float64`. `api_` SQL line 11: `SELECT bticker, date, price FROM fct_execution_rwa_backedfi_prices_daily` (no cast). `scripts/checks/check_api_tags.py` lines 86-88: `untyped = [c for c,meta in cols.items() if not meta.get('data_type')]`, fails only if non-empty — presence, never correctness.

**C07 (self-contradictory description)** — `intermediate/schema.yml` lines 19-20: `Float64` column described 'expressed as an unsigned integer with 8 decimal places precision'; model line 41 divides by `POWER(10,8)` yielding `Nullable(Float64)`. `fct_` says 'numeric value', `api_` says 'respective currency units' — contradiction isolated to `int_` line 20.

**C08 (RMT without FINAL)** — code only. `fct` line 38 reads `ref('int_execution_rwa_backedfi_prices')` (`engine=ReplacingMergeTree()`, `order_by=(date,bticker)`, `incremental_strategy=insert_overwrite`) with no `FINAL`. Inner `max(price) GROUP BY toDate(date)` per ticker collapses any same-day transient duplicate; `insert_overwrite` REPLACE PARTITION keeps one live version. Residual exposure nil in practice.

**C09 (wasted fill)**
```sql
SELECT min(date) FROM fct_execution_rwa_backedfi_prices_daily;  -- 2023-04-01
SELECT count() FROM fct_execution_rwa_backedfi_prices_daily WHERE date < '2023-04-01';  -- 0
```
`fct` line 13 `fill_start='2020-01-01'`; `WITH FILL FROM toDate('2020-01-01') TO today()` with `WHERE price IS NOT NULL` (line 48). ~`1186` calendar days x `9` tickers ~= `10.7k` transient NULL rows pruned. `fct` is `materialized='view'` — one-shot per-query compute, no storage.

**C11 (coverage gap, no fallback)**
```sql
SELECT symbol, min(date), max(date), count() FROM int_execution_token_prices_daily
WHERE symbol IN ('bCOIN','bMSTR','bNVDA','bCSPX') GROUP BY symbol;
-- bCOIN/bMSTR/bNVDA min 2025-01-01; bCSPX min 2024-11-27; zero rows before
```
`describe_table` hub = `(date, symbol, price)` only — no `source`/`provider` column, so no Dune fallback row is distinguishable. Oracle-start dates anchored to `models/contracts/backedfi/schema.yml` `full_refresh` start_dates.

**C12 (semantic collision + non-discoverability)** — code only + live resolver. `semantic_models.yml` registers `int_` (lines 2-26), `api_` (27-51), `fct_` (52-76), each `average(price)` measure (lines 16,41,66). Metric synonyms share bare `price` (lines 102,127,152); `api_`/`fct_` share `execution rwa backedfi prices daily` (lines 50,75) and `execution rwa backedfi prices daily price` (lines 126,151). All three `quality_tier: candidate`. Live `discover_metrics` for the exact synonym returned ZERO of the three (top hits `circles_v2_crc20_prices` and unrelated approved-tier metrics) — reconciling the round-2 contradiction into a twofold defect: latent YAML synonym collision (surfaces on promotion) + silent non-discoverability as candidates. `api_` is a thin `SELECT` from `fct_`.

**C13 (no currency assertion)** — code only. RWA `marts/schema.yml`/`intermediate/schema.yml` price descriptions carry no currency; hub `describe_table` price = 'recorded price of the token in USD' (blanket). `bC3M` tracks the EUR-denominated 3-month French government bond (C3M/BTF index). EUR denomination not re-pinned externally this round.

## Review log (>=3 rounds per case)

- **C01** R1 CONFIRMED (code: fct lines 28 forward-fill; run_len 60) -> challenge: prove the fill not the source is the cause via sparse pre-fill grain -> R2 CONFIRMED (int_ real_days=9 / last_real 2026-04-23 vs 59 filled rows) -> challenge: prove window frame is UNBOUNDED PRECEDING..CURRENT ROW -> R3 CONFIRMED (quoted exact window frame, per-ticker UNION branch, no date-distance guard). high throughout.
- **C02** R1 CONFIRMED (freshness only on marts) -> challenge: demonstrate blindness empirically -> R2 CONFIRMED (9 tickers every day for 15 days) -> challenge: show no per-bticker grouping in test config -> R3 CONFIRMED (only timestamp_column:date, no dimensions/anomaly_grouping anywhere). high throughout.
- **C03** R1 CONFIRMED (grain clean, no test on serving marts) -> challenge: confirm api_ also clean + untested -> R2 CONFIRMED (api_ 7249==uniqExact, zero hits) -> challenge: confirm int_ test cannot catch downstream dupes -> R3 CONFIRMED (only test upstream of the fill). medium throughout.
- **C04** R1 CONFIRMED (macro global max(date)) -> challenge: quote compiled watermark -> R2 CONFIRMED (insert_overwrite branch, no GROUP BY bticker) -> challenge: read compiled artifact -> R3 CONFIRMED (compiled rendered under var path; macro source is authoritative, single global predicate). medium throughout.
- **C05** R1 CONFIRMED (bC3M describe_table = Map; docs fabricated) -> challenge: spot-check keys + bCOIN -> R2 CONFIRMED (bC3M keys current/roundId/updatedAt; bCOIN Map) -> challenge: third model (TSLAx) -> R3 CONFIRMED (TSLAx Map, event_type/event_data absent; systemic across 9). medium throughout.
- **C06** R1 CONFIRMED (api_ UInt64 vs Nullable(Float64)) -> challenge: quote CI guard lines -> R2 CONFIRMED (check_api_tags.py presence-only) -> challenge: confirm no runtime cast -> R3 CONFIRMED (api_ SQL bare projection; doc/contract drift only). low throughout.
- **C07** R1 CONFIRMED (int_ line 20 'unsigned integer' on Float64) -> challenge: confirm not leaked to marts -> R2 CONFIRMED (fct_/api_ differ; isolated to int_) -> challenge: pin description-vs-column mismatch via model line 41 -> R3 CONFIRMED (toUInt256OrNull/POWER(10,8); internal contradiction). low throughout.
- **C08** R1 CONFIRMED (no FINAL on RMT source) -> challenge: prove GROUP BY date collapses dupes -> R2 CONFIRMED (inner max(price) GROUP BY date) -> challenge: confirm cross-day residual nil -> R3 CONFIRMED (insert_overwrite keeps one live version; nil in practice). low throughout.
- **C09** R1 CONFIRMED (fill_start 2020-01-01) -> challenge: quantify waste, prove output correct -> R2 CONFIRMED (zero rows before 2023-04-01) -> challenge: confirm view-build compute-only cost -> R3 CONFIRMED (materialized=view, ~10.7k pruned NULL rows, no storage). low throughout.
- **C10** R1 CONFIRMED (hub serves 126.2 to today-1, no flag) -> challenge: confirm no staleness col anywhere -> R2 CONFIRMED (hub/marts = (symbol/bticker,date,price) only) -> challenge: trace one downstream consumer -> R3 CONFIRMED (hub fed by fct via backedfi CTE priority 2; any USD-valuation consumer multiplies by stale 126.2). high throughout.
- **C11** R1 CONFIRMED (hub coverage starts at oracle-start, no rows before) -> challenge: confirm zero Dune fallback rows -> R2 CONFIRMED (no source/provider column, zero pre-oracle rows) -> challenge: pin listing date on-chain -> R3 CONFIRMED (gap existence high; exact length rests on baseline listing date, confidence medium). medium throughout.
- **C12** R1 CONFIRMED (triple registration, shared 'price') -> challenge: run resolver to show >1 returned -> R2 CONFIRMED-but-INSUFFICIENT (resolver returned ZERO of the three; contradiction flagged) -> challenge: reconcile resolver behavior + quality_tier -> R3 CONFIRMED (all candidate-tier, filtered from dispatch; twofold defect = latent collision + silent non-discoverability). medium throughout.
- **C13** R1 CONFIRMED (no currency in RWA schema; hub blanket USD) -> challenge: pin EUR via external anchor -> R2 CONFIRMED (load-bearing absence proven; EUR not re-pinned, kept low) -> challenge: fetch Backed.fi/contract metadata -> R3 CONFIRMED (documentation-absence proven in-repo; EUR not re-pinned, confidence medium, severity low). low throughout.
- **C14** R1 CONFIRMED (bC3M 59 days, contiguous 914 rows, excludes incidents) -> challenge: confirm contiguity has no holes -> R2 CONFIRMED (914==uniqExact over 1017-day span = weekend/holiday pattern) -> challenge: nail incident-B exclusion with precise gap dates -> R3 CONFIRMED (events stop exactly 2026-04-23, none on 2026-05-30/2026-06-14; genuine continuous silence). high throughout; magnitude grew 49->59 days.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P0 (ESCALATE) | Add a staleness cap and a `last_oracle_date` column to the forward-fill; stop carrying the last real print indefinitely. `bC3M` has served `126.2` as current for `59` days. Until fixed, gate or flag any price older than N days. | `models/execution/rwa/marts/fct_execution_rwa_backedfi_prices_daily.sql` |
| P0 (ESCALATE) | Expose a staleness signal (`last_oracle_date`/`is_stale`/`valid_through`) on the serving path so external consumers and the price hub can distinguish a stale fill from a live NAV. | `fct_`, `api_execution_rwa_backedfi_prices_daily.sql`, `models/execution/prices/intermediate/int_execution_token_prices_daily.sql` |
| P0 (ESCALATE) | Assert per-`bticker` freshness/recency on the `int_` source (pre-fill grain), or add a `dimensions: [bticker]` grouping to `freshness_anomalies` — current mart-level test is structurally blind. | `models/execution/rwa/intermediate/schema.yml`, `models/execution/rwa/marts/schema.yml` |
| P1 (KEEP) | Operationally resolve the genuine `bC3M` oracle silence (no `AnswerUpdated` events since `2026-04-23`) — source feed appears dead; not an incident artifact. | upstream Backed.fi `bC3M` oracle / `contracts/backedfi` |
| P1 (KEEP) | Replace the fabricated column docs across all 9 backedfi oracle models with the real `decoded_params Map(String,Nullable(String))` + standard log columns. | `models/contracts/backedfi/schema.yml` |
| P2 (KEEP) | Add `dbt_utils.unique_combination_of_columns([date, bticker])` to both serving marts; a downstream grain break is currently undetectable. | `models/execution/rwa/marts/schema.yml` |
| P2 (KEEP) | Allow per-`bticker` scoping (or per-partition full-refresh) for targeted single-ticker gap-fill; the global `max(date)` watermark blocks re-pulling one stalled ticker. | `models/execution/rwa/intermediate/int_execution_rwa_backedfi_prices.sql`, `macros/db/get_incremental_filter.sql` |
| P2 (KEEP) | De-duplicate the triple-registered `average(price)` metric / shared synonyms before any of the three is promoted from candidate; also surface or retire the silently non-discoverable candidates. | `semantic/authoring/execution/rwa/semantic_models.yml` |
| P2 (KEEP) | Disclose/handle pre-oracle coverage gaps (`bCOIN`/`bMSTR`/`bNVDA` from `2025-01-01`, `bCSPX` from `2024-11-27`) — no hub price and no Dune fallback before oracle start. | `models/execution/prices/intermediate/int_execution_token_prices_daily.sql`, `int_execution_rwa_backedfi_prices.sql` |
| P3 (KEEP) | Fix the `api_` price `data_type: UInt64` -> `Float64`; extend `check_api_tags.py` to validate type correctness, not just presence. | `models/execution/rwa/marts/schema.yml`, `scripts/checks/check_api_tags.py` |
| P3 (KEEP) | Correct the `int_` price description ('unsigned integer with 8 decimal places' on a `Float64`) and add a currency assertion (note `bC3M` is EUR-denominated; hub blanket-labels USD). | `models/execution/rwa/intermediate/schema.yml`, `models/execution/rwa/marts/schema.yml` |
| P3 (KEEP) | Lower `fill_start` waste: set the `WITH FILL` start to ~`2023-04-01` instead of `2020-01-01` (output already correct; compute-only saving). | `models/execution/rwa/marts/fct_execution_rwa_backedfi_prices_daily.sql` |
| P3 (KEEP) | Consider `FINAL` (or document the bounded-safe argument) on the `fct` read of the RMT `int_` source; residual exposure currently nil via `insert_overwrite` + inner `max(price)`. | `models/execution/rwa/marts/fct_execution_rwa_backedfi_prices_daily.sql` |

No DROP recommendations — zero cases resolved since baseline.
