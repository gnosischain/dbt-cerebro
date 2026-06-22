# Model review (revisit 2026-06-21): execution/cow

Baseline: `docs/model_review/execution-cow.md` (2026-06-11); 19 cases re-verified over 4 rounds. Headline: the P0-10 fee/solver-value ingestor outage is fully recovered (`42d -> 5d` stale) and 3 implementation gaps closed, but the incremental-lookback-mismatch footgun (C04/C05) is now demonstrated actively corrupting solver attribution and `cow_ratio` on current-month data, and the semantic column-drift break (C11) still fails two API-facing metrics at query time — 4 RESOLVED, 0 CHANGED, 15 STILL CONFIRMED, 0 NEW.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONCOW-C01 | P0-10 | `cow_api_trade_fees` 42d stale; fee KPIs NULL | critical | RESOLVED | resolved | high | other (P0-10) | 4 |
| EXECUTIONCOW-C02 | P0-10 | no source-freshness threshold on `cow_api_trade_fees` | high | RESOLVED | resolved | high | none | 3 |
| EXECUTIONCOW-C03 | | api_/fct_ marts + 2 staging lack `production` tag | high | RESOLVED | resolved | high | none | 3 |
| EXECUTIONCOW-C04 | | `int_execution_cow_trades` settlements 3-day lookback vs monthly trades filter -> NULL solver | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONCOW-C05 | | `int_execution_cow_batches` interactions 3-day lookback -> spurious is_cow, inflated cow_ratio | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONCOW-C06 | | `cow_ratio` returns 0 not NULL on LEFT-JOIN miss | medium | CONFIRMED | low | high | none | 3 |
| EXECUTIONCOW-C07 | | two fee/solver paths diverge on negative corrections | medium | CONFIRMED | low | high | none | 3 |
| EXECUTIONCOW-C08 | | `kpi_active_solvers` missing `window:7d` tag | medium | CONFIRMED | low | high | none | 3 |
| EXECUTIONCOW-C09 | | 0.6% all-time trades have NULL `amount_usd` | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONCOW-C10 | | symbol-keyed ASOF price join is collision-vulnerable | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONCOW-C11 | P0-10 | semantic model references columns absent from mart | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONCOW-C12 | | pre-Sep-2024 onchain fee overstates revenue (caveat) | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONCOW-C13 | | 4 auto-gen candidate metrics marked `quality_tier: approved` | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONCOW-C14 | | `cow_active_solvers` uses `agg: avg` (daily mean), undocumented | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONCOW-C15 | | solver_value has marts+KPI but no semantic metric | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONCOW-C16 | | Partial CoW threshold may under-credit peer matching | low | CONFIRMED | low | medium | none | 3 |
| EXECUTIONCOW-C17 | | ETH-flow orders not asserted/documented | low | RESOLVED | low | high | none | 3 |
| EXECUTIONCOW-C18 | | grain integrity: 0 dup keys, num_trades reconciles | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONCOW-C19 | | core trade/volume pipeline current | low | CONFIRMED | low | high | none | 3 |

Rollup: 15 CONFIRMED, 4 RESOLVED (C01, C02, C03, C17), 0 CHANGED, 0 UNVERIFIABLE, 0 NEW.

## Delta vs baseline

### RESOLVED (4)

- **C01 (P0-10, critical -> resolved):** the `crawlers_data.cow_api_trade_fees` ingestor recovered from `max(ingested_at)=2026-04-30` (42 days stale) to `2026-06-20` (`days_stale=5`); `fee_source='api'` coverage rose from `0.18%` to `54.69%` (`22,344 / 40,857` last 30d); both 7d KPIs are no longer NULL (`kpi_fees_7d=90.37`, `kpi_solver_value_7d=24.02`). The remaining `~4`-day trailing NULL-fee window in `fct_execution_cow_daily` is the expected steady-state ingest cadence (not creeping: `days_stale` stable ~5, NULL tail stable at 4 across rounds), not a regression. Incident: P0-10 ingestor fix.
- **C02 (high -> resolved):** `models/crawlers_data/sources.yml` L67-70 now defines `freshness: warn_after {36,hour} / error_after {48,hour}` on `cow_api_trade_fees` with `loaded_at_field: ingested_at`. No source-level suppression; `DBT_COW_FEES_SCHEMA` defaults to `crawlers_data`; wired into the cron run via `scripts/run_dbt_observability.sh` L154-156 (`dbt source freshness --select source:*`) and `cron.sh` L5 MANDATORY_STEPS includes `source-freshness`. At 5d (`~120h` > 48h) this would ERROR to an operator today.
- **C03 (high -> resolved):** every one of the 14 `api_execution_cow_*` marts, the 4 `fct_` marts, and both `stg_cow__solvers.sql` (L3) and `stg_cow__interactions.sql` (L3) now carry the `production` tag (grep-verified across all 18 marts + 4 staging models; no parsed manifest available read-only, stated explicitly — grep-based RESOLVED stands).
- **C17 (low -> resolved):** ETH-flow orders ARE captured — `421` of `432` EthFlow `OrderPlacement` events in the last 30d resolve to Trade rows in `int_execution_cow_trades` with `taker=0xba3cb449bd2b4adddbc894d8697f5170800eadec` (the EthFlow router); placement!=settlement, so the tx-hash mismatch is expected. No coverage gap. Documentation gap (no ETH-flow note in the cow SQL/schema) remains.

### CHANGED (0)

None. (C01 was CHANGED in Round 1, then settled to RESOLVED once the recovery proved durable.)

### STILL CONFIRMED (15)

- **C04 (high):** `models/execution/cow/intermediate/int_execution_cow_trades.sql` L62 settlements sub-query still uses `WHERE block_timestamp >= (SELECT addDays(max(toDate(block_timestamp)), -3) FROM {{ this }})` while the trades CTE (L46) uses `apply_monthly_incremental_filter`. Reproduced biting on current-month data: `11` contiguous days (`06-10..06-19`) at 100% NULL solver (e.g. `06-14: 2376/2376`), only `06-20..06-24` recovered. Propagates downstream: `fct_execution_cow_solvers_daily` has ZERO per-solver rows for `06-10..06-19`, and `fct_execution_cow_daily.active_solvers=0` for those days vs `6-8` for the settled tail. Incident: none (the `addDays(-3)` lookback footgun, NOT the June insert_overwrite-wipe incident).
- **C05 (high):** `models/execution/cow/intermediate/int_execution_cow_batches.sql` L42 interactions sub-query still uses `addDays(max(toDate(block_timestamp)), -3)`; `is_cow = coalesce(i.num_interactions,0)=0 AND bt.num_trades>1` (L83). Reproduced active inflation: for `06-10..06-19` every batch has `num_interactions=0` (`coalesce(NULL,0)=0`) and every multi-trade batch flips `is_cow=TRUE` (`cow_n == multi` each day, e.g. `06-14 cow_n=355`), inflating `fct_execution_cow_daily.cow_ratio` to `0.08-0.18` vs `0.0` for the settled tail. (The Round-2 verifier medium-downgrade was overridden back to high after data proof.) Incident: none (same `addDays(-3)` footgun as C04).
- **C06 (medium -> low):** `models/execution/cow/marts/fct_execution_cow_daily.sql` L71 `if(b.num_batches > 0, ..., 0)` still returns 0 (not NULL) on a LEFT-JOIN miss — reachable in code, but unreachable in data: `0` days all-history have `num_trades>0 AND (num_batches IS NULL OR 0) AND cow_ratio=0`. Protection is structural: `batch_daily` and `trade_daily` both aggregate the same `int_execution_cow_trades` universe, so every trade-bearing day necessarily has a batch row.
- **C07 (medium -> low):** `api_execution_cow_fees_ts.sql` L23 (`fee_usd>0`) / `api_execution_cow_solver_value_ts.sql` L21 (`solver_value_usd>0`) still differ from `fct_execution_cow_daily.sql` L28-29 unfiltered `sumIf(...,fee_source='api')`. But `0` non-positive api rows exist across all `813,702` `fee_source='api'` rows (`min=1.08e-20`), so the filter is a no-op; both paths sum to identical `3807.98` over a real 30-day window.
- **C08 (medium -> low):** `api_execution_cow_kpi_active_solvers.sql` L4-5 carries `granularity:last_7d` but not `window:7d`; `5` of `6` peer 7d KPIs carry it. Downgraded to low — `check_api_tags.py` does not enforce `window:` tags and no consumer filters by it (cosmetic tag-consistency gap, not a silent drop).
- **C09 (low):** `int_execution_cow_trades` has `16,253` NULL `amount_usd` of `2,702,295` rows (`0.601%`), concentrated pre-2026 (by year: `2678/2627/1448/1455/8000/45` for 2021-2026); last-30d only `2` of `40,857`. NULL rows contribute 0 to `sum(amount_usd)` volume — bounded historical understatement, not a recent regression.
- **C10 (low):** ASOF price join in `int_execution_cow_trades.sql` L85/L105 is still keyed `ON pb.symbol = s.token_bought_symbol` (symbol, not address). Only `2` colliding symbols in `stg_pools__tokens_meta` (4 addresses); the sole material CoW-traded collider is EURe (2 addresses, `~$14M` 90d), a EUR stablecoin whose `int_execution_token_prices_daily` feed returns exactly 1 distinct price per day (`rel_spread=0`), so the collision cannot materially mis-value it. No address-based guardrail added.
- **C11 (high):** `api_execution_cow_top_pairs_weekly.sql` emits only `date/label/value` while `semantic/authoring/execution/cow/semantic_models.yml` (L56-79) references `week/pair/volume_usd/num_trades`. Both bound metrics `cow_top_pairs_volume` and `execution_cow_pair_trades_value` are `quality_tier: approved` and selectable in `discover_metrics` (score 80) and fail at query time with column-not-found. (Live bind error uncapturable read-only: `reload_semantic_registry` did not clear `manifest_hash_mismatch`, `execution_available=false`; static column-diff + selectable-in-registry settles high.)
- **C12 (high, documented caveat):** cutover pinned — first `fee_source='api'` date = `2024-09-25` (`813,702` api rows). Every revenue-summing path hard-filters `fee_source='api'` (`fct_execution_cow_daily.sql` L28-29, `api_execution_cow_fees_ts.sql` L22, `api_execution_cow_solver_value_ts.sql` L21, `kpi_fees_7d` L19/L26, semantic `cow_fees_usd` via the already-api-gated `fees_usd`). Caveat documented in `schema.yml` (L67/L89/L310/L327). The gate must never be removed.
- **C13 (medium):** `semantic_models.yml` — `execution_cow_batches_value` (L267-290), `execution_cow_cow_batches_value` (L291-314), `execution_cow_gas_native_value` (L315-338), `execution_cow_pair_trades_value` (L339-362) all `quality_tier: approved` with description `Auto-generated candidate metric; review and promote before relying on it.` All four surface as approved/selectable in `discover_metrics`. Three bind to real columns (so consumers get real numbers from unvetted metrics); `execution_cow_pair_trades_value` is doubly broken (also a C11 victim).
- **C14 (medium):** `semantic_models.yml` L38-40 measure `execution_cow_active_solvers_value agg: avg, expr: active_solvers` = mean of daily `countDistinct(solver)`, diverging from the period-distinct 7d KPI. Quantified: `5.0`/day (7d) and `4.1`/day (30d) vs period `uniqExact(solver)` = `8` (7d) and `10` (30d) — a `>2x` understatement. Undocumented; metric `cow_active_solvers` binds to it and is selectable.
- **C15 (medium):** no `solver_value` measure or metric at ANY tier in `semantic_models.yml`; `discover_metrics('cow solver value usd')` returns no solver-value metric, despite `api_execution_cow_solver_value_ts` and `api_execution_cow_kpi_solver_value_7d` existing as marts. MCP consumers cannot answer solver-value questions via the semantic layer.
- **C16 (low):** `api_execution_cow_batch_routing_ts.sql` L19-22 still classifies Partial CoW only when `num_interactions < num_trades`. `3,337` of `5,408` multi-trade batches (`61.7%`, 90d excluding the 7-day unsettled tail) have `num_interactions >= num_trades` and are labelled Pure DEX. Severity hinges on an unconfirmed business definition; docs not authoritative read-only — flagged for owner confirmation.
- **C18 (low, healthy positive):** `0` duplicate `(block_timestamp, transaction_hash, log_index)` keys over the full May 2026 partition; `fct_execution_cow_daily.num_trades` reconciles diff=0 to `fct_execution_cow_trades` across all 31 days (`50,248` rows). Grain integrity holds at the month boundary — `insert_overwrite` is not dropping/duplicating rows.
- **C19 (low, healthy positive):** `fct_execution_cow_daily` `max(date)=2026-06-25` (tracks the data clock), `90` distinct days = 90-day span (fully contiguous, no internal holes); `volume_usd`, `num_batches`, `unique_traders` all non-NULL/non-zero across the last 14 days. Only affected core measures are `fees_usd`/`solver_value_usd` (C01 tail) and `active_solvers`/`cow_ratio` (C04/C05).

### NEW (0)

None.

### UNVERIFIABLE / UNRESOLVED (0)

None. (C17 was UNVERIFIABLE in Round 2 — the taker-based test returned 0 router-as-taker rows and could not distinguish capture from absence — then RESOLVED in Round 3 once the OrderPlacement->Trade trace proved capture.)

## Evidence appendix

### C01 — ingestor staleness & fee KPIs
```sql
SELECT max(ingested_at), dateDiff('day', max(toDate(ingested_at)), today()) FROM crawlers_data.cow_api_trade_fees;
-- max(ingested_at)=2026-06-20, days_stale=5, 5,927,255 rows (was 2026-04-30 / 42d)
SELECT count(*), countIf(fee_source='api'), round(countIf(fee_source='api')*100.0/count(*),3)
  FROM dbt.fct_execution_cow_trades WHERE toDate(block_timestamp)>=today()-30 AND toDate(block_timestamp)<today();
-- 40,857 total; 22,344 api = 54.69% (was 0.18%)
SELECT date, fees_usd, solver_value_usd FROM dbt.fct_execution_cow_daily WHERE date >= today()-16;
-- populated through 2026-06-20; exactly 4 trailing NULL-fee days (expected api ingest cadence)
-- KPIs: kpi_fees_7d value=90.37, kpi_solver_value_7d value=24.02 (no longer NULL)
```

### C02 — source freshness threshold (code)
`models/crawlers_data/sources.yml` L67-70: `loaded_at_field: ingested_at`, `freshness: warn_after {36,hour} / error_after {48,hour}`. `scripts/run_dbt_observability.sh` L154-156 runs `dbt source freshness --select source:*`; `cron.sh` L5 MANDATORY_STEPS includes `source-freshness`. No source-level suppression on `crawlers_data_cow`; `DBT_COW_FEES_SCHEMA` defaults to `crawlers_data`.

### C03 — production tags (code)
grep of `tags=` across `models/execution/cow`: all 14 `api_execution_cow_*` marts + `fct_execution_cow_daily` (L13), `fct_execution_cow_trades` (L16), `fct_execution_cow_solvers`/`solvers_daily`, `stg_cow__solvers` (L3), `stg_cow__interactions` (L3), `stg_cow__trades`, `stg_cow__settlements` all carry `production`.

### C04 / C05 — incremental-lookback-mismatch (shared)
```sql
SELECT toDate(block_timestamp) d, count(*), countIf(solver IS NULL OR solver='')
  FROM dbt.int_execution_cow_trades WHERE block_timestamp>=today()-16 GROUP BY d;
-- C04: 100% NULL solver for 06-10..06-19 (11 contiguous days; 06-14: 2376/2376), 0% for 06-20..06-24
SELECT date, countDistinct(solver), sum(num_batches) FROM dbt.fct_execution_cow_solvers_daily WHERE date>=today()-16 GROUP BY date;
-- C04 downstream: ZERO rows for 06-10..06-19; fct_execution_cow_daily.active_solvers=0 those days vs 6-8 settled
SELECT toDate(block_timestamp) d, count(*), countIf(is_cow), countIf(num_trades>1), countIf(num_interactions=0), avg(num_interactions)
  FROM dbt.int_execution_cow_batches WHERE block_timestamp>=today()-16 GROUP BY d;
-- C05: 06-10..06-19 num_interactions=0 for ALL batches, cow_n==multi (every multi-trade batch is_cow=TRUE); 06-20..06-24 avg_int~2.7, cow_n=0
SELECT date, cow_ratio FROM dbt.fct_execution_cow_daily WHERE date>=today()-16;
-- C05 downstream: cow_ratio 0.08-0.18 for 06-10..06-19 vs 0.0 for settled tail
```
Code: `int_execution_cow_trades.sql` L46 (monthly filter) vs L62 (`addDays(max(toDate(block_timestamp)),-3)`); `int_execution_cow_batches.sql` L29 (monthly) vs L42 (`addDays(...,-3)`), L83 `is_cow = coalesce(i.num_interactions,0)=0 AND bt.num_trades>1`.

### C06 — cow_ratio LEFT-JOIN miss
```sql
SELECT count(*) FROM dbt.fct_execution_cow_daily WHERE num_trades>0 AND (num_batches IS NULL OR num_batches=0) AND cow_ratio=0;
-- 0 days all-history (defect reachable in code at fct_execution_cow_daily.sql L71 if(b.num_batches>0,...,0), unreachable in data)
```

### C07 — two-path positivity divergence
```sql
SELECT countIf(fee_source='api' AND fee_usd<=0), min(fee_usd), count(*) FROM dbt.fct_execution_cow_trades WHERE fee_source='api';
-- 0 non-positive of 813,702 api rows; min=1.08e-20
SELECT round(sum(value),2) FROM dbt.api_execution_cow_fees_ts WHERE date>=today()-37 AND date<today()-7;        -- 3807.98
SELECT round(sum(fees_usd),2) FROM dbt.fct_execution_cow_daily WHERE date>=today()-37 AND date<today()-7;       -- 3807.98 (diff=0)
```

### C08 — missing window:7d tag (code)
`api_execution_cow_kpi_active_solvers.sql` L4-5: `tags=['production','execution','cow','kpi','tier0','api:cow_kpi_active_solvers','granularity:last_7d']` — no `window:7d`. 5 of 6 peer 7d KPIs (fees_7d, volume_7d, trades_7d, solver_value_7d, traders_7d) carry it.

### C09 — NULL amount_usd
```sql
SELECT toYear(block_timestamp) y, countIf(amount_usd IS NULL), count(*) FROM dbt.int_execution_cow_trades GROUP BY y;
-- all-time 16,253 NULL of 2,702,295 (0.601%); by year 2021-2026: 2678/2627/1448/1455/8000/45
SELECT countIf(amount_usd IS NULL AND block_timestamp>=today()-30), countIf(block_timestamp>=today()-30) FROM dbt.int_execution_cow_trades;
-- last-30d: 2 NULL of 40,857
```

### C10 — symbol-keyed price join
```sql
SELECT token, count(DISTINCT token_address) FROM dbt.stg_pools__tokens_meta WHERE token='EURe' GROUP BY token;  -- 2 addresses
SELECT date, count(DISTINCT price), (max(price)-min(price))/min(price) rel_spread
  FROM dbt.int_execution_token_prices_daily WHERE symbol='EURe' AND date>=today()-30 GROUP BY date;
-- exactly 1 distinct price/day, rel_spread=0 across 30d (e.g. 06-25 price 1.147)
```
Code: `int_execution_cow_trades.sql` L85 `ON pb.symbol = s.token_bought_symbol`, L105 `ON ps.symbol = s.token_sold_symbol`.

### C11 — semantic column drift (code + registry)
`api_execution_cow_top_pairs_weekly.sql` outputs `date/label/value` (L24-33). `semantic/authoring/execution/cow/semantic_models.yml` L56-79 references `week` (L66), `pair` (L71), `volume_usd` (L75), `num_trades` (L78). `discover_metrics` surfaces `cow_top_pairs_volume` and `execution_cow_pair_trades_value` (score 80) as approved/selectable. Live execution blocked: `reload_semantic_registry` -> `manifest_hash_mismatch`, `execution_available=false`.

### C12 — fee cutover & api-gate
```sql
SELECT min(toDate(block_timestamp)) FROM dbt.fct_execution_cow_trades WHERE fee_source='api';  -- 2024-09-25 (813,702 api rows)
-- onchain fee_usd 2023-06..2024-09-24 = $70; api fee_usd 2024-09-25..2025 = $282,954
```
Every revenue path gates `fee_source='api'`: `fct_execution_cow_daily.sql` L28-29, `api_execution_cow_fees_ts.sql` L22, `api_execution_cow_solver_value_ts.sql` L21, `kpi_fees_7d` L19/L26.

### C13 — candidate metrics approved (code + registry)
`semantic_models.yml`: `execution_cow_batches_value` (L267-290), `execution_cow_cow_batches_value` (L291-314), `execution_cow_gas_native_value` (L315-338), `execution_cow_pair_trades_value` (L339-362) — all `quality_tier: approved` + candidate disclaimer. `discover_metrics` returns all four selectable (pair_trades_value score 80).

### C14 — active_solvers agg:avg divergence
```sql
SELECT avg(active_solvers) FROM dbt.fct_execution_cow_daily WHERE date>=today()-7;   -- 5.0/day  (7d)
SELECT avg(active_solvers) FROM dbt.fct_execution_cow_daily WHERE date>=today()-30;  -- 4.1/day  (30d)
SELECT uniqExact(solver) FROM dbt.fct_execution_cow_solvers_daily WHERE num_batches>0 AND date>=today()-7;   -- 8  (7d)
SELECT uniqExact(solver) FROM dbt.fct_execution_cow_solvers_daily WHERE num_batches>0 AND date>=today()-30;  -- 10 (30d)
```
Code: `semantic_models.yml` L38-40 `execution_cow_active_solvers_value agg: avg`.

### C15 — solver_value semantic gap
`discover_metrics('cow solver value usd')` returns no solver-value metric (top hits `cow_active_solvers`/`cow_fees_usd`/`cow_volume_usd`). grep of `semantic_models.yml`: no `solver_value` measure/metric at any tier. Marts `api_execution_cow_solver_value_ts`, `api_execution_cow_kpi_solver_value_7d` exist unregistered.

### C16 — Partial CoW threshold
```sql
SELECT countIf(num_trades>1 AND num_interactions>0 AND num_interactions>=num_trades), countIf(num_trades>1)
  FROM dbt.int_execution_cow_batches WHERE block_timestamp>=today()-90 AND block_timestamp<today()-7;
-- 3,337 of 5,408 multi-trade batches (61.7%) labelled Pure DEX while num_interactions>=num_trades
```
Code: `api_execution_cow_batch_routing_ts.sql` L19-22 (`num_interactions < num_trades` => Partial CoW).

### C17 — ETH-flow capture
```sql
SELECT countIf(lower(replaceAll(taker,'0x',''))='ba3cb449bd2b4adddbc894d8697f5170800eadec'), count(*)
  FROM dbt.int_execution_cow_trades WHERE block_timestamp>=today()-30;  -- 421 EthFlow-router-taker trades
SELECT count(*) FROM dbt.contracts_CowProtocol_CoWSwapEthFlow_events WHERE event_name='OrderPlacement' AND block_timestamp>=today()-30;  -- 432 placements
-- 421/432 placements resolve to Trade rows -> EthFlow IS captured (under router as taker)
```

### C18 — grain integrity
```sql
SELECT count(*)-uniqExact(block_timestamp,transaction_hash,log_index)
  FROM dbt.int_execution_cow_trades WHERE toStartOfMonth(block_timestamp)='2026-05-01';  -- 0 dup keys (full May)
-- fct_execution_cow_daily.num_trades sum for May (50,248 / 31 days) - fct_execution_cow_trades May count = 0
```

### C19 — currency & contiguity
```sql
SELECT max(date), countDistinct(date), countIf(volume_usd IS NULL OR volume_usd=0),
       countIf((num_batches IS NULL OR num_batches=0) AND date>=today()-14),
       countIf((unique_traders IS NULL OR unique_traders=0) AND date>=today()-14)
  FROM dbt.fct_execution_cow_daily WHERE date>=today()-90;
-- max(date)=2026-06-25; 90 distinct=90-day span; volume_usd 0 bad over 90d; num_batches/unique_traders 0 bad over 14d
```

## Review log (>=3 rounds per case)

- **C01:** R1 CHANGED (42d->5d, KPIs recovered but trailing NULL tail) -> challenge: quantify steady-state ingest lag (is the ~4-day tail expected or widening?). R2 RESOLVED-low (days_stale=5, 54.69% api, 4 trailing NULL days = expected cadence) -> challenge: confirm durability vs one-time backfill. R3 RESOLVED (days_stale stable ~5, NULL tail stable at 4, not creeping). R4 held RESOLVED.
- **C02:** R1 RESOLVED (warn 36h/error 48h block exists) -> challenge: confirm it fires (no suppression, default schema, would ERROR at 5d). R2 RESOLVED (test wired, would ERROR) -> challenge: confirm in actual cron/CI path. R3 RESOLVED (`run_dbt_observability.sh` L154-156 + `cron.sh` MANDATORY_STEPS). R4 held.
- **C03:** R1 RESOLVED (all named models tagged production) -> challenge: run check_api_tags.py / `dbt ls` for manifest-level proof. R2 RESOLVED (exhaustive grep; manifest not runnable read-only) -> challenge: run against any parsed manifest. R3 RESOLVED (no manifest available read-only, stated; grep-based stands). R4 held.
- **C04:** R1 CONFIRMED code_only -> challenge: demonstrate blast radius with data. R2 CONFIRMED-high (06-01..06-15 100% NULL solver after early-month rerun) -> challenge: show propagation to consumer surfaces. R3 CONFIRMED-high (11 contiguous all-NULL days 06-10..06-19; solvers_daily zero rows; active_solvers=0). R4 held.
- **C05:** R1 CONFIRMED code_only -> challenge: prove inflation on data or downgrade. R2 verifier downgraded to medium ("no current inflation observed") -> orchestrator OVERRIDE: 2,225 is_cow batches all `num_interactions=0`, restore high. R3 CONFIRMED-high (every early-June multi-trade batch spuriously is_cow=TRUE; cow_ratio 0.08-0.18 vs 0). R4 held.
- **C06:** R1 CONFIRMED medium (if(...,0) on LEFT JOIN) -> challenge: is the defect reachable in data? R2 CONFIRMED-low (0 affected days all-history) -> challenge: confirm protection is structural not coincidental. R3 CONFIRMED-low (batch_daily & trade_daily share the same trade universe). R4 held.
- **C07:** R1 CONFIRMED medium (positivity-filter divergence) -> challenge: quantify negative-correction rows. R2 CONFIRMED-low (0 non-positive of 813,702) -> challenge: confirm identical totals on a real window. R3 CONFIRMED-low (both paths = 3807.98, diff=0). R4 held.
- **C08:** R1 CONFIRMED medium (missing window:7d) -> challenge: does a consumer/CI filter by window:7d? R2 CONFIRMED-low (check_api_tags.py does not enforce window:; no consumer) -> challenge: grep dashboards/MCP for window:7d. R4 CONFIRMED-low (re-measured: granularity present, window absent; cosmetic). (Verifier had no separate R3 entry; settled low across rounds.)
- **C09:** R1 CONFIRMED low (16,253/2,702,295=0.601%) -> challenge: confirm business framing (historical concentration). R2 CONFIRMED-low (by-year distribution, only 45 in 2026) -> challenge: confirm no downstream avg/coalesce distortion. R3 CONFIRMED-low (sum-based volume, NULL->0). R4 held.
- **C10:** R1 CONFIRMED low (symbol-keyed join) -> challenge: bound collision risk with data. R2 CONFIRMED-low (only 2 colliders; EURe ~$14M, stablecoin) -> challenge: verify EURe price harmlessness. R3 CONFIRMED-low (1 price/day, rel_spread=0). R4 held.
- **C11:** R1 CONFIRMED high (mart date/label/value vs semantic week/pair/volume_usd/num_trades) -> challenge: prove live query failure. R2 CONFIRMED-high (describe_table proof; live exec blocked by manifest_hash_mismatch) -> challenge: reload registry & capture bind error. R3 CONFIRMED-high (registry won't clear read-only; static + selectable settles high). R4 held.
- **C12:** R1 CONFIRMED high (cutover 2024-09-25; api-gate present) -> challenge: confirm no unguarded revenue path; check semantic cow_fees_usd. R2 CONFIRMED-high (all 5 sum paths api-gated) -> challenge: demonstrate magnitude. R3 CONFIRMED-high (onchain $70 vs api $282,954; gate must stay). R4 held.
- **C13:** R1 CONFIRMED medium (4 metrics approved + disclaimer) -> challenge: confirm discoverable from consumer side. R2 CONFIRMED-medium (all four selectable; pair_trades_value doubly broken) -> challenge: confirm one returns real data. R3 CONFIRMED-medium (3 bind to real columns; tier-demotion recommended). R4 held.
- **C14:** R1 CONFIRMED medium (agg:avg, undocumented) -> challenge: quantify divergence. R2 CONFIRMED-medium (5.0/4.1 day vs 8/10 distinct, >2x) -> challenge: confirm served via semantic layer. R3 CONFIRMED-medium (live query unavailable; static + R2 quant stands). R4 held.
- **C15:** R1 CONFIRMED medium (no solver_value metric) -> challenge: confirm gap from consumer side. R2 CONFIRMED-medium (discover_metrics returns none) -> challenge: confirm not suppressed at candidate/hidden tier. R3 CONFIRMED-medium (absent at every tier). R4 held.
- **C16:** R1 CONFIRMED low (num_interactions<num_trades threshold) -> challenge: bound the misclassification share. R2 CONFIRMED-low (62.7% of multi-trade batches in ambiguous set) -> challenge: resolve definition via docs. R3 CONFIRMED-low (61.7% excl tail; docs not authoritative read-only; flagged for owner). R4 held.
- **C17:** R1 CONFIRMED low (no ethflow reference) -> challenge: verify capture empirically. R2 UNVERIFIABLE (taker test 0/137,877 — capture vs absence indistinguishable) -> challenge: trace specific EthFlow tx hashes. R3 RESOLVED-low (421/432 OrderPlacements resolve to Trade rows; captured; doc gap remains). R4 held RESOLVED.
- **C18:** R1 CONFIRMED low (0 dups 14d, num_trades diff=0 5 days) -> challenge: extend reconciliation to a full month. R2 CONFIRMED-low (full May diff=0, max_diff=0) -> challenge: extend dup-key check to full May. R3 CONFIRMED-low (0 dups full May; 50,248 rows reconcile). R4 held.
- **C19:** R1 CONFIRMED low (max date 06-20, 14 contiguous) -> challenge: verify contiguity over 90 days. R2 CONFIRMED-low (90 distinct=90 span; data clock 06-25 flagged) -> challenge: confirm non-trade-count columns current. R3 CONFIRMED-low (volume_usd/num_batches/unique_traders all non-NULL non-zero). R4 held.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P1 (KEEP/ESCALATE) | Fix the incremental-lookback mismatch: widen the settlements/interactions sub-query window to match the monthly trades partition filter (or join on the partition month, not `addDays(max,-3)`). Actively NULLs solver attribution and inflates `cow_ratio` on every early-month `insert_overwrite` rerun (11 contiguous days corrupted today). | `models/execution/cow/intermediate/int_execution_cow_trades.sql` (L62), `models/execution/cow/intermediate/int_execution_cow_batches.sql` (L42) |
| P1 (KEEP/ESCALATE) | Fix semantic column drift: either rename `api_execution_cow_top_pairs_weekly` output columns to `week/pair/volume_usd/num_trades` or update the semantic model expressions to the mart's `date/label/value`. Both bound metrics fail at query time. | `semantic/authoring/execution/cow/semantic_models.yml` (L56-79), `models/execution/cow/marts/api_execution_cow_top_pairs_weekly.sql` |
| P1 (KEEP) | Preserve the `fee_source='api'` gate on all revenue paths and keep the Sep-2024 cutover caveat documented; do not sum pre-cutover `onchain` fees unfiltered. | `fct_execution_cow_daily.sql`, `api_execution_cow_fees_ts.sql`, `schema.yml` |
| P2 (KEEP) | Demote the 4 auto-generated candidate metrics from `quality_tier: approved` to `candidate` until reviewed. | `semantic/authoring/execution/cow/semantic_models.yml` (L267-362) |
| P2 (KEEP) | Document the `cow_active_solvers` measure's daily-mean semantics (`agg: avg` ~4-5/day vs period-distinct ~8-10), or add a separate period-distinct metric. | `semantic/authoring/execution/cow/semantic_models.yml` (L38-40) |
| P2 (KEEP) | Build a promoted `solver_value` semantic metric so MCP consumers can query gross solver value. | `semantic/authoring/execution/cow/semantic_models.yml`, `api_execution_cow_solver_value_ts.sql` |
| P3 (KEEP) | Confirm the Partial CoW / Pure DEX definition with the data owner (`61.7%` of multi-trade batches fall in the ambiguous set); document the intended threshold. | `api_execution_cow_batch_routing_ts.sql` (L19-22) |
| P3 (KEEP) | Add the `window:7d` tag to `api_execution_cow_kpi_active_solvers` for tag consistency (cosmetic — no consumer enforces it today). | `api_execution_cow_kpi_active_solvers.sql` (L4-5) |
| P3 (KEEP) | Add an ETH-flow handling note to the cow model SQL/schema (orders ARE captured via the EthFlow router as taker; documentation-only gap). | `int_execution_cow_trades.sql`, cow `schema.yml` |
| P4 (KEEP, optional) | Consider an address-based price-join guardrail; current symbol collision (EURe) is harmless but the join key is symbol, not address. | `int_execution_cow_trades.sql` (L85/L105) |
| - (DROP) | C01 ingestor staleness — RESOLVED (42d->5d, KPIs recovered, durable). | `crawlers_data.cow_api_trade_fees` |
| - (DROP) | C02 source-freshness threshold — RESOLVED (36h/48h live, wired into cron). | `models/crawlers_data/sources.yml` |
| - (DROP) | C03 production tags — RESOLVED (all 18 marts + 4 staging tagged). | `models/execution/cow/**` |
| - (DROP) | C17 ETH-flow capture gap — RESOLVED (proven captured; only a doc note remains, moved to P3). | `int_execution_cow_trades.sql` |

Note: C06 (LEFT-JOIN-miss returns 0) and C07 (two-path positivity divergence) remain CONFIRMED but downgraded to low/non-biting (0 affected days / 0 non-positive rows) — no code change required beyond optional hardening; not listed as priorities.
