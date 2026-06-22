# Model review (revisit 2026-06-21): execution/prices

Baseline: `docs/model_review/execution-prices.md` (2026-06-11). 20 cases re-verified over 3 rounds. Headline: 0 fully resolved, 3 CHANGED (two freshness/gap items recovered, one doc-omission downgraded), 17 STILL CONFIRMED — the critical SAFE forward-fill overstatement is not just unfixed but has worsened to `4.32x` and is now traced to a real served `~$1.95M/day` overstatement in a production USD mart.

---

## Status summary

| Case | P0 | Claim (short) | Orig sev | Status | New sev | Conf | Incident | Rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONPRICES-C01 | P0-16 | SAFE 3.1x overstated via uncapped forward-fill beating Dune | critical | CONFIRMED | critical | high | none | 3 |
| EXECUTIONPRICES-C02 |  | No forward-fill staleness guard / no demotion vs Dune (structural) | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONPRICES-C03 |  | Hub lacks `unique_combination_of_columns` on `(date,symbol)` | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONPRICES-C04 |  | sGNO coverage doc omits sGNO from dex_ratios prose | high | CHANGED | medium | high | none | 3 |
| EXECUTIONPRICES-C05 |  | WxDAI dual-sourced at equal priority=1 (non-deterministic dedup) | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONPRICES-C06 |  | sDAI INNER JOIN on xDAI drops sDAI on oracle gap day | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONPRICES-C07 |  | Oracle reads append+RMT source without FINAL (argMax latent) | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONPRICES-C08 |  | CoW LEFT JOINs lack `join_use_nulls` per convention | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONPRICES-C09 |  | schema.yml labels a ReplacingMergeTree table a 'view' | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONPRICES-C10 |  | Redundant `ORDER BY` in hub final SELECT | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONPRICES-C11 |  | sGNO DEX series (4 obs) is effectively dead code | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONPRICES-C12 |  | USDC/USDT serve oracle market prices incl 2023 depeg; intent undoc | high | CONFIRMED | medium | high | none | 3 |
| EXECUTIONPRICES-C13 |  | Data: 229 off-peg USDC rows / 2,786 total, min 0.9689 | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONPRICES-C14 |  | sGNO priced as GNO ~99% of history; caveat not surfaced | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONPRICES-C15 |  | BRZ relies entirely on BRLA proxy; no availability monitor | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONPRICES-C16 |  | Forex-peg approximations not surfaced as schema.yml caveat | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONPRICES-C17 |  | Only semantic metric is candidate-tier unfiltered avg; no api: tag | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONPRICES-C18 | P0-16 | Data: SAFE 3.1x; hub max 3d behind; dex max 2026-06-07 | critical | CHANGED | critical | high | none | 3 |
| EXECUTIONPRICES-C19 |  | Data: hub 0 duplicate-grain (date,symbol) | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONPRICES-C20 |  | Data: wstETH oracle 2-day gap (1,117 vs 1,119) | low | CHANGED | low | high | none | 3 |

No NEW cases surfaced during the revisit.

---

## Delta vs baseline

### RESOLVED (0)

No case is fully resolved. The two freshness/gap data observations (C18 freshness component, C20) recovered, but both are recorded as CHANGED because either a non-data component persists (C18) or the underlying observation materially shifted (C20). No code defect was fixed.

### CHANGED (3)

- **EXECUTIONPRICES-C18** (critical) — Split finding. The **freshness component RESOLVED**: `int_execution_token_prices_daily`, `int_execution_prices_native_daily`, `int_execution_prices_oracle_daily` now max at `2026-06-20` = `today()-1`, which is each model's own `WHERE date < today()` ceiling (fully fresh; baseline hub was `2026-06-08`, 3 days behind, dex `2026-06-07`). `int_execution_prices_dex_ratios` max = `2026-06-19`. The **SAFE-overstatement component is NOT resolved** and worsened (see C01). Incident attribution corrected to `none` (normal daily-cron catch-up): the baseline 3-day lag cannot be tied to either June incident — `docs/incidents/logs_ingestion_gap_2026.md` explicitly scopes incident B to two short windows (`2026-05-30` ~5.5 min / 65 blocks + `2026-06-14` ~8.5 min / 100 blocks = 165 blocks, ~11,550 logs) that cannot produce a multi-day table lag, and is "not the June insert_overwrite wipe". The prior `microbatch_insert_overwrite` attribution was dropped.
- **EXECUTIONPRICES-C20** (low) — wstETH oracle gap closed. Baseline `1,117` distinct dates vs `1,119` span (2-day interior gap masked by forward-fill); now `1,129` distinct == `1,129` span with stable `min(date)=2023-05-15` and `max(date)=2026-06-20`, so the `+12` rows are ~10 new trailing days + 2 interior backfilled days — a genuine backfill, not a window slide. Oracle symbol count still `12`. No incident attribution.
- **EXECUTIONPRICES-C04** (high -> medium) — sGNO coverage doc omission persists in `models/execution/prices/intermediate/schema.yml` (dex_ratios model description and symbol-column description list only `GBPe, BRLA, BRZ, COW, SAFE`, omitting sGNO that the SQL prices). Downgraded to medium because the omission is confined to the dex_ratios prose; the consumer-facing hub description never enumerates per-symbol coverage, and no API/MCP/metric generator consumes the dex_ratios coverage prose as a contract. No incident.

### STILL CONFIRMED (17)

- **EXECUTIONPRICES-C01** (critical) — SAFE overstatement worse, not better. Hub SAFE `0.366661` (frozen across `2026-06-18/19/20`) vs Dune `0.084782` on `2026-06-20` = `4.32x` (baseline `3.1x`). Native SAFE is a single constant `0.366661` (`uniqExact(price)=1`) across all 232 days `2025-11-01..2026-06-20`; last DEX obs `2025-11-18` (day 20395), `262` days stale. Served blast radius proven: `fct_execution_tokens_metrics_daily` SAFE `2026-06-20` supply `6,920,005` * `0.366661` = supply_usd `$2,537,298` vs `~$586,700` at Dune price = `~$1.95M/day` overstatement into a production USD mart. No incident.
- **EXECUTIONPRICES-C02** (high) — Code unchanged: `int_execution_prices_native_daily.sql` lines 117-126 still `last_value(d.price) IGNORE NULLS OVER (... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` with no age cap and no demote-vs-Dune path; hub gives native priority 1 over Dune priority 3 unconditionally. Live blast radius: SAFE is the only symbol frozen `>30d` today (`262d`); the other DEX-priced symbols changed recently (GBPe=1d, BRLA=2d, BRZ=2d, COW=19d) so each is one liquidity dry-up from SAFE's fate.
- **EXECUTIONPRICES-C03** (high) — Hub `int_execution_token_prices_daily` (schema.yml lines 4-56) still carries only Elementary volume/freshness/schema-change tests; no `dbt_utils.unique_combination_of_columns`. All three sublayers carry it (oracle line 125, dex_ratios line 174, native line 245). `26` SQL models ref the hub; none tests the hub's `(date,symbol)` grain (each consumer test is on its own grain). A hub dedup regression would be caught by no test anywhere. Currently masked by a clean grain (C19).
- **EXECUTIONPRICES-C05** (medium) — Two priority=1 WXDAI sources persist: native WXDAI (hub line 79) and `wxdai_from_xdai` (hub line 83), with no tiebreaker in the `row_number ORDER BY priority` (line 102). Provably never value-divergent today: `0` days where native WXDAI != native xDAI across all history (both derive from `DAI_USD`). Latent-medium.
- **EXECUTIONPRICES-C06** (medium) — `int_execution_prices_native_daily.sql` line 53 still `INNER JOIN xdai x ON x.date = r.date` for sDAI; no LEFT JOIN fallback. Live drop-risk `0`: xDAI oracle distinct == span = `1,929` (dense, grew from baseline 1,917), and xDAI oracle `max(date)=2026-06-20` equals sDAI rate `max(date)=2026-06-20`, so the newest sDAI day is not silently dropped. Latent-medium.
- **EXECUTIONPRICES-C07** (medium) — `int_execution_prices_oracle_daily.sql` line 76 still reads `contracts_chainlink_feeds_events` without FINAL; argMax at line 92. Source config `incremental_strategy='append'` + `ReplacingMergeTree`, unique_key `(block_timestamp,log_index)`. `0` duplicate `(contract_address, block_timestamp, log_index)` groups across ALL history (AnswerUpdated set) — latent-medium hub-wide.
- **EXECUTIONPRICES-C08** (medium) — `int_execution_prices_dex_ratios.sql` CoW subquery LEFT JOINs `stg_pools__tokens_meta` twice (lines 66-72) with no `join_use_nulls` pre/post hook. Current month (`2026-06-01..today`): `0` unmatched anchor legs (bought_unmatched=0, sold_unmatched=0) of `2,996` whitelist-token CoW trades, so the 18-decimal/empty-token default never fires for in-scope trades. Contained-not-cosmetic; medium.
- **EXECUTIONPRICES-C09** (low) — schema.yml line 5 still reads "...view consolidates daily price data..." while the hub config (lines 3-9) is `materialized='table'`, `engine='ReplacingMergeTree()'`; `describe_table` reports materialization=table. Doc is wrong, not the model.
- **EXECUTIONPRICES-C10** (low) — `int_execution_token_prices_daily.sql` line 123 final SELECT still ends `ORDER BY d.date, symbol`, duplicating engine `order_by='(date, symbol)'` (line 5) on a full-rebuild ReplacingMergeTree. Pure rebuild-sort cost.
- **EXECUTIONPRICES-C11** (low) — dex_ratios sGNO still 4 observations (`2023-12-19..2024-08-02`), `0` rows in the current incremental window; `SGNO` still in the whitelist (SQL lines 102, 116). After last obs sGNO falls back to GNO oracle (native priority-2). Effectively dead code, still scanned each run. (Baseline cited latest `2024-03-09`; actual latest `2024-08-02` — still 4 obs, still 2024.)
- **EXECUTIONPRICES-C12** (high -> medium) — Ordering unchanged: `usd_pegs` priority=4 loses to native oracle priority=1 (hub lines 89/79), so oracle market prices including the 2023 USDC depeg win; intent still undocumented in SQL/schema.yml. Downgraded to medium because after `2023-03-12` USDC has `0` off-peg rows and USDT only `1` (`USDT 2025-10-11 = 1.00592`) — historical, not a current served error. The downstream mart `fct_execution_tokens_metrics_daily` is unbounded (`2020-07-19..2026-06-20`) so the depeg rows are queryable historically but not part of any current dashboard number.
- **EXECUTIONPRICES-C13** (medium) — Re-measured: USDC hub total `2,798` (baseline `2,786`, +12 elapsed days), off-peg (`abs(price-1)>0.005`) `229` (unchanged), min `0.9689` (unchanged), all off-peg `<= 2023-03-12`. The 2 lowest (`2023-03-11=0.9689`, `2023-03-12=0.99162`) cluster on the March-2023 SVB/USDC depeg window — a real event, not an artifact.
- **EXECUTIONPRICES-C14** (low) — `int_execution_prices_native_daily.sql` documents `sGNO ~= GNO` in the SQL comment (line 24) and applies the GNO-oracle fallback (lines 91-93), but schema.yml native description (lines 181-189) does not surface the staking-discount approximation as a consumer caveat. On the 4 DEX days `sGNO_dex/GNO_oracle` ranged `0.945..0.998` (up to ~5.5% discount). Low only because sGNO has negligible downstream consumption.
- **EXECUTIONPRICES-C15** (low) — BRZ served `356` rows via the BRZ<-BRLA priority-2 fallback (native line 90) vs `1` direct DEX observation; proxy validated (`BRZ_direct 0.18518` vs same-day `BRLA 0.18712`, ratio `0.9896`); no BRLA availability monitor/alert in SQL or schema.yml. (Baseline `344` rows; +12 elapsed days.)
- **EXECUTIONPRICES-C16** (low) — Oracle SQL still maps `EUR_USD->EURe`, `CHF_USD->ZCHF`, `CHF_USD->svZCHF` (feed_symbols lines ~32-45); the forex-peg/depeg-blindness approximation is documented in `docs/native_token_prices_build_plan.md` (lines 64, 110, 151) but not surfaced as a schema.yml consumer caveat (oracle description lines 58-68).
- **EXECUTIONPRICES-C17** (low) — Hub tags (`int_execution_token_prices_daily.sql` line 8) = `['production','execution','prices','daily']`, no `api:` tag (SQL-ref-only). The only metric `execution_token_prices_daily__price_value` (`semantic/authoring/execution/prices/semantic_models.yml`) is `agg=average, expr=price` (no symbol filter), `quality_tier=candidate`, with a "do NOT sum / auto-generated candidate" warning. No `api_` per-token series model exists under `models/execution/prices/`.
- **EXECUTIONPRICES-C19** (low) — Hub duplicate-grain check on `(date,symbol)` still `0` over the last 30 days; also `0` across ALL history. Grain clean; corroborates that C03 is a latent regression-guard gap, not active dupes.

### UNVERIFIABLE / UNRESOLVED (0)

All 20 cases reached a settled status with high confidence; no fabricated evidence was detected across the three rounds (every code citation matched live files and every warehouse number reproduced).

---

## Evidence appendix

**C01 / C18 (SAFE overstatement + freshness)**
```sql
SELECT h.date, h.price hub_safe, d.price dune_safe, h.price/d.price ratio
FROM (SELECT date,price FROM dbt.int_execution_token_prices_daily WHERE upper(symbol)='SAFE' AND date>=20619) h
LEFT JOIN (SELECT date,price FROM dbt.stg_crawlers_data__dune_prices WHERE upper(symbol)='SAFE') d ON h.date=d.date;
-- hub SAFE = 0.366661 frozen 2026-06-15..06-20; Dune 0.0922 -> 0.0848 same window; ratio 3.98x -> 4.32x on 2026-06-20.

SELECT uniqExact(price), min(price), max(price), count()
FROM dbt.int_execution_prices_native_daily
WHERE upper(symbol)='SAFE' AND date BETWEEN 20392 AND 20624;
-- uniqExact=1, min=max=0.366661359549769 across 232 days (2025-11-01..2026-06-20).

SELECT max(date) FROM dbt.int_execution_token_prices_daily; -- 2026-06-20 (today-1)
SELECT max(date) FROM dbt.int_execution_prices_native_daily; -- 2026-06-20
SELECT max(date) FROM dbt.int_execution_prices_oracle_daily; -- 2026-06-20
SELECT max(date) FROM dbt.int_execution_prices_dex_ratios;   -- 2026-06-19

-- served blast radius:
-- fct_execution_tokens_metrics_daily SAFE 2026-06-20 supply 6,920,005 * 0.366661 = supply_usd $2,537,298
-- vs ~$586,700 at Dune 0.08478 -> ~$1.95M/day overstatement.
```

**C02 (per-symbol staleness)**
```sql
-- days since last price CHANGE per DEX-priced symbol (lagInFrame on int_execution_prices_native_daily):
-- SAFE=262, COW=19, GBPe=1, BRLA=2, BRZ=2, sGNO=0. Only SAFE frozen >30d.
-- Code: native lines 117-126 uncapped last_value IGNORE NULLS; hub native priority=1 > Dune priority=3.
```

**C03 / C19 (grain test + dupe check)**
```sql
-- schema.yml: hub lines 4-56 = only elementary.{volume_anomalies,freshness_anomalies,schema_changes}; NO dbt_utils.unique_combination_of_columns.
-- oracle line 125, dex_ratios line 174, native line 245 all carry unique_combination_of_columns on (date,symbol).
SELECT count() FROM (SELECT date,symbol,count() c FROM dbt.int_execution_token_prices_daily GROUP BY date,symbol HAVING c>1);
-- 0 across all history (and 0 over last 30 days). 26 SQL models ref the hub; none test its (date,symbol) grain.
```

**C04 (sGNO coverage doc)**
```
-- dex_ratios SQL whitelist (lines 102,116) includes 'SGNO'; warehouse 4 sGNO dex rows (2023-12-19..2024-08-02).
-- schema.yml dex_ratios model desc (line 137) + symbol-col desc (line 143) list only 'GBPe, BRLA, BRZ, COW, SAFE'.
-- Hub schema.yml description (line 5) does NOT enumerate per-symbol coverage at all.
```

**C05 (WXDAI dual priority)**
```sql
SELECT count() FROM (
  SELECT w.date FROM (native WXDAI) w INNER JOIN (native xDAI) x ON x.date=w.date WHERE abs(w.price-x.price)>1e-12);
-- 0 days of WXDAI != xDAI across all history. Code: two priority=1 sources (hub lines 79,83), no row_number tiebreaker (line 102).
```

**C06 (sDAI INNER JOIN)**
```sql
-- native line 53: INNER JOIN xdai x ON x.date=r.date (no fallback).
-- xDAI oracle: uniqExact(date)=1929, span=1929 (dense); xDAI oracle max(date)=2026-06-20 = sDAI rate max(date)=2026-06-20. 0 orphans.
```

**C07 (RMT without FINAL)**
```sql
SELECT count() FROM (
  SELECT contract_address,block_timestamp,log_index,count() c
  FROM contracts_chainlink_feeds_events WHERE event_name='AnswerUpdated' GROUP BY 1,2,3 HAVING c>1);
-- 0 across ALL history. oracle line 76 reads source without FINAL; argMax line 92; source append+ReplacingMergeTree.
```

**C08 (CoW join_use_nulls)**
```sql
-- dex_ratios lines 66-72: two LEFT JOINs on stg_pools__tokens_meta; no join_use_nulls pre/post hook.
-- current month: bought_unmatched=0, sold_unmatched=0 of 2,996 whitelist-token CoW trades.
```

**C09 (view vs table)**
```
-- schema.yml line 5: '...view consolidates daily price data...'; hub config (lines 3-9): materialized='table', engine='ReplacingMergeTree()'.
-- describe_table int_execution_token_prices_daily -> materialization: table.
```

**C10 (redundant ORDER BY)** — `int_execution_token_prices_daily.sql` line 123 final SELECT `ORDER BY d.date, symbol`; engine `order_by='(date, symbol)'` (line 5).

**C11 / C14 / C15 (sGNO / BRZ proxies)**
```sql
SELECT symbol,count(),min(date),max(date) FROM dbt.int_execution_prices_dex_ratios GROUP BY symbol;
-- sGNO: count=4, min 2023-12-19, max 2024-08-02. BRZ direct dex obs=1 (2024-08-29). native BRZ via BRLA fallback=356 rows.
-- sGNO_dex/GNO_oracle on the 4 days: 0.9854, 0.9525, 0.9450, 0.9983. BRZ_direct 0.18518 vs BRLA 0.18712 = 0.9896.
```

**C12 / C13 (USDC/USDT off-peg)**
```sql
SELECT symbol,count(),countIf(abs(price-1)>0.005),min(price),maxIf(date,abs(price-1)>0.005)
FROM dbt.int_execution_token_prices_daily WHERE symbol IN ('USDC','USDT') GROUP BY symbol;
-- USDC: 229 off-peg of 2,798, min 0.9689, last off-peg 2023-03-12.
-- USDT: 266 off-peg of 2,813, min 0.9795, last off-peg 2025-10-11 (only 1 off-peg after 2023-04-01).
-- 5 lowest USDC: 2023-03-11=0.9689, 2023-03-12=0.99162, 2020-04-11=0.99593, 2021-02-14=0.99638, 2021-02-13=0.99672.
-- usd_pegs priority=4 (line 89) loses to native priority=1 (line 79); no intent doc.
```

**C16 (forex peg)** — oracle feed_symbols map `EUR_USD->EURe`, `CHF_USD->ZCHF`, `CHF_USD->svZCHF`; schema.yml oracle desc carries no depeg-blindness caveat; `docs/native_token_prices_build_plan.md` lines 64/110/151 document the peg assumption.

**C17 (semantic / api tags)** — hub tags line 8 = `['production','execution','prices','daily']` (no `api:`); `semantic_models.yml` measure `execution_token_prices_daily__price_value` agg=average, expr=price (no symbol filter), quality_tier=candidate; no `api_` per-token series model.

**C20 (wstETH oracle gap)**
```sql
SELECT uniqExact(toDate(date)) distinct, dateDiff('day',min(date),max(date))+1 span, min(date), max(date)
FROM dbt.int_execution_prices_oracle_daily WHERE symbol='wstETH';
-- distinct=1129, span=1129 (gap 2->0), min=2023-05-15, max=2026-06-20. Oracle symbol count=12.
```

---

## Review log (>= 3 rounds per case)

- **C01** — R1 CONFIRMED critical (hub 0.36666 vs Dune 0.08478 = 4.325x; 230d stale) -> orch challenge: prove propagation end-to-end (native single constant + hub priority spot-check) -> R2 CONFIRMED (native uniqExact=1 over 232 days; hub returns only native pri-1 + Dune pri-3 for SAFE) -> orch challenge: scope downstream served-number blast radius -> R3 CONFIRMED (fct_execution_tokens_metrics_daily SAFE supply_usd $2,537,298 = ~$1.95M/day overstatement; 26 refs).
- **C02** — R1 CONFIRMED high (code uncapped, no demotion) -> challenge: quantify live blast radius -> R2 CONFIRMED (only SAFE frozen >30d, 262d) -> challenge: stress the generalization -> R3 CONFIRMED (GBPe=1d/BRLA=2d/BRZ=2d/COW=19d, all <30d; SAFE the live realization).
- **C03** — R1 CONFIRMED high (hub lacks the test sublayers carry) -> challenge: confirm fan-out scoped to regression-guard (whitelist_symbols grain-unique) -> R2 CONFIRMED -> challenge: count consumers and verify none test the hub grain -> R3 CONFIRMED (26 refs; consumer tests on own grain only).
- **C04** — R1 CONFIRMED high (SQL prices sGNO; schema.yml omits) -> challenge: justify high vs medium (any contract consumer?) -> R2 CONFIRMED, recommend medium (developer-facing only) -> challenge: confirm hub doc doesn't leak the omission -> R3 CHANGED medium (hub desc never enumerates per-symbol coverage).
- **C05** — R1 CONFIRMED medium (two pri-1 WXDAI sources, no tiebreaker) -> challenge: quantify divergence vs xDAI -> R2 CONFIRMED (884/2813 hub WxDAI!=xDAI days from Dune CTE, not the pri-1 tie) -> challenge: prove the two native pri-1 sources never diverge -> R3 CONFIRMED (0 WXDAI!=xDAI in native).
- **C06** — R1 CONFIRMED medium (INNER JOIN, no fallback) -> challenge: bound live risk (gaps/orphans) -> R2 CONFIRMED (1929 dense, 0 orphans) -> challenge: confirm xDAI max keeps pace with sDAI max -> R3 CONFIRMED (both 2026-06-20).
- **C07** — R1 CONFIRMED medium (append+RMT read without FINAL) -> challenge: measure dup re-decodes -> R2 CONFIRMED (0 in 2026) -> challenge: extend across all history -> R3 CONFIRMED (0 all-history).
- **C08** — R1 CONFIRMED medium (no join_use_nulls) -> challenge: quantify residual numeric path -> R2 CONFIRMED (whitelist bounds it; numeric path exists) -> challenge: count unmatched anchor legs this month -> R3 CONFIRMED (0 of 2,996; contained-not-cosmetic).
- **C09** — R1 CONFIRMED low ('view' label) -> challenge: confirm warehouse matches config -> R2 CONFIRMED (describe_table=table) -> R3 CONFIRMED (unchanged).
- **C10** — R1 CONFIRMED low (redundant ORDER BY) -> challenge: confirm no query-time benefit -> R2 CONFIRMED (consumers re-ref via SELECT, no order contract) -> R3 CONFIRMED.
- **C11** — R1 CONFIRMED low (4 sGNO obs) -> challenge: confirm 0 rows in current window yet still scanned -> R2 CONFIRMED (0 current-window rows; still in whitelist) -> R3 CONFIRMED.
- **C12** — R1 CONFIRMED high (oracle beats peg, intent undoc) -> challenge: quantify current-era impact -> R2 CONFIRMED, recommend medium (only 1 off-peg row after 2023-03-12) -> challenge: confirm downstream mart not window-bounded -> R3 CONFIRMED medium (mart unbounded but deviation historical-only).
- **C13** — R1 CONFIRMED medium (229/2,798, min 0.9689) -> challenge: tie 5 lowest to SVB depeg window -> R2 CONFIRMED (2 lowest on 2023-03-11/12) -> R3 CONFIRMED.
- **C14** — R1 CONFIRMED low (caveat not surfaced) -> challenge: magnitude check sGNO_dex/GNO_oracle -> R2 CONFIRMED (up to -5.5%) -> R3 CONFIRMED.
- **C15** — R1 CONFIRMED low (356 rows BRLA proxy, 1 direct) -> challenge: validate proxy quality + no monitor -> R2 CONFIRMED (ratio 0.9896, no test) -> R3 CONFIRMED.
- **C16** — R1 CONFIRMED low (forex peg not surfaced) -> challenge: confirm build plan documents it -> R2 CONFIRMED (build plan lines 64/110/151) -> R3 CONFIRMED.
- **C17** — R1 CONFIRMED low (no api: tag) but metric not re-read -> challenge (insufficient): quote the metric def -> R2 CONFIRMED (candidate-tier unfiltered avg, no api_ model) -> R3 CONFIRMED (settled).
- **C18** — R1 CHANGED critical, attribution=microbatch_insert_overwrite (freshness recovered; SAFE worse) -> challenge (insufficient): attribution contradicts incident doc -> R2 RESOLVED-freshness but kept insert_overwrite attribution -> challenge (insufficient): drop microbatch attribution, .gap_refresh.log is gap_window_refresh.py output (incident B) -> R3 CHANGED critical, attribution=none (normal cron catch-up; SAFE overstatement still CONFIRMED).
- **C19** — R1 CONFIRMED low (0 dupes 30d) -> challenge: re-run over all history -> R2 CONFIRMED (0 all-history) -> R3 CONFIRMED.
- **C20** — R1 CHANGED low (gap 2->0; 1,129==1,129) -> challenge: confirm real backfill vs span slide -> R2 CONFIRMED gap-closed (couldn't pin the 2 exact dates) -> challenge: confirm stable min(date) -> R3 CHANGED low (min 2023-05-15 stable; +12 = 10 trailing + 2 backfilled).

---

## Refreshed recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 (ESCALATE) | Cap the forward-fill age and/or demote a stale native price below Dune so a DEX-only token that loses liquidity stops serving a frozen price. SAFE is serving `4.32x` (`~$1.95M/day` overstatement into `fct_execution_tokens_metrics_daily`). One root fix covers C01/C02/C18-SAFE. | `models/execution/prices/intermediate/int_execution_prices_native_daily.sql` (lines 117-126), `int_execution_token_prices_daily.sql` (priority stack lines 79/87/89) |
| P1 (KEEP) | Add `dbt_utils.unique_combination_of_columns` on `(date, symbol)` to the hub — the three sublayers carry it; no consumer tests the hub grain. Latent today (0 dupes) but a dedup regression fans out to all 26 consumers untested. | `models/execution/prices/intermediate/schema.yml` (hub entry) |
| P1 (KEEP) | Document the USDC/USDT oracle-market-over-peg intent (priority 4 peg loses to priority 1 oracle, so the 2023 SVB depeg is served historically). C12/C13: 229 off-peg USDC rows, all `<=2023-03-12`. | `int_execution_token_prices_daily.sql`, `schema.yml` |
| P2 (KEEP) | Give the two priority=1 WXDAI sources distinct priorities or add a `row_number` tiebreaker (currently non-deterministic but provably never value-divergent). | `int_execution_token_prices_daily.sql` (lines 79/83/102) |
| P2 (KEEP) | Replace the sDAI `INNER JOIN xdai` with a LEFT JOIN + fallback (live risk 0 today; structural fragility remains). | `int_execution_prices_native_daily.sql` (line 53) |
| P2 (KEEP) | Read `contracts_chainlink_feeds_events` with FINAL (or dedup before argMax) — append+RMT without FINAL is latent (0 dup re-decodes today). | `int_execution_prices_oracle_daily.sql` (line 76) |
| P2 (KEEP) | Add the `join_use_nulls` pre/post hook around the CoW `stg_pools__tokens_meta` LEFT JOINs per project convention (0 unmatched legs this month; contained). | `int_execution_prices_dex_ratios.sql` (lines 66-72) |
| P3 (KEEP) | Surface consumer caveats in schema.yml: add sGNO to dex_ratios coverage prose (C04, downgraded to medium); add sGNO~=GNO staking-discount note (C14, up to -5.5%); add forex-peg depeg-blindness note (C16); add BRZ<-BRLA proxy + availability-monitor note (C15). | `models/execution/prices/intermediate/schema.yml` |
| P3 (KEEP) | Fix the 'view' label to 'table (ReplacingMergeTree)' (C09) and drop the redundant final `ORDER BY` (C10). | `schema.yml` line 5, `int_execution_token_prices_daily.sql` line 123 |
| P3 (KEEP) | If per-token price series are to be exposed via API/MCP, build a validated `api_`/per-token series model — the only metric today is a candidate-tier symbol-unfiltered average and the hub has no `api:` tag (C17). | `semantic/authoring/execution/prices/semantic_models.yml`, hub |
| n/a (DROP) | Drop the baseline freshness/lag recommendation (hub now fresh at `today()-1` = each model's `date<today()` ceiling; the 3-day lag was normal cron catch-up, not an incident). | C18 freshness component |
| n/a (DROP) | Drop the wstETH oracle 2-day-gap concern (closed; distinct==span=1,129, stable min). | C20 / `int_execution_prices_oracle_daily.sql` |
