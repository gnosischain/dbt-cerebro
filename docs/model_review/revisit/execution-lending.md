# Model review (revisit 2026-06-21): execution/lending

Baseline `docs/model_review/execution-lending.md` (dated `2026-06-11`); `18` cases re-verified over `3` rounds. Headline: the June `microbatch_insert_overwrite` REPLACE-PARTITION wipe recovery cleared the production data-side damage — `6` cases RESOLVED (incl. the critical `e27` utilization underflow) — while `9` code/schema/scope defects remain CONFIRMED, `1` CHANGED, and `1` NEW latent duplicate-partition issue was discovered.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | conf | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONLENDING-C01 | P0-15 | Int256 underflow -> WxDAI utilization `~4.4e27` served live | critical | RESOLVED | low | high | microbatch_insert_overwrite | 3 |
| EXECUTIONLENDING-C02 | P0-15 | Negative `cumulative_scaled_borrow`; RepayWithATokens keys `repayer` not `user` | high | RESOLVED | low | high | none | 3 |
| EXECUTIONLENDING-C03 | | Lenders STOCK vs borrowers FLOW sold as sibling `*_count_7d` | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONLENDING-C04 | | `balance_usd=0` on positive balance (price LEFT JOIN coalesce->0) | high | RESOLVED | resolved | high | microbatch_insert_overwrite | 3 |
| EXECUTIONLENDING-C05 | P0-07 | Agave silently excluded from entire lending pipeline | high | CONFIRMED | medium | high | none | 3 |
| EXECUTIONLENDING-C06 | | `materialized='table'` -> `is_incremental()` dead branches | medium | RESOLVED | resolved | high | none | 3 |
| EXECUTIONLENDING-C07 | | Data 4 days stale; SparkLend `0/0`; no freshness test | medium | RESOLVED | low | high | microbatch_insert_overwrite | 3 |
| EXECUTIONLENDING-C08 | | `toUInt64(bitmap_state)` instead of `bitmapCardinality()` | medium | CONFIRMED | low | high | none | 3 |
| EXECUTIONLENDING-C09 | | `tvl_by_token` emits undocumented `protocol` col; schema says "aggregated" | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONLENDING-C10 | | Treasury/collector position not excluded from top-lenders/TVL | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONLENDING-C11 | | top_lenders stack tagged `dev` despite tier1 promotion intent | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONLENDING-C12 | | `max(date)-7`, duplicate `lending,lending` tags, `week AS date` allowlist | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONLENDING-C13 | | Near-zero tests on the three Int256/UInt256 intermediates | low | CONFIRMED | medium | high | none | 3 |
| EXECUTIONLENDING-C14 | | Semantic-layer gaps + un-pruned auto-gen weekly APY candidates | low | CHANGED | low | high | none | 3 |
| EXECUTIONLENDING-C15 | | sDAI potential cross-unit double-count, scope undocumented | low | RESOLVED | low | medium | none | 3 |
| EXECUTIONLENDING-C16 | | `29` rows `util>100` (distinct from e27 underflow) | medium | RESOLVED | low | high | microbatch_insert_overwrite | 3 |
| EXECUTIONLENDING-C17 | | Null borrow APY rows (benign non-variable-borrow reserves) | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONLENDING-N01 | | Doubled `2026-06-18` partition in user_balances_daily (RMT not merged) | (new) | CONFIRMED-NEW | medium | high | microbatch_insert_overwrite | 2 |

## Delta vs baseline

### RESOLVED (6)
- **C01** (critical -> low): the `14` live WxDAI rows at `~4.4-4.5e27` utilization are gone end-to-end. WxDAI (`0xe91d153e...`) shows `util>1000 EVER=0`, `e27 EVER=0`, `neg_borrow EVER=0` across full history; only `1` stale `2023-10-12` SparkLend GNO row at `6.52e30` remains. Served `fct_execution_yields_opportunities_latest` Lending max utilization `98.07%`, `0` rows `>100`. Attribution: `microbatch_insert_overwrite` recovery. The Int256 code defect is **latent** (CASE guard at lines `282-284` still checks only `cumulative_scaled_supply>0`; `toUInt256(neg)` at line `287` still wraps) but has zero live blast radius.
- **C02** (high -> low): `cumulative_scaled_borrow<0` rows collapsed `15 -> 1` (the same `2023` row). RepayWithATokens keying defect (`diffs_daily` line `125` keys `decoded_params['repayer']` not `['user']`) is **dormant**: `0` of `789` events (`753` Aave + `36` Spark) are third-party (`repayer!=user`), so `0%` of repaid debt is mis-credited. The `15->1` collapse was the partition-wipe recovery, not a code fix.
- **C04** (high -> resolved): `zero-usd-on-positive-balance = 0` of `33,546` positive rows on `2026-06-20` (`0.00%`), and `0%` across every day `2026-05-01`..`2026-06-20` (51 days). Baseline was `100%` SparkLend GNO / `96%` sDAI / `51%` Aave GNO. Robust to the N01 duplicate (06-18 doubled partition also `0%`). Attribution: recovery + complete price/symbol coverage.
- **C06** (medium -> resolved): `int_execution_lending_aave_daily.sql` is now `materialized='incremental'`, `incremental_strategy='insert_overwrite'` (baseline claimed `'table'`). The `is_incremental()` lka LEFT JOIN + `COALESCE(f.apy_daily, lka.last_apy)` forward-fill branches now compile to `True` on incremental runs. The dead-branch premise is false; branches are reachable (though rarely exercised given near-daily RDU activity).
- **C07** (medium -> low): max date advanced `2026-06-07` (4 days stale) -> `2026-06-20` (1 day, fresh) across all four models. SparkLend `0/0` proven genuine (`0` SparkLend Supply/Borrow events in 7d window). Residual: **no** source-freshness or elementary volume test exists on the aaveV3/spark sources. Attribution: recovery + scheduled refresh (model carries `tags=['production',...]`).
- **C16** (medium -> low): `util>100` band shrank `29 -> 14` rows (all historical, none in `2026-04..06`); served yields endpoint caps at `98.07%`, `0` rows `>100`. The band is a historical index-timing artifact with zero live reach.
- **C15** (UNVERIFIABLE -> RESOLVED-as-docs-gap, low): no in-repo aggregator UNIONs/sums lending sDAI TVL with savings sDAI; `api_execution_lending_tvl_by_token_latest` has zero downstream model consumers. Separate sDAI units exist (revenue/yields/ubo) but none collapse with the lending leg. Scope boundary remains undocumented (docs gap, not realized double-count). Confidence medium (external BI layer not inspectable).

### CHANGED (1)
- **C14** (low -> low): a lending `semantic_models.yml` now exists (APY daily/weekly + deposit/borrow volumes) — partial coverage added since baseline. But utilization, TVL-by-token, lender/borrower counts, balance cohorts, and top-lenders still have **no** semantic model (MCP must drop to raw SQL). The two auto-generated weekly APY candidates `execution_lending_apy_weekly_value` (lines `179-204`) and `execution_lending_borrow_apy_weekly_value` (lines `205-230`) remain un-pruned, both self-labeled "Auto-generated candidate metric; review and promote".

### STILL CONFIRMED (9)
- **C03** (high): served mismatch quantified — `lenders_count_7d` ALL `= 32,511` (STOCK, `granularity:latest`) vs `borrowers_count_7d` ALL `= 36` (7-day FLOW, `granularity:last_7d`), a `~900x` gap sold as sibling `*_count_7d`. The borrowers `schema.yml` (lines `129-135`) even mislabels the FLOW as a STOCK "positive debt position". In-repo consumer reach unprovable (no dashboard JSON in repo; both views have zero downstream model consumers) — reach inferred from naming/tagging.
- **C05** (high -> medium): Agave still entirely excluded (`0` references in any lending model). `contracts_agave_LendingPool_events` = `63,381,065` events lifetime, `100%` `event_name=''` (ABI never loaded, = P0-07). Severity eased to medium because live Agave is dormant: only `88` events in the trailing 90 days (max date `2026-06-21`). Historical undercount large; current flow trivial.
- **C08** (medium -> low): `fct_execution_lending_weekly.sql` lines `41-42` compute `toUInt64(lenders_bitmap_state)`/`toUInt64(borrowers_bitmap_state)` on the merged `groupBitmapMerge(...)` AggregateFunction. The implicit cast is **currently correct** (`198/18/15/7` match `lenders_count_weekly`); `bitmapCardinality(groupBitmapMerge(...))` actually ERRORS code `43` on this CH version. Pure idiom-consistency nit.
- **C09** (medium): `api_execution_lending_tvl_by_token_latest.sql` emits `protocol, token, value, as_of_date` (per-protocol rows) but `schema.yml` (lines `153-163`) documents only `token, value, as_of_date` and still says "aggregated across all protocols". Per-protocol rows confirmed: sDAI -> Aave V3 `$18.54M` + SparkLend `$53k` (two rows). False schema contract; double-count latent (no in-repo consumer collapses protocol).
- **C10** (medium): `treasury_mint_deltas` CTE (`diffs_daily` lines `267-304`/`314`) still credits the collector via half-up rounding (`intDiv(...+intDiv(index,2),index)` line `277`, not rayDiv), with no exclusion filter anywhere. Collector `0x3e652e97ff339b73421f824f5b03d75b62f1fb51` is rank `#1` in `6` Aave reserves; `$593,095` = `17.77%` of Aave V3 WxDAI reserve TVL; `$1,028,747` total = `1.2%` of the `$86.0M` Aave V3 total TVL. Distorts per-reserve rankings/cohorts; small headline TVL effect.
- **C11** (low): all three top_lenders models still carry `tags=['dev',...]`; `api_execution_lending_top_lenders_latest` simultaneously carries `tier1`+`granularity:latest`+`elementary.schema_changes` (promotion intent). `check_api_tags.py` line `53` (`if 'production' not in tags: continue`) skips them; `cron_preview.sh` line `9` (tag:production) excludes them. Confirmed live: `top_lenders_ranked` rank-1 rows all `balance_usd=0` (stale, never rebuilt by recovery).
- **C12** (low): all three sub-findings verbatim — `fct_execution_lending_top_lenders_latest.sql` line `18` `max(date) - 7` (Date arithmetic, not `subtractDays`); duplicate `['production','execution','lending','lending']` tags on `fct_execution_lending_latest.sql` (line `6`) and `fct_execution_lending_weekly.sql` (line `8`); `week AS date` alias in both weekly api marts with `check_api_tags.allow` lines `116-117` `::no_grain_col`.
- **C13** (low -> medium): `tests: []` on `int_execution_lending_aave_utilization_daily` (line `96`) and `int_execution_lending_aave_user_balances_daily` (line `209`); `int_execution_lending_aave_diffs_daily` has no tests block at all. The gap ties to live defects: a range test would catch the surviving `util>1000` row, a grain test would catch the N01 `2026-06-18` duplicate (`82,260` rows / `41,138` keys). `diffs_daily` keying columns are currently clean (`0` nulls / `216,041` rows) so the non-null test is latent.
- **C17** (low): `0` null supply APY both protocols; null borrow APY `1,004` (Aave) + `1,772` (Spark), up from baseline `991`/`1,746` as days accrued. Maps cleanly to non-variable-borrow reserves (SparkLend GNO `986`, Aave sDAI `957`, SparkLend sDAI `784`, Aave GNO `47`; `2` isolated edge rows). Benign.

### NEW (1)
- **N01** (medium): `int_execution_lending_aave_user_balances_daily` (ReplacingMergeTree, `incremental_strategy='append'`, tag `refill_append`) has a doubled `2026-06-18` partition: `82,260` rows for `41,138` distinct `(protocol,reserve_address,user_address)` keys (exact `2x`); neighbors clean (`06-17`=`41,131`, `06-19`=`41,141`, `06-20`=`41,144`). Of `41,122` duplicated grains, `310` carry conflicting `balance_usd`/`balance` between copies (not byte-identical), so a forced merge/DELETE is needed — RMT FINAL would resolve those `310` nondeterministically (no version column). Currently unreachable by live KPIs (all marts read `max(date)=2026-06-20`, clean). Attribution: the recovery refill's own un-deduped append side-effect.

## Evidence appendix

### C01 / C16 (utilization underflow + util>100 band) — shared queries
- `SELECT toStartOfMonth(date), protocol, countIf(cumulative_scaled_borrow<0), countIf(utilization_rate>1000) FROM dbt.int_execution_lending_aave_utilization_daily WHERE date>='2026-04-01' GROUP BY ...` -> `2026-04/05/06` both protocols: `neg_borrow=0`, `e27_band=0` every month; max date `2026-06-20`.
- All-history: exactly `1` neg-borrow row = `1` e27 row (the `2023-10-12` SparkLend GNO `6.52e30`), down from `15` at baseline.
- WxDAI-scoped (`0xe91d153e...`): `util>1000 EVER=0`, `e27 EVER=0`, `neg_borrow EVER=0`.
- Served: `SELECT max(utilization_rate), countIf(utilization_rate>100), countIf(>1e26) FROM dbt.fct_execution_yields_opportunities_latest WHERE type='Lending'` -> max `98.07`, `0` rows `>100`, `0` e27, `n=12`.
- C16 band: `14` rows `util in (100,1000]` (all historical, incl `1` SparkLend WETH/GNO `~101.38%`), `0` negative supply.
- Code: guard at `models/execution/lending/intermediate/int_execution_lending_aave_utilization_daily.sql` lines `282-284` checks only `cumulative_scaled_supply>toInt256(0)`; line `287` `toUInt256(c.cumulative_scaled_borrow)` still wraps negatives.

### C02 (RepayWithATokens keying)
- `WITH ev AS (SELECT lower(decoded_params['repayer']) repayer, lower(decoded_params['user']) usr FROM dbt.contracts_aaveV3_PoolInstance_events WHERE event_name='Repay' AND decoded_params['useATokens']='1' UNION ALL ... contracts_spark_Pool_events ...) SELECT count(), countIf(repayer!=usr), countIf(repayer=usr) FROM ev` -> Aave V3: `753` events, `0` third-party, `753` self; SparkLend: `36` events, `0` third-party, `36` self.
- `models/execution/lending/intermediate/int_execution_lending_aave_diffs_daily.sql` line `125` still keys `user_address = lower(decoded_params['repayer'])`.

### C03 (lenders STOCK vs borrowers FLOW)
- `SELECT value FROM api_execution_lending_lenders_count_7d WHERE protocol='ALL' UNION ALL SELECT value FROM api_execution_lending_borrowers_count_7d WHERE protocol='ALL'` -> lenders `= 32,511` (STOCK, `granularity:latest`); borrowers `= 36` (FLOW, `granularity:last_7d`). `~900x`.
- `models/execution/lending/marts/api_execution_lending_lenders_count_7d.sql` counts `countDistinct(user_address) WHERE balance>0` at `max(date)`; `api_execution_lending_borrowers_count_7d.sql` reads `fct_execution_lending_latest WHERE label='Borrowers' AND window='7D'`. Borrowers `schema.yml` lines `129-135` describe it as a STOCK "positive debt position".
- Grep of `models/` for FROM/JOIN: both views have zero downstream model consumers; no dashboard JSON in repo.

### C04 (balance_usd=0)
- `SELECT date, countIf(balance_usd=0 AND balance>0), round(100.0*.../count(),2) FROM int_execution_lending_aave_user_balances_daily WHERE date>='2026-05-01' GROUP BY date` -> `0`/`0.00%` every day for 51 days.
- `2026-06-20`: all 15 `(protocol,symbol)` pairs `pos_zero_usd=0` (e.g. SparkLend GNO `29/29` priced, Aave WxDAI `24,280` priced). Doubled `06-18` partition (`82,260` rows): `pos_zero_usd_raw=0` as well.

### C05 (Agave excluded)
- `SELECT count(), uniqExact(event_name), countIf(event_name=''), min/max(toDate(block_timestamp)), countIf(block_timestamp>=today()-90) FROM dbt.contracts_agave_LendingPool_events` -> `63,381,065` events, `1` distinct event_name (`100%` empty), range `2022-04-19`..`2026-06-21`, only `88` events in trailing 90 days.
- `grep -rni agave models/execution/lending/` -> NONE.

### C06 (materialization)
- `models/execution/lending/intermediate/int_execution_lending_aave_daily.sql` lines `1-11`: `config(materialized='incremental', incremental_strategy='insert_overwrite')`. `is_incremental()` branches: lka LEFT JOIN lines `320-335`, `COALESCE(f.apy_daily, lka.last_apy)` lines `289-336`, `last_value(...) IGNORE NULLS` forward-fill lines `246-265`.

### C07 (freshness)
- `SELECT max(date) FROM ...` -> `2026-06-20` for aave_daily, user_balances, diffs, utilization (yesterday; today `2026-06-21`).
- `fct_execution_lending_latest` 7D ALL: Aave V3 lenders `162`/borrowers `36`; SparkLend `0`/`0`. Last-7d SparkLend `int_execution_lending_aave_daily`: lenders `0`, borrowers `0`, deposits `0`, borrows `0` (genuine zero activity).
- Model `tags=['production','execution','lending','aave','spark']` (line `9`); no dbt source freshness block, no elementary freshness/volume test, model `schema.yml` `tests: []`.

### C08 (bitmap cardinality)
- `models/execution/lending/marts/fct_execution_lending_weekly.sql` lines `22-23` build `groupBitmapMerge(...)`; lines `41-42` apply `toUInt64(...)` on the merged result. `toUInt64(lenders_bitmap_state)` == `lenders_count_weekly` (`198,18,15,7`). `bitmapCardinality(groupBitmapMerge(state))` raises `ILLEGAL_TYPE_OF_ARGUMENT` (code `43`). `fct_execution_lending_latest.sql` line `44` wraps `toUInt64(groupBitmapMerge(...))` inline.

### C09 (tvl_by_token schema)
- `SELECT protocol, token, value FROM api_execution_lending_tvl_by_token_latest WHERE token IN ('sDAI','WxDAI',...)` -> sDAI: Aave V3 `$18,537,032` + SparkLend `$53,437`; WxDAI: Aave V3 `$3,336,683` + SparkLend `$9,106` (two rows each). `api_execution_lending_tvl_by_token_latest.sql` outputs `protocol, b.symbol AS token, sum(b.balance_usd) AS value, as_of_date`; `schema.yml` lines `153-163` document only `token, value, as_of_date`, desc "aggregated across Aave V3 and SparkLend ... across all protocols".

### C10 (treasury pollution)
- `SELECT user_address, round(balance_usd,0), round(100.0*balance_usd/sum(balance_usd) OVER (),2) FROM int_execution_lending_aave_user_balances_daily WHERE date='2026-06-20' AND protocol='Aave V3' AND symbol='WxDAI' AND balance_usd>0 ORDER BY balance_usd DESC` -> collector `0x3e652e97ff339b73421f824f5b03d75b62f1fb51` rank `#1` at `$593,095` = `17.77%` of Aave V3 WxDAI TVL.
- Across all Aave V3 reserves: `$1,028,747` = `1.2%` of `$86.0M` total. Rank `#1` in WxDAI/USDC/EURe/USDC.e/GNO/sDAI; SparkLend treasury `0xb9e6dbfa...` rank `#1` in `5` reserves.
- `int_execution_lending_aave_diffs_daily.sql` `treasury_mint_deltas` CTE lines `267-304`, half-up rounding line `277`; no exclusion in diffs/top_lenders/tvl.

### C11 (dev tags)
- `fct_execution_lending_top_lenders_ranked.sql` line `4` `tags=['dev',...]`; `fct_execution_lending_top_lenders_latest.sql` line `4` `tags=['dev',...]`; `api_execution_lending_top_lenders_latest.sql` line `4` `tags=['dev','execution','tier1','api:lending_top_lenders','granularity:latest']` + `elementary.schema_changes` (schema.yml `363-367`). `check_api_tags.py` line `53`; `cron_preview.sh` line `9`. DB: `top_lenders_ranked` rank-1 rows `balance_usd=0`.

### C12 (idioms)
- `fct_execution_lending_top_lenders_latest.sql` line `18` `SELECT max(date) - 7`. `fct_execution_lending_latest.sql` line `6` + `fct_execution_lending_weekly.sql` line `8` `tags=[...,'lending','lending']`. `api_execution_lending_activity_counts_weekly.sql`/`_volumes_weekly.sql` line `9` `week AS date`; `scripts/checks/check_api_tags.allow` lines `116-117` `::no_grain_col`.

### C13 (test coverage)
- `intermediate/schema.yml`: utilization `tests: []` (line `96`), user_balances `tests: []` (line `209`), diffs_daily no tests block (lines `97-138`).
- `SELECT countIf(user_address=''/NULL), countIf(reserve_address=''/NULL), countIf(diff_scaled IS NULL), count() FROM int_execution_lending_aave_diffs_daily` -> `0, 0, 0, 216,041` (keying clean; non-null test latent). Range test would catch `util>1000=1`; grain test would catch N01 `06-18` duplicate.

### C14 (semantic layer)
- `semantic/authoring/execution/lending/semantic_models.yml`: semantic models for `execution_lending_apy_daily` and `execution_lending_weekly` only (APY + deposit/borrow volumes). No model for utilization/TVL/counts/cohorts/top-lenders. Candidate metrics `execution_lending_apy_weekly_value` (lines `179-204`) and `execution_lending_borrow_apy_weekly_value` (lines `205-230`), self-labeled auto-generated, zero saved_query/dashboard references.

### C15 (sDAI scope)
- Grep `models/` downstream of `api_execution_lending_tvl_by_token_latest` -> only its own schema.yml. Separate sDAI marts in execution/yields (`api_execution_yields_overview_sdai_supply.sql`, `_apy.sql`) but no cross-sector `fct_*_overview`/`api_*_tvl_total` UNIONs/sums lending sDAI with savings sDAI.

### C17 (null APY)
- `SELECT protocol, countIf(apy_daily IS NULL), countIf(borrow_apy_variable_daily IS NULL), count() FROM int_execution_lending_aave_daily GROUP BY protocol` -> Aave V3 `0`/`1,004`/`5,470`; SparkLend `0`/`1,772`/`7,788`. CASE at lines `166-172` maps `variableBorrowRate=0/NULL -> NULL`.

### N01 (doubled partition)
- `SELECT date, count(), uniqExact(protocol,reserve_address,user_address) FROM int_execution_lending_aave_user_balances_daily WHERE date IN ('2026-06-17'..'2026-06-20') GROUP BY date` -> `06-18`=`82,260`/`41,138` (2x); `06-17`=`41,131`/`41,131`; `06-19`=`41,141`/`41,141`; `06-20`=`41,144`/`41,144`.
- Of `41,122` duplicated grains on `06-18`, `310` carry a `balance_usd`/`balance` conflict (max-min `>1e-9`) between copies. No mart filters to fixed `date='2026-06-18'`; all read `max(date)`.

## Review log (>=3 rounds per case)

- **C01**: R1 CHANGED (14 WxDAI e27 -> 1 ancient SparkLend row; code unfixed) -> challenge: prove zero downstream blast radius + full-history scan -> R2 RESOLVED (WxDAI `0` e27 EVER, yields serves `34.17%`) -> challenge: durability across recovered months -> R3 RESOLVED/low (neg_borrow=0 every month `2026-04/05/06`; served `98.07%`).
- **C02**: R1 CHANGED (`15->1`; line 125 still `repayer`) -> challenge: find a real third-party repay -> R2 CHANGED (`0` of `789` third-party) -> challenge: size self-repay exposure -> R3 RESOLVED/low (100% self-repays, `0%` mis-credited; dormant code-quality).
- **C03**: R1 CONFIRMED (code unchanged) -> challenge: quote served values -> R2 CONFIRMED (`32,511` vs `36`, ~900x) -> challenge: prove consumer co-presentation -> R3 CONFIRMED/high (no in-repo consumer; reach inferred; borrowers schema mislabels FLOW as STOCK).
- **C04**: R1 RESOLVED (`0%` zero-usd on `2026-06-20`) -> challenge: prove durable across month + May -> R2 RESOLVED (`0%` 51 days) -> challenge: survive N01 duplicate -> R3 RESOLVED (`06-18` doubled partition also `0%`).
- **C05**: R1 CONFIRMED (`0` refs; `63.4M` @ `100%` empty) -> challenge: size undercount -> R2 CONFIRMED/high (undecoded, P0-07 sole blocker) -> challenge: live vs dormant via raw logs -> R3 CONFIRMED/medium (`88` events/90d, dormant).
- **C06**: R1 RESOLVED (materialized `incremental`, not `table`) -> challenge: prove forward-fill fires -> R2 CHANGED/resolved (flip conclusive; couldn't catch a single row) -> challenge: concrete carry-forward day -> R3 CHANGED/low (branch reachable but rarely exercised; near-daily RDU).
- **C07**: R1 CHANGED (fresh `2026-06-20`; SparkLend `0/0` unproven) -> challenge: prove `0/0` real + freshness test -> R2 RESOLVED (`0/0` genuine zero activity) -> challenge: on scheduled refresh + any freshness test -> R3 RESOLVED/low (on tag:production; no freshness test = residual gap).
- **C08**: R1 CONFIRMED (toUInt64 over merged state) -> challenge: compute both ways -> R2 CHANGED/low (toUInt64 correct; bitmapCardinality ERRORS code 43) -> R3 CONFIRMED/low (idiom nit, working).
- **C09**: R1 CONFIRMED -> challenge: show per-protocol duplicate rows -> R2 CONFIRMED (sDAI two rows) -> challenge: does a consumer collapse protocol -> R3 CONFIRMED/medium (no in-repo aggregator; false schema contract holds).
- **C10**: R1 CONFIRMED (code defect; dev mart stale) -> challenge: measure against live user_balances -> R2 CONFIRMED (collector rank #1 in 6 reserves) -> challenge: quantify TVL leg share -> R3 CONFIRMED/medium (`17.77%` WxDAI reserve, `1.2%` headline).
- **C11**: R1 CONFIRMED -> R2 CONFIRMED (guard + refresh both skip dev) -> R3 CONFIRMED/low (stale `balance_usd=0` proves not refreshed).
- **C12**: R1 CONFIRMED -> R2 CONFIRMED (`max(date)-7` functionally equal) -> R3 CONFIRMED/low (all three sub-findings verbatim).
- **C13**: R1 CONFIRMED (`tests: []`) -> challenge: prove missing tests catch live defect -> R2 CONFIRMED/medium (range catches util>1000; grain catches `06-18` dup) -> challenge: non-null on diffs_daily -> R3 CONFIRMED/medium (diffs keying clean/latent; util+grain real).
- **C14**: R1 CONFIRMED -> challenge -> R2 CHANGED (APY/volume semantic models now exist; rest uncovered) -> challenge: live MCP route + candidate refs -> R3 CHANGED/low (utilization/TVL/counts still raw-SQL-only; candidates unpruned).
- **C15**: R1 UNVERIFIABLE (ls too narrow) -> challenge: broaden grep -> R2 CONFIRMED (separate sDAI units, no joint sum) -> challenge: check overview rollups specifically -> R3 RESOLVED-as-docs-gap/low (no cross-sector aggregator).
- **C16**: R1 CHANGED (`29->14`; SparkLend WETH `101.38` present) -> challenge: index-timing vs accounting error -> R2 CONFIRMED (historical, served `98.07%`) -> challenge: other served consumer -> R3 RESOLVED/low (zero live reach).
- **C17**: R1 CONFIRMED (`0` supply null; borrow null grew) -> challenge: map null borrow to non-variable reserves -> R2 CONFIRMED (GNO/sDAI dominate) -> R3 CONFIRMED/low (refreshed counts; benign).
- **N01**: R2 NEW (discovered: `06-18` 2x partition) -> challenge: byte-identical? reachable? -> R3 CONFIRMED/medium (`310` value-conflict grains; unreachable, max date clean; needs forced merge/DELETE).

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P1 (KEEP/ESCALATE) | Rename or re-tag so `lenders_count_7d` (STOCK) and `borrowers_count_7d` (FLOW) are not sibling `*_count_7d`; fix borrowers `schema.yml` (lines `129-135`) to describe a 7-day FLOW not a STOCK | `marts/api_execution_lending_lenders_count_7d.sql`, `marts/api_execution_lending_borrowers_count_7d.sql`, `marts/fct_execution_lending_latest.sql` |
| P1 (KEEP) | Add a treasury/collector exclusion filter (`0x3e652e97...`, `0xb9e6dbfa...`); replace half-up rounding (line `277`) with rayDiv | `intermediate/int_execution_lending_aave_diffs_daily.sql`, `marts/fct_execution_lending_top_lenders_ranked.sql`, `marts/api_execution_lending_tvl_by_token_latest.sql` |
| P1 (ADD - NEW) | Force-merge or DELETE the doubled `2026-06-18` partition (`310` value-conflict grains); add grain-uniqueness test to prevent recurrence | `intermediate/int_execution_lending_aave_user_balances_daily.sql` |
| P2 (KEEP) | Add grain / range / non-null tests to the three Int256 intermediates; reconcile against on-chain aToken/variable-debt totalSupply | `intermediate/int_execution_lending_aave_{diffs,user_balances,utilization}_daily.sql` |
| P2 (KEEP) | Fix `tvl_by_token` schema contract: document `protocol` column, correct the "aggregated across protocols" description (output is per-protocol) | `marts/api_execution_lending_tvl_by_token_latest.sql`, `marts/schema.yml` |
| P2 (KEEP) | Load the Agave ABI (P0-07) to decode `contracts_agave_LendingPool_events`, then wire Agave (Deposit, not Supply) into the pipeline; document the exclusion meanwhile | `intermediate/int_execution_lending_aave_daily.sql`, `marts/api_execution_lending_tvl_by_token_latest.sql` |
| P3 (KEEP - latent) | Add `AND cumulative_scaled_borrow >= toInt256(0)` guard before `toUInt256(...)` (line `287`); rekey RepayWithATokens to `decoded_params['user']` (line `125`) | `intermediate/int_execution_lending_aave_utilization_daily.sql`, `intermediate/int_execution_lending_aave_diffs_daily.sql` |
| P3 (KEEP) | Add a dbt source freshness / elementary volume_anomaly test on the aaveV3/spark Pool-events sources | `intermediate/int_execution_lending_aave_daily.sql` + sources |
| P3 (KEEP) | Resolve top_lenders promotion conflict: either promote (drop `dev`, add `production`) or remove tier1/granularity:latest/elementary intent | `marts/fct_execution_lending_top_lenders_{ranked,latest}.sql`, `marts/api_execution_lending_top_lenders_latest.sql` |
| P4 (KEEP) | Idiom cleanup: `max(date)-7` -> `subtractDays`; remove duplicate `'lending','lending'` tags; revisit `week AS date` no_grain_col allowlist | `marts/fct_execution_lending_top_lenders_latest.sql`, `marts/fct_execution_lending_{latest,weekly}.sql`, weekly api marts |
| P4 (KEEP) | Add semantic models for utilization/TVL/counts/cohorts/top-lenders; prune or curate the two auto-gen weekly APY candidate metrics | `semantic/authoring/execution/lending/semantic_models.yml` |
| P4 (KEEP - latent) | Switch `fct_execution_lending_weekly` bitmap extraction to `toUInt64(groupBitmapMerge(...))` inline for idiom consistency (current `toUInt64(state)` is correct; do NOT use `bitmapCardinality` — errors code 43) | `marts/fct_execution_lending_weekly.sql` |
| P5 (DROP) | C01/C04/C06/C07/C16 production manifestations RESOLVED by recovery — drop the live-incident framing; retain only the latent-code notes (folded into P3 above) | n/a |
| P5 (DROP) | C15 sDAI double-count not realized in-repo — close as a docs note (document the lending/savings scope boundary) | n/a |
