# Model review (revisit 2026-06-21): execution/transactions

Baseline `docs/model_review/execution-transactions.md` (dated 2026-06-11), 15 cases re-verified over 3 rounds. Headline: 2 resolved (`C04` staleness recovered, `C09` confirmed non-defect), 3 changed severity (`C07` and `C10` downgraded, `C08` upgraded on a newly-found broken semantic dimension), and 10 still confirmed including the critical `C01` Int32 gas-price truncation that still reaches the tier-1 gas API.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONTRANSACTIONS-C01 | - | `gas_price_avg`/`gas_price_median` CAST to Int32 truncate sub-1-Gwei txs to 0 while fee>0; schema declares Float64; served to tier-1 gas API | critical | CONFIRMED | critical | high | none | 3 |
| EXECUTIONTRANSACTIONS-C02 | - | `unique_addresses` carries unmerged duplicate rows (append on ReplacingMergeTree, no FINAL); conflicting `first_seen_date` | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONTRANSACTIONS-C03 | - | `by_project_hourly_recent` full rebuild with `[max-2,max)` window can drop newest hours across UTC midnight | high | CONFIRMED | high | medium | none | 3 |
| EXECUTIONTRANSACTIONS-C04 | - | `by_project_daily` 4d stale, `info_daily` 2d stale; Elementary freshness did not alert | medium | RESOLVED | resolved | high | microbatch_insert_overwrite | 3 |
| EXECUTIONTRANSACTIONS-C05 | - | `by_project_daily` (delete+insert) vs `by_project_alltime_state` (insert_overwrite) can desync on late corrections; no reconciliation guard | medium | CONFIRMED | medium | medium | none | 3 |
| EXECUTIONTRANSACTIONS-C06 | - | `daily_active_addresses` hard-codes 181-day window; 90D prev window grazes the floor with no boundary test | medium | CONFIRMED | medium | medium | none | 3 |
| EXECUTIONTRANSACTIONS-C07 | - | `by_project_hourly_recent` issues SYSTEM DROP CACHE pre-hooks before every rebuild on shared CH Cloud | medium | CHANGED | low | high | none | 3 |
| EXECUTIONTRANSACTIONS-C08 | - | schema.yml documents phantom columns: `address` on two int models, `max_date` on two snapshot marts | low | CHANGED | medium | high | none | 3 |
| EXECUTIONTRANSACTIONS-C09 | - | Gas-share partial-day denominator: verified NOT a defect (`tot` CTE filter matches numerator) | low | RESOLVED | resolved | high | none | 3 |
| EXECUTIONTRANSACTIONS-C10 | - | `fee_usd_sum` silently equals native amount when xDAI price missing via `coalesce(px.price,1.0)` | high | CHANGED | low | high | none | 3 |
| EXECUTIONTRANSACTIONS-C11 | - | `change_pct` returns -100% instead of NULL when prior window is 0 (both snapshot models) | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONTRANSACTIONS-C12 | - | API-tag typo `transactions_coun_per_project_top5` (missing 't'); two overlapping top5 semantic models | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONTRANSACTIONS-C13 | - | `by_project_monthly_top5` silently excludes current partial month; undocumented; diverges from daily/weekly | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONTRANSACTIONS-C14 | - | 'Unknown' project/sector share unmonitored; no upper-bound alert on labelling regression | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONTRANSACTIONS-C15 | - | No reconciliation test across the three active-account counting paths | low | CONFIRMED | low | high | none | 3 |

Roll-up: 10 CONFIRMED, 2 RESOLVED, 3 CHANGED, 0 NEW, 0 UNVERIFIABLE/UNRESOLVED.

## Delta vs baseline

### RESOLVED (2)
- `C04` — `int_execution_transactions_by_project_daily` and `int_execution_transactions_info_daily` both now reach `max(date)=2026-06-20` (fresh to yesterday, the expected `block_timestamp < today()` boundary), versus baseline `2026-06-07` (4d stale) and `2026-06-09` (2d stale). Per-month day-counts are contiguous and complete across the entire insert_overwrite blast radius: `2025-06..2025-12` show `30/31/31/30/31/30/31` days at `7.41M/7.64M/7.03M/6.17M/6.09M/5.90M/5.40M` txs, and `2026-01..2026-06` show `31/28/31/30/31/20` days at `6.1M-9.0M` txs/mo with no wiped or zero months. `2026-06-14` (the incident-B logs-gap date) is present with `318k` txs / `61` projects. `elementary.freshness_anomalies` is now declared at `severity:error` on both models. Attribution: `microbatch_insert_overwrite` incident family; the prior silent freshness pass was because the table was being wiped/rebuilt, not stalled.
- `C09` — Confirmed non-defect, as the baseline closed it. `api_execution_transactions_gas_share_by_project_daily.sql` `tot` CTE (line 13) and outer numerator (line 22) both filter `WHERE date < today()` on the same source. Data corroboration: for `2026-06-20`, `SUM(value)=99.99` across 56 projects — proving denominator and numerator share the identical filtered day-set, so the shares close to 100%.

### CHANGED (3)
- `C07` (medium -> low) — The pre_hook has exactly `4` node-local `SYSTEM DROP CACHE` statements (MARK / UNCOMPRESSED / COMPILED EXPRESSION / QUERY at `int_execution_transactions_by_project_hourly_recent.sql:8-13`), not the baseline's `12` cluster-wide. None carry `ON CLUSTER`, so the blast radius is confined to the single CH Cloud compute node the build runs on. They still fire on every cron rebuild (the model is `materialized=table`, tag `hourly`, rebuilt on the cron observability path, not the 45s live loop), so it is a low-severity recurring local degradation.
- `C08` (low -> medium) — All four phantom schema.yml columns persist (`address` on `int_execution_transactions_by_project_daily` and `..._hourly_recent` at `intermediate/schema.yml:174,228`; `max_date` on both snapshot marts at `marts/schema.yml:1033,1068`). But this is more than doc-only: the semantic layer declares a live categorical dimension `address` (`expr: address`) on BOTH int models (`semantic_models.yml:288-290` and `328-330`) over models that project no `address` column — a metric query grouping by that dimension fails at SQL resolution. `max_date` remains a harmless doc orphan with no semantic consumer.
- `C10` (high -> low) — `coalesce(px.price, 1.0)` fallback unchanged (`int_execution_transactions_info_daily.sql:103`), `525` rows still have `fee_usd_sum == fee_native_sum > 0` (baseline `533`). But materiality is far lower than baseline's `~9%`: xDAI is a USD-pegged stablecoin trading `0.9994-1.0005` on the affected dates, so the `1.0` fallback ~= true price and the true understatement is `~0.06%`. All `525` rows end at `2026-05-01`, none in the live-served window; fees are xDAI-denominated exclusively (the `px` CTE filters `upper(symbol)='XDAI'`).

### STILL CONFIRMED (10)
- `C01` (critical) — Lines 75-76 still `CAST(... / 1e9 AS Int32)`; `1,509` rows have `gas_price_avg=0 & fee>0` (baseline `1,477`) and `2,793` have `gas_price_median=0 & fee>0` (baseline `2,722`), now spanning all tx types 0-4 not just type-4. schema.yml (lines 84-90) still declares both Float64. Served row-grain on `2026-06-20`: type-4 `gas_price_avg=0, gas_price_median=0, fee_native_sum=0.0084` over `265` txs. A Float64 forward fix is safe — the only consumers are `api_execution_transactions_gas_used_daily.sql` (pure pass-through), `api_execution_transactions_gas_used_weekly.sql` (gas_price lines commented out), and `int_execution_mmm_controls_weekly.sql:71` (float `avg()`, only improved by sub-1 floats).
- `C02` (high) — `count()=4,478,093` vs `uniqExact(address_hash)=4,302,454`, `175,639` unmerged duplicate rows (baseline gap `209,108`, shrank via background merges). Model still `append` on ReplacingMergeTree ordered by `address_hash` with no FINAL/OPTIMIZE. Proof the duplicates do NOT leak: `cumulative_daily` all-time `cumulative_accounts = 4,302,454` = `uniqExact` (not the inflated `4,478,093`); both consumers (`int_execution_transactions_cumulative_daily.sql`, `fct_execution_network_retention_monthly.sql`) grain-collapse via `GROUP BY address_hash`.
- `C03` (high) — Watermark/window logic unchanged (`int_execution_transactions_by_project_hourly_recent.sql:53-69`, exclusive `< max_day` boundary). Currently healthy: `max(hour)=2026-06-20 23:00 UTC` while source `max(block_timestamp) ~2026-06-21 18:xx UTC` — all ~18 of today's hours are intentionally absent by design and the newest served day is fully present. Latent midnight-crossing race remains; blast radius is three tier-1 hourly sector marts that re-aggregate without re-filtering the hour window.
- `C05` (medium) — Both materialization configs unchanged; `alltime_state` reads `FROM` `by_project_daily` and re-aggregates monthly. Reconciliation is exact today (`max abs diff = 0` for every settled month; the only non-zero is the current partial June, an expected exclusion). The desync is genuinely possible: a correction landing >1 month back is only picked up if both models are explicitly re-scoped with matching `start_month`/`end_month`; otherwise `alltime_state` stays stale. No CI/lineage reconciliation guard exists (grep of `tests/` found only consensus/account tests).
- `C06` (medium) — `WHERE date > subtractDays(today(), 181)` unchanged (`int_execution_transactions_daily_active_addresses.sql:15`). Horizon `179` days (`2025-12-23..2026-06-20`). The 90D prev_aa window needs `date > 2025-12-22`, satisfied exactly by the first retained day `2025-12-23` — currently fully covered (prev distinct `161,037`, curr `124,837`, served 90D change_pct `-22.5%`), but margin is exactly 1 day with no boundary test.
- `C11` (medium) — `(coalesce(curr/nullIf(prev,0),0)-1)*100` unchanged in both snapshot models. `68` served rows carry `change_pct=-100` with `value>0`, ALL in `fct_execution_transactions_by_project_snapshots` (chain-level `fct_execution_transactions_snapshots` has 0). Distribution: 1D=7, 7D=25, 30D=15, 90D=21 — concentrated in 7D/90D where new/dormant projects most often have a zero prior window.
- `C12` (medium) — `api_execution_transactions_by_project_monthly_top5.sql:4` still tags `api:transactions_coun_per_project_top5` (missing 't'). `check_api_tags.py` passes it (no canonical-key dictionary — only structural rules). Two semantic models claim the top5 concept with the shared synonym `execution transactions by project monthly top5` (`semantic_models.yml:363` and `388`); the canonical key `transactions_count_per_project_top5` routes to nothing.
- `C13` (medium) — `WHERE date < toStartOfMonth(today())` unchanged (`fct_execution_transactions_by_project_monthly_top5.sql:24`). Monthly `max(date)=2026-05-01` while daily reaches `2026-06-20` and weekly `2026-06-14` — the entire partial June (1-20) is missing from monthly charts. All three api monthly_top5 siblings inherit the exclusion (internally consistent), but it is undocumented and diverges from daily/weekly grain.
- `C14` (low) — Row-share Unknown `1.89%` (unchanged from baseline). Volume-weighted is far larger: all-time `36.42%`, trailing-14-day `76.07-88.44%` (mean ~80, stddev ~4.4pp, stable no-spike). `int_crawlers_data_labels` schema.yml has only `elementary.schema_changes` (severity warn) — no volume/coverage alert, so a labelling-coverage drop shifting volume to Unknown would not trip any test.
- `C15` (low) — Three counting methods unchanged (groupBitmapMerge / cityHash64 first-seen / countDistinct over bitmapToArray) with no reconciliation test. Numerically they agree at all-time grain: `cumulative_daily` `4,302,454` = `groupBitmapMerge` over all `by_project_daily` bitmaps `4,302,454` (0.00% gap); 90D window paths (a)/(b) both `124,442`. The open gap is purely the absence of an automated reconciliation test.

### NEW (0)
None.

### UNVERIFIABLE / UNRESOLVED (0)
None.

## Evidence appendix

**C01** — `int_execution_transactions_info_daily.sql:75-76`, schema.yml:84-90.
```sql
SELECT countIf(gas_price_avg=0 AND fee_native_sum>0),
       countIf(gas_price_median=0 AND fee_native_sum>0),
       countIf(gas_price_avg=0 AND fee_native_sum>0 AND transaction_type='4')
FROM dbt.int_execution_transactions_info_daily
```
Returned: `1,509` (avg=0 & fee>0), `2,793` (median=0 & fee>0), `694` (type-4 subset). Served grain: `2026-06-20` type-4 `gas_price_avg=0, gas_price_median=0, fee_native_sum=0.0084`, 265 txs; `2026-06-18` 0/0 fee `0.0202` (793 txs); `2026-06-17` 0/0 fee `0.0186` (350 txs). `describe_table` confirms columns are Int32 at relation level. Consumer grep: 3 references, none with integer arithmetic / `=0` filter / join on these columns.

**C02 / C15** (shared relation).
```sql
SELECT count(), uniqExact(address_hash), count()-uniqExact(address_hash)
FROM dbt.int_execution_transactions_unique_addresses
```
Returned: `4,478,093`, `4,302,454`, `175,639`. Cross-path: `cumulative_daily` all-time `cumulative_accounts=4,302,454` = `groupBitmapMerge` over all `by_project_daily` bitmaps `4,302,454` (gap 0.00%). 90D: path (a) `124,442` = path (b) `124,442`.

**C03** — `int_execution_transactions_by_project_hourly_recent.sql:53-69`.
```sql
SELECT min(hour), max(hour), count(), countDistinct(project)
FROM dbt.int_execution_transactions_by_project_hourly_recent
```
Returned: `min=2026-06-19 00:00`, `max=2026-06-20 23:00`, `1,444` rows, `64` projects. `now('UTC') ~2026-06-21 18:22`.

**C04** — staleness + monthly continuity.
```sql
SELECT max(date) FROM int_execution_transactions_by_project_daily;   -- 2026-06-20
SELECT max(date) FROM int_execution_transactions_info_daily;         -- 2026-06-20
SELECT toStartOfMonth(date), uniqExact(date), sum(tx_count)
FROM int_execution_transactions_by_project_daily
WHERE date >= '2025-06-01' GROUP BY 1 ORDER BY 1
```
Returned: `2025-06..2025-12` day-counts `30/31/31/30/31/30/31` (txs `7.41M..5.40M`); `2026-01..2026-06` `31/28/31/30/31/20` (txs `6.1M-9.0M`). `2026-06-14` present (`318k` txs, 61 projects). `elementary.freshness_anomalies severity:error` declared on both models.

**C05** — reconciliation across all months.
```sql
WITH bpd AS (SELECT toStartOfMonth(date) mo, sum(tx_count) txs FROM int_execution_transactions_by_project_daily WHERE date<toStartOfMonth(today()) GROUP BY mo),
     amt AS (SELECT toStartOfMonth(month) mo, sumMerge(txs_state) txs FROM int_execution_transactions_by_project_alltime_state GROUP BY mo)
SELECT mo, bpd.txs, amt.txs, abs(bpd.txs-amt.txs) d FROM bpd FULL OUTER JOIN amt USING(mo) ORDER BY d DESC LIMIT 5
```
Returned: max abs diff `0` for all settled months (e.g. `2023-12: 27,492,804 = 27,492,804`); only non-zero is partial June `6,440,873` (expected exclusion boundary). No cross-model reconciliation test in `tests/` or schema.yml.

**C06**.
```sql
SELECT min(date), max(date), dateDiff('day', min(date), max(date))
FROM int_execution_transactions_daily_active_addresses
```
Returned: `2025-12-23`, `2026-06-20`, `179`. 90D prev_aa distinct `161,037`, curr `124,837`; served change_pct `(124,837/161,037 - 1)*100 = -22.5%`. 181-day floor min(date) `2025-12-23` exactly satisfies the prev window's `date > 2025-12-22`.

**C07** — code only. `int_execution_transactions_by_project_hourly_recent.sql:8-13`: 4 statements `SYSTEM DROP MARK CACHE`, `SYSTEM DROP UNCOMPRESSED CACHE`, `SYSTEM DROP COMPILED EXPRESSION CACHE`, `SYSTEM DROP QUERY CACHE`; none carry `ON CLUSTER`.

**C08** — code only. Phantom columns: `intermediate/schema.yml:174,228` (`address`), `marts/schema.yml:1033,1068` (`max_date`). `describe_table(by_project_daily)` returns 7 columns (`date, project, sector, tx_count, ua_bitmap_state, gas_used_sum, fee_native_sum`), no `address`. Semantic dimension `address` declared at `semantic_models.yml:288-290` and `328-330`.

**C09**.
```sql
SELECT date, round(sum(value),2), count() FROM api_execution_transactions_gas_share_by_project_daily
WHERE date=(SELECT max(date) FROM api_execution_transactions_gas_share_by_project_daily) GROUP BY date
```
Returned: `2026-06-20`, `99.99`, `56` projects. `tot` CTE line 13 / numerator line 22 both `WHERE date < today()`.

**C10**.
```sql
SELECT countIf(fee_usd_sum=fee_native_sum AND fee_native_sum>0),
       maxIf(date, fee_usd_sum=fee_native_sum AND fee_native_sum>0)
FROM dbt.int_execution_transactions_info_daily
```
Returned: `525` rows, max date `2026-05-01`. xDAI price on affected dates `0.9994-1.0005`; understatement `~0.06%`. `coalesce(px.price,1.0)` at line 103; `px` CTE filters `upper(symbol)='XDAI'`.

**C11**.
```sql
SELECT 'by_project', window, label, count() FROM fct_execution_transactions_by_project_snapshots
WHERE change_pct=-100 AND value>0 GROUP BY window, label
```
Returned: `68` total (1D=7, 7D=25, 30D=15, 90D=21); chain-level `fct_execution_transactions_snapshots` = 0. Examples: Nfts2me 1D Transactions value=1; Qidao 1D FeesNative value=0.000193; Shutter 1D ActiveAccounts value=1.

**C12** — code only. `api_execution_transactions_by_project_monthly_top5.sql:4` tag `api:transactions_coun_per_project_top5`. `semantic_models.yml:363` and `388` both carry synonym `execution transactions by project monthly top5`. `check_api_tags.py` validates structure only (no canonical-key dictionary).

**C13**.
```sql
SELECT max(date) FROM fct_execution_transactions_by_project_monthly_top5;  -- 2026-05-01
SELECT max(date) FROM api_execution_transactions_gas_used_daily;           -- 2026-06-20
SELECT max(date) FROM api_execution_transactions_by_sector_weekly;         -- 2026-06-14
```
`WHERE date < toStartOfMonth(today())` at `fct_execution_transactions_by_project_monthly_top5.sql:24`; `toStartOfMonth(today())=2026-06-01`.

**C14**.
```sql
SELECT countIf(project='Unknown')/count(),
       sumIf(tx_count,project='Unknown')/sum(tx_count)
FROM int_execution_transactions_by_project_daily
```
Returned: row-share `1.89%`; all-time volume-share `36.42%`. Trailing-14-day volume-Unknown per day `76.07-88.44%` (min `76.07` on 2026-06-20, max `88.44` on 2026-06-12). `int_crawlers_data_labels` schema.yml: only `elementary.schema_changes` (warn).

## Review log (>=3 rounds per case)

- **C01**: R1 CONFIRMED critical (avg0 1,477->1,509, med0 2,722->2,793, scope widened to all tx types) -> challenge: corroborate served-API propagation + row-grain -> R2 CONFIRMED (tier-1 view passes columns through; 2026-06-20/18/17 row-grain shown) -> challenge: confirm no consumer relies on Int32 (fix safety) -> R3 CONFIRMED (3 consumers, none integer-dependent; Float64 safe).
- **C02**: R1 CONFIRMED high (gap 209,108->175,639) -> challenge: first_seen_date conflict + self-heal -> R2 CONFIRMED (cumulative_daily uses min() GROUP BY; self-healing) -> challenge: does any served output leak duplicates -> R3 CONFIRMED (both consumers grain-collapse; served count `4,302,454` not inflated).
- **C03**: R1 CONFIRMED high (code unchanged, currently healthy 2 full UTC days) -> challenge: downstream blast radius -> R2 CONFIRMED (3 tier-1 sector marts inherit window) -> challenge: quantify trigger condition / cron near 00:00 UTC -> R3 CONFIRMED medium-conf (today's ~18 hours absent by design; no 00:00 cron found; latent).
- **C04**: R1 RESOLVED (both at 2026-06-20, 1-day boundary) -> challenge: full-window recovery + freshness tests -> R2 RESOLVED (Jan-Jun continuity complete; freshness_anomalies present) -> challenge: recovery below 2026-01 + threshold -> R3 RESOLVED (2025-06..2025-12 full; severity:error freshness declared).
- **C05**: R1 CONFIRMED medium (diff=0 for 2026-04/05; no guard) -> challenge: extend to all months + grep tests -> R2 CONFIRMED (max diff 0 all settled months; no reconciliation test) -> challenge: is desync genuinely possible on late corrections -> R3 CONFIRMED (>1-month-back correction leaves alltime_state stale unless re-scoped).
- **C06**: R1 CONFIRMED medium (181 literal, horizon 179) -> challenge: confirm 90D doesn't touch floor, identify at-risk window -> R2 CONFIRMED (curr safe; prev_aa grazes floor) -> challenge: quantify clip impact on served change_pct -> R3 CONFIRMED (prev fully covered today, change_pct -22.5%, margin exactly 1 day).
- **C07**: R1 CONFIRMED medium (4 not 12 statements) -> challenge: ON CLUSTER vs node-local + cron frequency -> R2 CONFIRMED low (node-local, no ON CLUSTER) -> challenge: size recurrence cost -> R3 CHANGED low (fires every cron rebuild, 4 node-local drops; downgraded from 12 cluster-wide).
- **C08**: R1 CONFIRMED low (4 phantom columns) -> challenge: confirm absent not NULL-served -> R2 CONFIRMED low (describe_table proves absent) -> challenge: any broken semantic consumer -> R3 CHANGED medium (`address` is a live semantic dimension on 2 models = broken resolution; `max_date` harmless orphan).
- **C09**: R1 CONFIRMED non-defect (filters match) -> challenge: corroborate with data -> R2 RESOLVED (SUM=99.99 across 56 projects) -> (no further challenge) -> R3 RESOLVED (re-measured, symmetric on date<today()).
- **C10**: R1 CONFIRMED high (533->525 rows, coalesce unchanged) -> challenge: corroborate ~9% materiality -> R2 CHANGED low (xDAI ~1.0 stablecoin, ~0.06% not 9%) -> challenge: any non-stablecoin fee token -> R3 CHANGED low (fees xDAI-only, newest equal-row 2026-05-01, none live-served).
- **C11**: R1 CONFIRMED medium (formula unchanged) -> challenge: confirm -100% in served data -> R2 CONFIRMED (Nfts2me/Qidao/Shutter prev=0, curr>0) -> challenge: size scope/frequency -> R3 CONFIRMED (68 served rows, 7D/90D concentrated).
- **C12**: R1 CONFIRMED medium (typo + dual semantic models) -> challenge: would CI guard catch it; which is canonical -> R2 CONFIRMED (check_api_tags.py passes typo; collision persists) -> challenge: MCP-resolution impact -> R3 CONFIRMED (canonical key routes to nothing; ambiguous synonym match).
- **C13**: R1 CONFIRMED medium (WHERE unchanged, undocumented) -> challenge: quantify gap vs daily/weekly -> R2 CONFIRMED (monthly 2026-05-01 vs daily 2026-06-20) -> challenge: consistency vs sibling monthly views -> R3 CONFIRMED (3 api monthly siblings uniformly exclude; divergent from daily/weekly grain).
- **C14**: R1 CONFIRMED low (1.888% row-share, no alert) -> challenge: corroborate by volume -> R2 CHANGED medium (volume-Unknown 77.69% vs 1.62% row-share, ~48x) -> challenge: stability + detectability -> R3 CONFIRMED low (76-88% stable no-spike structural baseline; no volume/coverage test).
- **C15**: R1 CONFIRMED low (3 paths, no test) -> challenge: reconcile on common window -> R2 CONFIRMED (90D paths a/b both 124,442) -> challenge: reconcile cumulative path at all-time grain -> R3 CONFIRMED (cumulative 4,302,454 = bitmap 4,302,454, 0.00% gap; only the test is missing).

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P1 (ESCALATE) | Change `gas_price_avg`/`gas_price_median` from `CAST(... AS Int32)` to Float64; verified safe — no consumer does integer arithmetic, `=0` filtering, or joins on these columns. Removes silent 0s on `1,509`/`2,793` rows reaching the tier-1 gas API. | `models/execution/transactions/intermediate/int_execution_transactions_info_daily.sql`, `models/execution/transactions/intermediate/schema.yml` |
| P2 (KEEP) | Add `OPTIMIZE TABLE ... FINAL` (post-hook) or convert to delete+insert so `unique_addresses` carries no unmerged duplicates; today neutralized downstream but fragile for any future naive consumer. | `models/execution/transactions/intermediate/int_execution_transactions_unique_addresses.sql` |
| P2 (KEEP) | Make the hourly window inclusive of the freshest complete hour (or add a boundary test) so a build crossing UTC midnight cannot drop the newest hours that propagate to 3 tier-1 sector marts. | `models/execution/transactions/intermediate/int_execution_transactions_by_project_hourly_recent.sql` |
| P2 (KEEP) | Fix the API-tag typo `api:transactions_coun_per_project_top5` -> `..._count_...`; mark one of the two overlapping top5 semantic models canonical to resolve ambiguous MCP routing. | `models/execution/transactions/marts/api_execution_transactions_by_project_monthly_top5.sql`, `semantic/authoring/execution/transactions/semantic_models.yml` |
| P2 (KEEP) | Fix the phantom `address` semantic dimension (remove or back with a real column) — it is a live broken resolution, not just stale docs. | `semantic/authoring/execution/transactions/semantic_models.yml` (lines 288-290, 328-330), `models/execution/transactions/intermediate/schema.yml` |
| P3 (KEEP) | Return NULL (not -100%) when the prior window is 0 in both snapshot models; currently mislabels `68` served new/dormant-project rows as total decline. | `models/execution/transactions/marts/fct_execution_transactions_snapshots.sql`, `models/execution/transactions/marts/fct_execution_transactions_by_project_snapshots.sql` |
| P3 (KEEP) | Add a cross-model reconciliation test between `by_project_daily` and `by_project_alltime_state`, and across the three active-account counting paths; both desync windows are unguarded today. | `int_execution_transactions_by_project_daily.sql`, `int_execution_transactions_by_project_alltime_state.sql`, `fct_execution_transactions_active_accounts_daily.sql`, `int_execution_transactions_cumulative_daily.sql` |
| P3 (KEEP) | Widen the `181`-day window margin (or add a boundary test) so the 90D prev_aa window cannot clip during a partial rebuild; margin is exactly 1 day. | `models/execution/transactions/intermediate/int_execution_transactions_daily_active_addresses.sql`, `models/execution/transactions/marts/fct_execution_transactions_snapshots.sql` |
| P4 (KEEP) | Document the current-month exclusion on monthly_top5 (and consider serving the partial month) to remove the unexplained gap vs daily/weekly. | `models/execution/transactions/marts/fct_execution_transactions_by_project_monthly_top5.sql`, `models/execution/transactions/marts/schema.yml` |
| P4 (KEEP) | Reduce `by_project_hourly_recent` cache drops to only the cache(s) actually needed (or remove); they fire every cron rebuild on the shared node. | `models/execution/transactions/intermediate/int_execution_transactions_by_project_hourly_recent.sql` |
| P4 (KEEP) | Add a volume-weighted upper-bound/coverage alert on the Unknown bucket; row-share monitoring would miss a volume regression (Unknown is ~76-88% by volume, unmonitored). | `int_execution_transactions_by_project_daily.sql`, `int_crawlers_data_labels` |
| P5 (KEEP, low) | Replace `coalesce(px.price, 1.0)` with NULL/flag on missing xDAI price for correctness hygiene; financially immaterial (~0.06%) but a real silent-fallback smell. | `models/execution/transactions/intermediate/int_execution_transactions_info_daily.sql`, `int_execution_token_prices_daily.sql` |
| P5 (KEEP, low) | Remove the harmless `max_date` doc orphans from snapshot mart schema.yml. | `models/execution/transactions/marts/schema.yml` |
| DROP | C04 staleness — RESOLVED: both models fresh to `2026-06-20`, full-history continuity intact, `freshness_anomalies severity:error` now declared. | `int_execution_transactions_by_project_daily.sql`, `int_execution_transactions_info_daily.sql` |
| DROP | C09 gas-share denominator — RESOLVED non-defect: `tot` CTE and numerator both `date < today()`, shares sum to `99.99%`. | `api_execution_transactions_gas_share_by_project_daily.sql` |
