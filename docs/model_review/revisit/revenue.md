# Model review (revisit 2026-06-21): revenue

Baseline `docs/model_review/revenue.md` (dated 2026-06-11), 19 cases re-verified over 4 rounds: **1 RESOLVED** (the critical Q1-2026 monthly coverage gap), **3 CHANGED/downgraded** (`REVENUE-C04`, `REVENUE-C13`, `REVENUE-C15`), and **14 STILL CONFIRMED** (including 2 high-severity reconciliation/semantic gaps and 3 high-severity implementation defects).

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| REVENUE-C01 | — | 3 daily fee models tagged `refill_append` omit `refill_safe_*` hooks (no `max_memory_usage=8e9` cap) → Code 241 OOM risk | high | CONFIRMED | high | high | none | 3 |
| REVENUE-C02 | — | `int_revenue_fees_weekly_per_user` uses banned `delete+insert` in prod, waivered in `no_delete_insert.allow` | high | CONFIRMED | high | high | none | 3 |
| REVENUE-C03 | — | `int_revenue_sdai_fees_daily` `INNER JOIN rates USING(date)` silently drops all rows on rate-missing days | high | CONFIRMED | high | high | none | 3 |
| REVENUE-C04 | — | NULL USD fees propagate from gnosis_app/gpay through COALESCE-less unified view | high | CHANGED | low | high | none | 3 |
| REVENUE-C05 | — | Weekly fct views are plain views over RMT under delete+insert, no FINAL/dedup | medium | CONFIRMED | medium | high | none | 3 |
| REVENUE-C06 | — | `int_revenue_fees_weekly_per_user` has `tests: []` — no uniqueness test on most complex model | medium | CONFIRMED | medium | high | none | 3 |
| REVENUE-C07 | — | Fan-in unified view + weekly model carry zero model-level tests | medium | CONFIRMED | medium | high | none | 3 |
| REVENUE-C08 | — | 28 `api_revenue_*` views globally allowlisted (56 entries), no typed columns — invisible to API guard | medium | CONFIRMED | medium | high | none | 3 |
| REVENUE-C09 | — | Holdings APYs + 10% sDAI DAO share are hardcoded Jinja constants, no audit trail; 10% unverified post-sUSDS | medium | CONFIRMED | medium | medium | none | 3 |
| REVENUE-C10 | — | ~31% of BRLA user-days have `balance_usd_total=0 AND fees=0` from `round(.,6)` dust truncation | medium | CONFIRMED | medium | high | none | 3 |
| REVENUE-C11 | — | `countIf(month_fees > 0)` under `WHERE month_fees >= 0.01` is dead code (always true) | low | CONFIRMED | low | high | none | 3 |
| REVENUE-C12 | — | Weekly per-user/totals descriptions omit gnosis_app from "all streams" phrasing (doc drift) | low | CONFIRMED | low | high | none | 3 |
| REVENUE-C13 | — | Weekly api_ views lack `window:rolling_52w` tag despite exposing `annual_rolling_fees` | low | CHANGED | low | medium | none | 3 |
| REVENUE-C14 | — | Monthly pipeline ~75% coverage gap; Q1 2026 unreportable (`toStartOfYear(month)` partition eviction) | critical | RESOLVED | resolved | high | other | 3 |
| REVENUE-C15 | — | GPay hardcodes settlement address; possible missing card-spend fees post-April-2025 router change | high | CHANGED | low | high | none | 4 |
| REVENUE-C16 | — | Cohorts use `>=0.01`, totals use `>=0.50` — ~16.9k sub-$0.50 user-months diverge, undocumented | high | CONFIRMED | high | high | none | 3 |
| REVENUE-C17 | — | Gnosis App stream absent from semantic layer; no `has_gnosis_app` dimension; MCP cannot reach it | high | CONFIRMED | high | high | none | 3 |
| REVENUE-C18 | — | Cross-stream cohorts use `include_below_one=true` while per-stream use `=false` — non-summable | medium | CONFIRMED | medium | high | none | 3 |
| REVENUE-C19 | — | `int_revenue_sdai_fees_daily` 79% zero-fee dust rows (data observation) | low | CONFIRMED | low | high | none | 3 |

Rollup: CONFIRMED 14, RESOLVED 1, CHANGED 3, UNVERIFIABLE/UNRESOLVED/NEW 0.

## Delta vs baseline

### RESOLVED (1)
- **REVENUE-C14** (was `critical`): The ~75% monthly coverage gap is gone. `fct_revenue_active_users_totals_monthly` now has `32` rows / `32` contiguous months (`2023-10-01` .. `2026-05-01`, zero gaps); Q1 2026 present (`2026-01`=`10,574`, `2026-02`=`10,056`, `2026-03`=`10,906` users). `fct_revenue_per_user_monthly` went `452k`/11mo → `1,164,875`/32mo; `fct_revenue_active_users_cohorts_monthly` `256` rows (8 cohorts × 32). Root cause fixed in `models/revenue/intermediate/int_revenue_fees_monthly_per_user.sql` L17: `partition_by` changed from `toStartOfYear(month)` to `'month'`, so partition grain now equals the `insert_overwrite` grain. **Incident attribution: `other`** — this is the `toStartOfYear` partition-eviction design defect, explicitly distinct from the June microbatch `insert_overwrite` wipe (incident A).

### CHANGED / downgraded (3)
- **REVENUE-C04** (`high` → `low`): The `728` gnosis_app NULL-fee rows are gone (`countIf(fees IS NULL)` on `int_revenue_gnosis_app_fees_daily` = `0` of `150,266`). Only `8` gpay GBPe NULLs remain (2024-01-08..15 token launch, of `1,367,051`); traced into `int_revenue_fees_weekly_per_user` where `sum(fees)` zeroes them (weekly GBPe NULL count = `0`) — harmlessly absorbed, not NULL-propagated. Residual: `int_revenue_fees_unified_daily` L37 still selects `d.fees` raw (no COALESCE), but its total live NULLs = `8`, all gpay/GBPe.
- **REVENUE-C13** (`low`, reclassified): Reframed from "convention violation" to unenforced suggestion. `check_api_tags.py` has no `window:` enforcement rule and `rolling_52w` is absent from the documented vocabulary (`POINT_GRANS` lists `last_7d`/`7d`/`30d`/`60d`/`rolling_180d`, not `rolling_52w`). The only repo-wide occurrence of `rolling_52w` is inside `docs/model_review/revenue.md` (the baseline itself).
- **REVENUE-C15** (`high` → `low`): The "GPay missing all card-spend fees since April-2025" data-loss claim is **disproven**. Model continuity (`int_revenue_gpay_fees_daily`) shows smooth growing volume across the cutover (`2025-03` `$6.24M` → `2025-04` `$6.60M` → `2025-05` `$8.86M` → `2025-06` `$9.54M`, full 28-31 day months, no cliff). On-chain `rpc_scan_logs` over the last ~7 days (blocks `46,689,600`→`46,816,527`, through `2026-06-21`) found `80,157` ERC-20 Transfers / `44,922` txs **to** the hardcoded settlement address `0x4822521e6135cd2599199c83ea35179229a172ee` — still the live sink. The Spender router `0xcff260...549b` handles Spend/card-auth events, a separate mechanism. Residual: latent unguarded single hardcoded address (`int_revenue_gpay_fees_daily.sql` L1).

### STILL CONFIRMED (14)
High severity:
- **REVENUE-C01** (`high`): `macros/db/refill_safe_hooks.sql` L29 emits `SET max_memory_usage = 8000000000`; all three daily fee models carry tag `refill_append` but hand-roll hooks that omit this OOM cap. `int_revenue_gpay_fees_daily` is most exposed (pre_hook = only `grace_hash` + `max_bytes_in_join`); holdings/sdai add the two spill thresholds but still omit the 8 GiB cap.
- **REVENUE-C02** (`high`): `int_revenue_fees_weekly_per_user.sql` L41 `incremental_strategy=('append' if start_month else 'delete+insert')` still resolves to `delete+insert` in prod; sole revenue model on `scripts/checks/no_delete_insert.allow` (L33); scheduled orchestration (`cron_preview.sh`, all `scripts/*.sh`) never passes `start_month` (only `refresh.py` manual backfills do).
- **REVENUE-C03** (`high`): `int_revenue_sdai_fees_daily.sql` L100 still `INNER JOIN rates r USING (date)` (gpay uses LEFT JOIN). Latent silent complete-row-drop. Historical exposure measured = `0` gap-days over full history (rate_max == fees_max == `2026-06-19`), so the code defect is genuine but with zero realized impact.
- **REVENUE-C16** (`high`): `fct_revenue_active_users_cohorts_monthly` L23 `WHERE month_fees >= 0.01` vs `fct_revenue_active_users_totals_monthly` L21 `countIf(month_fees >= 0.50)`. 2026-05: cohorts `27,655` vs totals `10,776`; sub-$0.50 gap = `16,879` user-months. Undocumented in `schema.yml`; latent reconciliation hazard (no consumer currently sums cohorts→totals, but both metrics published).
- **REVENUE-C17** (`high`): No `revenue_gnosis_app_cohorts_*` semantic model; `has_gnosis_app` dimension absent from both `revenue_per_user_weekly` (L16-27) and `revenue_per_user_monthly` (L61-72) in `semantic/authoring/revenue/semantic_models.yml`. Live `query_metrics(revenue_per_user_monthly_users, dim=has_gnosis_app)` failed; `discover_metrics('gnosis app revenue cohort users')` returned zero revenue-module gnosis_app cohort metrics.

Medium severity:
- **REVENUE-C05** (`medium`): `fct_revenue_per_user_weekly` and `fct_revenue_active_users_cohorts_weekly` are `materialized='view'` with no FINAL/`deduplicate` over RMT `int_revenue_fees_weekly_per_user` (order_by/unique_key = `(week,stream_type,symbol,user)`). `dup_excess` = `0` today (merges caught up); real-but-transient.
- **REVENUE-C06** (`medium`): `models/revenue/intermediate/schema.yml` L319 `tests: []` for `int_revenue_fees_weekly_per_user`; grep across `models/` and `tests/` found no uniqueness test for it anywhere. Gap total; grain is unique today (latent).
- **REVENUE-C07** (`medium`): `int_revenue_fees_unified_daily` (schema.yml L199-221) and weekly model carry zero model-level tests. Upstream daily intermediates carry `not_null` only on `date`; `stream_type`/`user`/`symbol`/`fees` untested, so the fan-in is genuinely the first failure point at query time.
- **REVENUE-C08** (`medium`): `scripts/checks/check_api_tags.allow` has `56` `api_revenue_*` entries (28 views × `{columns_missing, no_grain_col}`); `marts/schema.yml` api_ views are description-only (no typed `columns:` block). Both rules suppressed per view → fully invisible to the guard.
- **REVENUE-C09** (`medium`): `int_revenue_holdings_fees_daily.sql` L1-4 (EURe/USDC.e `0.0000096`, BRLA `0.0000561349`, ZCHF `0.0000136646`) and `int_revenue_sdai_fees_daily.sql` L1 (`dao_share_pct = 0.1`) are flat Jinja constants. `seeds/savings_xdai_regimes.csv` tracks the DAI/sDAI→USDS/sUSDS switch at `2025-11-07` but records no DAO revenue-share %; the flat 10% remains unverifiable for the post-Nov-2025 sUSDS regime (held as open economic question, not retracted).
- **REVENUE-C10** (`medium`): `int_revenue_holdings_fees_daily` BRLA: `960,090` of `3,124,121` rows (30.7%) have `balance_usd_total = 0 AND fees = 0` (baseline `917k`/`2.99M` = 31%). `round(sum(balance_usd),6)` (L117) truncates dust; `WHERE balance_usd > 0` (L57) does not exclude it. Inflation confined to intermediate-grain row count — no served-metric leak (per-stream/cross-stream floors drop fees=0 rows).
- **REVENUE-C18** (`medium`): `fct_revenue_active_users_cohorts_weekly.sql` L19 `cohort_bucket_yearly(..., include_below_one=true)` + `WHERE annual_rolling_fees > 0` emits a `<1` bucket; all per-stream weekly cohorts use `include_below_one=false` + `WHERE annual_rolling_fees >= 1`. Measured for week `2026-05-25`: cross-stream `<1` bucket = `100,452` users vs summed per-stream `<1` = `0`. Non-summable; undocumented.

Low severity:
- **REVENUE-C11** (`low`): `countIf(month_fees > 0)` under `WHERE month_fees >= 0.01` in all 5 monthly cohort models (cross-stream + gnosis_app/gpay/holdings/sdai). Family-wide copy-paste dead code; no numeric effect.
- **REVENUE-C12** (`low`): `marts/schema.yml` L242-243 (`fct_revenue_per_user_weekly`) and L320-321 (`fct_revenue_active_users_totals_weekly`) enumerate "holdings + sDAI + gpay" and omit gnosis_app though SQL exposes `has_gnosis_app`. Internal fct_ docs only; api_ descriptions avoid enumeration.
- **REVENUE-C19** (`low`): `int_revenue_sdai_fees_daily` zero-fee rows = `18,646,524` of `23,503,982` (79.33%). Pure data observation; dust never reaches a served count (fails all downstream `>0`/`>=1`/`>=0.01` floors).

### NEW (0) / UNVERIFIABLE or UNRESOLVED (0)
None.

## Evidence appendix

**REVENUE-C01** (code_only): `macros/db/refill_safe_hooks.sql` L28-33 — `refill_safe_pre_hook` emits `SET max_memory_usage=8000000000; SET max_bytes_before_external_group_by=2000000000; SET max_bytes_before_external_sort=2000000000; SET join_algorithm='grace_hash'`. `int_revenue_holdings_fees_daily` (tag L29) / `int_revenue_sdai_fees_daily` (tag L26): pre_hook = `grace_hash` + `max_bytes_in_join=5e8` + 2 spill thresholds, **no** `max_memory_usage`. `int_revenue_gpay_fees_daily` (tag L26): pre_hook = only `grace_hash` + `max_bytes_in_join=5e8` (omits 3 of 4).

**REVENUE-C02** (code_only): `int_revenue_fees_weekly_per_user.sql` L41 `incremental_strategy=('append' if start_month else 'delete+insert')`; `scripts/checks/no_delete_insert.allow` L33 = `model.gnosis_dbt.int_revenue_fees_weekly_per_user`; `grep -rn 'delete+insert' models/revenue` = exactly 1 hit; grep of `cron_preview.sh` / `scripts/*.sh` for `start_month` = empty.

**REVENUE-C03** (sql): `SELECT (SELECT max(date) FROM dbt.int_yields_sdai_rate_daily WHERE rate IS NOT NULL) AS rate_max, (SELECT max(date) FROM dbt.int_revenue_sdai_fees_daily) AS fees_max, (SELECT count() FROM (SELECT DISTINCT b.date FROM (SELECT DISTINCT date FROM dbt.int_execution_tokens_balances_daily WHERE symbol='sDAI' AND balance_usd>0 AND date<today()) b LEFT ANTI JOIN (SELECT DISTINCT date FROM dbt.int_yields_sdai_rate_daily WHERE rate IS NOT NULL) r ON b.date=r.date)) AS missing` → `rate_max=2026-06-19`, `fees_max=2026-06-19`, `missing=0`. Code L100 still `INNER JOIN rates r USING (date)`.

**REVENUE-C04** (sql): `SELECT stream_type, countIf(fees IS NULL) AS null_fees, count() AS total FROM dbt.int_revenue_fees_unified_daily GROUP BY stream_type` → holdings=`0`, sdai=`0`, gpay=`8`, gnosis_app=`0`. `int_revenue_gnosis_app_fees_daily` NULL=`0` of `150,266`; `int_revenue_gpay_fees_daily` NULL=`8` of `1,367,051`. Unified view L37 selects `d.fees` raw (no COALESCE).

**REVENUE-C05 / REVENUE-C06** (sql, shared): `SELECT count() AS total_rows, count() - uniqExact(week, stream_type, symbol, user) AS dup_excess FROM dbt.int_revenue_fees_weekly_per_user WHERE week >= toDate('2026-01-01')` → `total_rows=2,950,975`, `dup_excess=0`. Both fct views `materialized='view'`, no FINAL. `schema.yml` L319 `tests: []`.

**REVENUE-C07** (code_only): `schema.yml` L199-221 (`int_revenue_fees_unified_daily`) = 5 typed columns, no model- or column-level `tests:`. Daily intermediates: `not_null` only on `date` (L25,72,117,166) + one model-level `dbt_utils.unique_combination_of_columns` each.

**REVENUE-C08** (code_only): `grep ^api_revenue_ scripts/checks/check_api_tags.allow` = `56` lines (L132-187). `marts/schema.yml` api_ entries (e.g. `api_revenue_per_user_weekly` L281-285) = description-only, no `columns:`.

**REVENUE-C09** (code_only): `int_revenue_holdings_fees_daily.sql` L1-4 + `int_revenue_sdai_fees_daily.sql` L1 (`dao_share_pct=0.1`). `seeds/savings_xdai_regimes.csv` = DAI/sDAI→USDS/sUSDS @ `2025-11-07T18:07:25Z`, same vault `0xaf20...3701`; no DAO-share %. `search_docs` returned no sUSDS DAO-share statement.

**REVENUE-C10** (sql): `SELECT countIf(symbol='BRLA' AND balance_usd_total=0) AS brla_zero, countIf(symbol='BRLA') AS brla_total, countIf(symbol='BRLA' AND balance_usd_total=0 AND fees=0) AS brla_zero_both FROM dbt.int_revenue_holdings_fees_daily` → `brla_zero=960,090`, `brla_total=3,124,121`, `brla_zero_both=960,090`. `round(sum(balance_usd),6)` L117; `WHERE balance_usd>0` L57.

**REVENUE-C11** (code_only): 5 models with `countIf(month_fees > 0)` under `WHERE month_fees >= 0.01`: cross-stream (L21/L23), gnosis_app (L12/L15), gpay (L13/L16), holdings (L13/L16), sdai (L12/L15).

**REVENUE-C12** (code_only): `marts/schema.yml` L242-243 + L320-321 enumerate "holdings + sDAI + gpay", omit gnosis_app; `has_gnosis_app` exposed at col L259.

**REVENUE-C13** (code_only): `check_api_tags.py` `POINT_GRANS` (L27-28) lists `last_7d/7d/30d/60d/rolling_180d`, not `rolling_52w`; no `window:` enforcement rule. `rolling_52w` repo-wide only in `docs/model_review/revenue.md`.

**REVENUE-C14** (sql): `SELECT 'totals',count(),uniqExact(month),min(month),max(month) FROM dbt.fct_revenue_active_users_totals_monthly UNION ALL SELECT 'per_user',count(),uniqExact(month),... UNION ALL SELECT 'cohorts',count(),...` → totals `32`/`32` months (`2023-10-01`..`2026-05-01`), per_user `1,164,875`/`32`, cohorts `256`/`32`. `int_revenue_fees_monthly_per_user.sql` L17 `partition_by='month'`.

**REVENUE-C15** (sql + rpc): model continuity `SELECT toStartOfMonth(date), round(sum(volume_usd),0), round(sum(fees),2) FROM dbt.int_revenue_gpay_fees_daily WHERE date>='2025-01-01' GROUP BY 1` → `Mar 6.24M / Apr 6.60M / May 8.86M / Jun 9.54M` (no cliff). `rpc_scan_logs` Transfer to `0x4822...172ee`, blocks `46,689,600`→`46,816,527` → `80,157` logs / `44,922` txs, address still active.

**REVENUE-C16** (sql): `SELECT (SELECT sum(users_cnt) FROM dbt.fct_revenue_active_users_cohorts_monthly WHERE month='2026-05-01') AS cohorts_users, (SELECT users_cnt FROM dbt.fct_revenue_active_users_totals_monthly WHERE month='2026-05-01') AS totals_users, (SELECT count() FROM (SELECT user, sum(month_fees) AS mf FROM dbt.int_revenue_fees_monthly_per_user WHERE month='2026-05-01' GROUP BY user HAVING mf>=0.01 AND mf<0.50)) AS sub_half_gap` → `cohorts=27,655`, `totals=10,776`, `gap=16,879`. Code: cohorts `>=0.01` (L23) vs totals `>=0.50` (L21).

**REVENUE-C17** (mcp_tool): `query_metrics(revenue_per_user_monthly_users, dim=has_gnosis_app)` → `Semantic execution unavailable: manifest_hash_mismatch` (and `has_gnosis_app` not a declared dimension). `discover_metrics('gnosis app revenue cohort users')` → 0 revenue-module gnosis_app cohort metrics. `semantic_models.yml` L16-27/L61-72 declare only `has_holdings/has_sdai/has_gpay/is_revenue_active`.

**REVENUE-C18** (code_only + sql): `fct_revenue_active_users_cohorts_weekly.sql` L19 `include_below_one=true` + L23 `WHERE annual_rolling_fees>0`; per-stream cohorts `include_below_one=false` + `WHERE annual_rolling_fees>=1` (holdings L11/16, sdai L10/15, gpay L11/16, gnosis_app L10/15). Week `2026-05-25`: cross-stream `<1` bucket = `100,452` users vs summed per-stream `<1` = `0`.

**REVENUE-C19** (sql): `SELECT countIf(fees=0) AS zero_fee, count() AS total, round(countIf(fees=0)/count(),4) AS share FROM dbt.int_revenue_sdai_fees_daily` → `zero_fee=18,646,524`, `total=23,503,982`, `share=0.7933`.

## Review log (>=3 rounds per case)

- **REVENUE-C01**: R1 CONFIRMED (hand-rolled SETs, not macro calls) → challenge: rank OOM exposure across the 3 → R2 CONFIRMED (gpay most exposed, omits 3 of 4 settings) → challenge: quote macro's exact SETs, is `max_memory_usage` real? → R3 CONFIRMED (macro L29 sets `8e9`, all three omit it). Settled high.
- **REVENUE-C02**: R1 CONFIRMED (L41 delete+insert, sole allow entry) → challenge: prove waiver scope + only revenue model → R2 CONFIRMED (1 grep hit) → challenge: do scheduled runs pass start_month? → R3 CONFIRMED (cron never passes it). Settled high.
- **REVENUE-C03**: R1 CONFIRMED (INNER JOIN, gpay LEFT) → challenge: quantify latent risk / worst rate lag → R2 CONFIRMED (rate_max==fees_max today, latent) → challenge: historical worst-case gap-day count → R3 CONFIRMED (0 gap-days ever; defect genuine, exposure 0). Settled high.
- **REVENUE-C04**: R1 CHANGED (gnosis_app 728→0; 8 gpay NULLs; no COALESCE) → challenge: trace NULLs into weekly model → R2 CHANGED→low (NULLs zeroed by sum) → challenge: any non-GBPe stream feeding live NULL? → R3 CHANGED (8 total, all GBPe). Settled low.
- **REVENUE-C05**: R1 CONFIRMED (views, no FINAL) → challenge: live duplicate count → R2 CONFIRMED (0 dups, real-but-transient) → challenge: confirm RMT key = grain → R3 CONFIRMED (order_by = grain). Settled medium.
- **REVENUE-C06**: R1 CONFIRMED (`tests: []` L319) → challenge: run the uniqueness check → R2 CONFIRMED (0 violations, latent) → challenge: covered elsewhere? → R3 CONFIRMED (no test anywhere). Settled medium.
- **REVENUE-C07**: R1 CONFIRMED (no model-level tests) → challenge: distinguish no model-level vs no tests at all → R2 CONFIRMED (zero coverage of any kind) → challenge: are upstreams well-tested on fan-in columns? → R3 CONFIRMED (upstreams only not_null on date). Settled medium.
- **REVENUE-C08**: R1 CONFIRMED (56 entries, no typed columns) → challenge: prove guard fully bypassed → R2 CONFIRMED (both rules suppressed per view) → challenge: does semantic layer supply typing instead? → R3 CONFIRMED (semantic only partial, omits has_gnosis_app/n_streams). Settled medium.
- **REVENUE-C09**: R1 CONFIRMED (hardcoded Jinja) → challenge: substantiate/retract the post-sUSDS sub-claim → R2 CHANGED→low (regime tracked upstream via seed) → challenge: verify 10% for both regimes → R3 CONFIRMED (10% unverifiable for sUSDS, held medium as open economic question). Settled medium.
- **REVENUE-C10**: R1 CONFIRMED (~31% zero-both) → challenge: trace downstream inflation → R2 CHANGED→low (refuted: filtered before active counts) → challenge: check per-stream holdings cohort paths → R3 CONFIRMED (medium, intermediate-grain inflation, no served leak). Settled medium.
- **REVENUE-C11**: R1 CONFIRMED (dead predicate) → challenge: any 0<mf<0.01 admitted? → R2 CONFIRMED (46,503 excluded before countIf, harmless) → challenge: single-model or family pattern? → R3 CONFIRMED (5 models share it). Settled low.
- **REVENUE-C12**: R1 CONFIRMED (weekly docs omit gnosis_app) → challenge: check monthly counterparts → R2 CONFIRMED (drift isolated to weekly) → challenge: do api_ descriptions inherit the drift? → R3 CONFIRMED (api_ avoid enumeration; internal-only). Settled low.
- **REVENUE-C13**: R1 CONFIRMED (no window tag) → challenge: is the tag required by the codified convention? → R2 CHANGED (unenforced; not in vocabulary) → challenge: read the authoritative convention doc → R3 CHANGED (rolling_52w undocumented + unenforced, suggestion). Settled low.
- **REVENUE-C14**: R1 RESOLVED (32 contiguous months, partition_by='month') → challenge: prove fix durable, check downstream cohorts → R2 RESOLVED (sdai cohorts 256 rows full coverage) → challenge: confirm all monthly marts contiguous → R3 RESOLVED (totals/per_user/cohorts all 32 months). Settled resolved, attribution `other`.
- **REVENUE-C15**: R1 RESOLVED (continuity confirmed) → challenge: close router-boundary residual → R2 CHANGED→low (router sends 0 direct settlement transfers) → challenge: confirm no future-migration guard → (R3 implicit) → R4 CHANGED→low (on-chain 80,157 transfers last 7 days, data-loss claim disproven). Settled low (4 rounds).
- **REVENUE-C16**: R1 CONFIRMED (dual threshold, 16,879 gap) → challenge: is the mismatch documented? → R2 CONFIRMED (undocumented, weekly pair too) → challenge: does any consumer reconcile cohorts→totals? → R3 CONFIRMED (no consumer, but both published; latent hazard). Settled high.
- **REVENUE-C17**: R1 CONFIRMED (no semantic model/dimension) → challenge: prove MCP-surface harm → R2 CONFIRMED (registry read) → challenge: live query_metrics failure → R3 CONFIRMED (manifest_hash_mismatch + 0 discovered metrics). Settled high.
- **REVENUE-C18**: R1 CONFIRMED (include_below_one true vs false) → challenge: numeric non-additivity demo → R2 CONFIRMED (structural mechanism) → challenge: measure the divergence → R3 CONFIRMED (week 2026-05-25: 100,452 vs 0). Settled medium.
- **REVENUE-C19**: R1 CONFIRMED (79.33% dust) → challenge: prove no downstream distortion → R2 CONFIRMED (filtered before counts) → challenge: confirm served count from non-zero rows only → R3 CONFIRMED (filter chain proves 0 leakage). Settled low.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P1 (ESCALATE) | Replace hand-rolled SET hooks with `refill_safe_pre_hook()`/`refill_safe_post_hook()` so the `max_memory_usage=8e9` OOM cap is applied; gpay is most exposed | `models/revenue/intermediate/int_revenue_holdings_fees_daily.sql`, `int_revenue_sdai_fees_daily.sql`, `int_revenue_gpay_fees_daily.sql` |
| P1 (KEEP) | Document the dual `>=0.01` (cohorts) vs `>=0.50` (totals) threshold in `schema.yml`, or unify; analysts summing cohorts get a ~2.5x overcount (16,879 sub-$0.50 user-months for 2026-05) | `models/revenue/marts/fct_revenue_active_users_cohorts_monthly.sql`, `fct_revenue_active_users_totals_monthly.sql` |
| P1 (KEEP) | Add `revenue_gnosis_app_cohorts_weekly/monthly` semantic models and a `has_gnosis_app` dimension to both per-user semantic models so MCP can reach the stream | `semantic/authoring/revenue/semantic_models.yml`, `fct_revenue_gnosis_app_cohorts_weekly.sql`, `fct_revenue_gnosis_app_cohorts_monthly.sql` |
| P2 (KEEP) | Migrate `delete+insert` to an allowed strategy and remove the `no_delete_insert.allow` waiver | `models/revenue/intermediate/int_revenue_fees_weekly_per_user.sql`, `scripts/checks/no_delete_insert.allow` |
| P2 (KEEP) | Convert the sDAI rates `INNER JOIN` to `LEFT JOIN` + COALESCE/NULL sentinel to remove the latent silent complete-row-drop (0 realized impact, but no guard) | `models/revenue/intermediate/int_revenue_sdai_fees_daily.sql` |
| P2 (KEEP) | Add `SELECT ... FINAL` (or `deduplicate`) to the weekly fct views and a `unique_combination_of_columns` test on the weekly model | `models/revenue/marts/fct_revenue_per_user_weekly.sql`, `fct_revenue_active_users_cohorts_weekly.sql`, `models/revenue/intermediate/schema.yml` |
| P2 (KEEP) | Document/unify the cross-stream `include_below_one=true` vs per-stream `=false` cohort floor (non-summable `<1` bucket: 100,452 vs 0 for week 2026-05-25) | `models/revenue/marts/fct_revenue_active_users_cohorts_weekly.sql` + per-stream weekly cohort marts |
| P3 (KEEP) | Add model-level tests to the fan-in unified view + not_null/accepted_values on `stream_type`; add COALESCE on `fees` in the unified view | `models/revenue/intermediate/int_revenue_fees_unified_daily.sql`, `models/revenue/intermediate/schema.yml` |
| P3 (KEEP) | Move holdings APYs and the 10% sDAI DAO share to dbt vars/seed with effective-dates; verify the 10% share for the post-Nov-2025 sUSDS regime (open economic question) | `models/revenue/intermediate/int_revenue_holdings_fees_daily.sql`, `int_revenue_sdai_fees_daily.sql` |
| P3 (KEEP) | Add typed `columns:` blocks to `api_revenue_*` views and remove the 56 `check_api_tags.allow` entries | `models/revenue/marts/schema.yml`, `scripts/checks/check_api_tags.allow` |
| P4 (KEEP) | Add a volume/freshness continuity assertion on the GPay settlement address so a future migration is caught (continuity proven through 2026-06; residual is the unguarded single hardcode) | `models/revenue/intermediate/int_revenue_gpay_fees_daily.sql` |
| P4 (KEEP, low) | Clean up: remove dead `countIf(month_fees > 0)` predicate (5 models); add gnosis_app to weekly per-user/totals descriptions; exclude BRLA dust rows from the intermediate row count | monthly cohort marts; `models/revenue/marts/schema.yml`; `int_revenue_holdings_fees_daily.sql` |
| DROP | C14 monthly coverage gap — resolved by `partition_by='month'`; remove from open-issues list | `models/revenue/intermediate/int_revenue_fees_monthly_per_user.sql` |
| DROP | C15 "GPay missing card-spend fees" data-loss claim — disproven on-chain + in model; downgrade to the P4 hardcode-guard item above | `models/revenue/intermediate/int_revenue_gpay_fees_daily.sql` |
| DROP | C04 high-severity NULL-propagation — gnosis_app 728→0, residual is 8 harmless zeroed GBPe NULLs; fold into the P3 COALESCE cleanup | `models/revenue/intermediate/int_revenue_fees_unified_daily.sql` |
| DROP | C13 `window:rolling_52w` tag — not in the documented vocabulary nor guard-enforced; non-finding/suggestion only | `models/revenue/marts/schema.yml` |
