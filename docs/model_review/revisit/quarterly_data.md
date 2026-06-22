# Model review (revisit 2026-06-21): quarterly_data

Re-verification of baseline [`docs/model_review/quarterly_data.md`](../quarterly_data.md) (dated `2026-06-11`) across `20` cases over `3` rounds. Headline: `0` resolved, `4` CHANGED (all from one ESG carbon-source backfill, all down-graded to low), `16` STILL CONFIRMED including both `high`-severity completeness/peak-guard defects; `0` new issues.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| QUARTERLYDATA-C01 | | `peak_swappers` reads daily fct with no `date<today()` guard (unlike 3 monthly siblings) | high | CONFIRMED | high | high | none | 3 |
| QUARTERLYDATA-C02 | | Zero dbt data tests across all 8 `quarterly_data` schema.yml | medium | CONFIRMED | medium | high | none | 3 |
| QUARTERLYDATA-C03 | P0-01 | ESG fallback `CROSS JOIN last_existing_date` → silent zero rows if source empty | medium | CONFIRMED | medium | high | other/both | 3 |
| QUARTERLYDATA-C04 | | `tokens_included` hardcoded CASE string, drifts from `tokens_whitelist.csv` | medium | CONFIRMED | medium | high | none | 3 |
| QUARTERLYDATA-C05 | | `argMax(is_estimated, date)` collapses mixed-flag quarters to worst-case | low | CONFIRMED | low | high | none | 3 |
| QUARTERLYDATA-C06 | | No `is_complete`/`is_partial`/`quarter_end_date` on any of 22 marts | high | CONFIRMED | high | high | none | 3 |
| QUARTERLYDATA-C07 | | ESG uses `toStartOfMonth` cutoff vs non-ESG `date<today()` → 20-day window mismatch | medium | CONFIRMED | medium | high | none | 3 |
| QUARTERLYDATA-C08 | | xDAI (STABLECOIN) lands in non-USD bucket; peg_class undocumented | medium | CHANGED | low | high | none | 3 |
| QUARTERLYDATA-C09 | | `gpay_active_users` serves `max(mau)` but endpoint name omits "peak" | medium | CHANGED | low | high | none | 3 |
| QUARTERLYDATA-C10 | | No semantic authoring for any of 20 tier0 quarterly endpoints | medium | CONFIRMED | medium | high | none | 3 |
| QUARTERLYDATA-C11 | | ESG carbon coverage starts late vs other subsectors; start undocumented | low | CHANGED | low | high | other/both | 3 |
| QUARTERLYDATA-C12 | | Stablecoin holders are token-level (double-count); caveat only on holders model | low | CONFIRMED | low | high | none | 3 |
| QUARTERLYDATA-C13 | | `circles_active_trusts` = 7 rows, max 2026-Q2, 0 null keys | low | CONFIRMED | low | high | none | 3 |
| QUARTERLYDATA-C14 | | `int_quarterly_stablecoin_cohorts_stats` = 383 rows, 24 quarters, 0 dup on true grain | low | CONFIRMED | low | high | none | 3 |
| QUARTERLYDATA-C15 | | `gnosis_app_peak_swappers` = 3 rows, max 2026-Q2 | low | CONFIRMED | low | high | none | 3 |
| QUARTERLYDATA-C16 | | `staked_gno` = 19 rows | low | CONFIRMED | low | high | none | 3 |
| QUARTERLYDATA-C17 | P0-01 | `carbon_emissions` = 3 rows, 2 `is_estimated=True` | low | CHANGED | low | high | other/both | 3 |
| QUARTERLYDATA-C18 | | ESG fallback Q2-2026 = 61 rows (Apr+May only) | low | CHANGED | low | high | other/both | 3 |
| QUARTERLYDATA-C19 | | Rising `staked_gno/validators_active` ratio is EIP-7251 consolidation, not a bug | low | CONFIRMED | low | high | none | 3 |
| QUARTERLYDATA-C20 | | `staked_gno` = `argMax(effective_balance,date)/32`, matches `staked_latest` | low | CONFIRMED | low | high | none | 3 |

Rollup: confirmed `16`, changed `4`, resolved `0`, new `0`, unverifiable/unresolved `0`.

## Delta vs baseline

### RESOLVED (0)
None. No defect from the baseline was fixed in code; the four moved cases moved because of an upstream data backfill, not a model change.

### CHANGED (4) — all attributable to ONE ESG carbon-source backfill
All four trace to a single upstream change: `fct_esg_carbon_footprint_uncertainty` grew from December-only `31` rows to `182` rows spanning `2025-12-01`..`2026-05-31`. No `quarterly_data` model SQL changed; the `argMax`/`CASE`/`CROSS JOIN` logic is byte-for-byte identical to baseline. Not related to the June `insert_overwrite` partition-wipe incident.

- **QUARTERLYDATA-C08** — Severity down `medium → low`. The inflation half is disproven: `symbol='xDAI'` has `0` rows / `0` holders / `0` supply across ALL history in `fct_execution_tokens_metrics_daily` (it is the native gas token, not an ERC-20 here), so it cannot inflate non-USD aggregates. The documentation half remains open: `schema.yml` `peg_class` description still reads `"Stablecoin peg classification: 'USD-pegged' or 'non-USD'."` with no xDAI mention. (Attribution: none — this is a status correction, not a data change.)
- **QUARTERLYDATA-C09** — Severity down `medium → low`. Aggregation still `max(mau)`, but `schema.yml` DOES document peak semantics (model desc `"Peak monthly active users for Gnosis Pay within the quarter."`; column desc `"Highest monthly active user count across the three months of the quarter."`). Only the endpoint tag `api:gpay_active_users` (the sole surface a name-only consumer sees) omits "peak". (Attribution: none — status correction.)
- **QUARTERLYDATA-C11** — Earliest carbon quarter shifted `Q3-2025 → Q4-2025` (Q4-2025 now fed by December only). Still `3`-quarter coverage vs circles `7` / cohorts `24` / validators `19`. `schema.yml` line 5 still has no coverage-start note. (Attribution: other/both — source backfill.)
- **QUARTERLYDATA-C17** — `is_estimated=True` count flipped `2 → 0` (all three rows `153.48`/`118.56`/`125.05` now real). Earliest quarter shifted `Q3-2025 → Q4-2025`. The flip is purely data-driven: every in-window month feeding the 3 quarters is now all-real; estimated rows exist only in the excluded current month (June). (Attribution: other/both — source backfill.)
- **QUARTERLYDATA-C18** — ESG fallback intermediate Q2-2026 grew `61 → 81` rows (Apr `30` real + May `31` real + Jun `20` estimated). The marts still serve only Apr+May via the `toStartOfMonth` cutoff. (Attribution: other/both — source backfill.)

### STILL CONFIRMED (16)
Two `high`-severity defects remain live:

- **QUARTERLYDATA-C01** (high) — `api_quarterly_data_gnosis_app_peak_swappers.sql` L20-25 still does `max(n_swappers)` over `fct_execution_gnosis_app_swaps_daily` GROUP BY quarter with NO `date < today()` guard, unlike the 3 monthly-sourced siblings. Currently DORMANT: today (`day20624`) `n_swappers=182`, below the Q2 peak `294` on `day20616` (a complete past day). Across the last 35 days no in-progress partial ever exceeded the running quarter max, so real overstatement likelihood is very low (only fires on quarter days 1-2 before any full day is recorded). The code defect persists.
- **QUARTERLYDATA-C06** (high) — No `is_complete`/`is_partial`/`quarter_end_date` on any of `22` mart files. Quantified on a SUM metric: `api_quarterly_data_gpay_volume` Q2-2026 serves `$19,799,305.64` (April `$9,668,818.19` + May `$10,130,487.45`, June absent) = `2` of `3` months, against a prior-4-quarter mean of `$29.66M` → `0.668`, i.e. `~33.2%` low, with no flag to disambiguate incompleteness from a real decline.

Medium-severity, still confirmed:

- **QUARTERLYDATA-C02** (medium) — `0` dbt unique/not_null/accepted_values tests across all 8 schema.yml. Broadened grain probes (`stablecoin_supply` `39=39`/0 null, `gpay_active_users` `12=12`/0, `validators_active` `19=19`/0, plus 4 earlier marts) prove a pure coverage gap masking no live integrity defect.
- **QUARTERLYDATA-C03** (medium) — `int_quarterly_esg_carbon_footprint_with_fallback.sql` (a view) still uses `CROSS JOIN last_existing_date` on both `node_distribution` and `client_efficiency_by_category` CTEs. If the source is empty, `max_date=NULL` → both UNION arms empty → zero rows, silent end-to-end (no dbt test, no elementary, no semantic surface). Currently non-empty (`182` rows) only because of the backfill.
- **QUARTERLYDATA-C04** (medium) — `tokens_included` is still a hardcoded CASE string in all 3 stablecoin marts (`supply.sql` L45-48, `holders.sql` L44-47, `transfers.sql` L30-34). The seed already carries STABLECOIN symbols the static label cannot represent: `xDAI` (absent) and `BRZ` (explicitly excluded via `symbol NOT IN ('BRZ')`). Any future seed STABLECOIN addition silently flows into aggregates but not the label.
- **QUARTERLYDATA-C07** (medium) — ESG marts use `toStartOfMonth(date) < toStartOfMonth(today())`; non-ESG use `date < today()`. Served-data gap proven: carbon Q2 max contributing date `day20604` (`2026-05-31`) vs circles Q2 `day20624` (`2026-06-20`) = exactly `20` days.
- **QUARTERLYDATA-C10** (medium) — No `quarterly_data/` authoring subdir, `0` grep hits for `api_quarterly_data` in `semantic/`, and `discover_metrics` returns only non-quarterly root_models. The 20 tier0 endpoints are unqueryable via the MCP metric layer.

Low-severity, still confirmed:

- **QUARTERLYDATA-C05** (low) — `argMax(is_estimated, date)` intact in both ESG marts (carbon L23, energy L23). Latent: estimated rows live only in June (excluded by cutoff). Flips live the moment `today()` rolls to `2026-07-01` and June's estimated tail becomes the last in-window date for 2026-Q2.
- **QUARTERLYDATA-C12** (low) — Holders mart is token-level. 2026-Q2 USD-pegged per-token max holders sum to `222,435` vs largest single token `84,159` (USDC) → upper-bound over-count `2.64x`. Double-count caveat only on the holders schema.yml (L36-37), absent on supply/transfers.
- **QUARTERLYDATA-C13** (low) — `7` rows, max 2026-Q2, `0` null/zero, `0` dup; source `fct_execution_circles_v2_active_trusts_daily` has the same `7` distinct quarters; `active_trusts` monotonic `1,285 → 414,746`.
- **QUARTERLYDATA-C14** (low) — `383` rows, `24` quarters, max 2026-Q2; true-grain `(quarter,balance_bucket,peg_class)` dup `0`; the `144` `(quarter,balance_bucket)` collisions are the 2 peg classes per bucket. Recent quarters each `20` rows = `10` buckets × `2` peg classes.
- **QUARTERLYDATA-C15** (low) — `3` rows, max 2026-Q2; served peaks `97`/`170`/`294` tie exactly to the daily-fct per-quarter max; Q2 peak `294` on `day20616` (complete past day).
- **QUARTERLYDATA-C16** (low) — `19` rows, max 2026-Q2, `0` null/zero, `0` dup; contiguous `2021-Q4`..`2026-Q2`.
- **QUARTERLYDATA-C19** (low) — Single clean slot `28376691` (`2026-06-07`): `107,054` validators `≤32 GNO` + `4,424` `>32 GNO`, max `2,048 GNO`, total `111,478` = exact `validators_active` Q2. Bimodal EIP-7251 consolidation, not a formula bug.
- **QUARTERLYDATA-C20** (low) — `staked_gno` Q2-2026 `334,875.9` (`round(argMax(effective_balance,date)/32,1)`, L22) matches `api_consensus_info_staked_latest.value` `334,875` to the integer; `334,875.9/111,478 = 3.004` GNO/validator, internally consistent with the consolidation ratio.

### NEW (0)
None.

### UNVERIFIABLE / UNRESOLVED (0)
None.

## Evidence appendix

### Code-only cases
- **C01 / C15** — `api_quarterly_data_gnosis_app_peak_swappers.sql` L20-25: `SELECT toStartOfQuarter(date), max(n_swappers) FROM fct_execution_gnosis_app_swaps_daily GROUP BY quarter` — no `WHERE date < today()` guard. The 3 siblings (swaps/swaps_filled/swap_volume) read `fct_execution_gnosis_app_swaps_monthly`. Daily fct: `day20624` (today) `n_swappers=182`; Q2 peak `294` on `day20616`; max date in quarter is today. Mart serves peaks `97`/`170`/`294` (3 rows, max quarter `20544`).
- **C02** — `grep -rnE 'tests:|unique|not_null|accepted_values' models/quarterly_data/**/schema.yml` → `0` data-test blocks across all 8 files (only the literal word "unique" in prose). Grain/null probes: `stablecoin_supply` `39` rows = `uniqExact(quarter,peg_class)=39`, `0` null; `gpay_active_users` `12=12`, `0`; `validators_active` `19=19`, `0`; plus `staked_gno`/`circles_active_trusts`/`carbon_emissions`/`cohorts_stats` all clean.
- **C03** — `int_quarterly_esg_carbon_footprint_with_fallback.sql` L3 view; L30-32 `last_existing_date = max(date)`; L44-45 and L56-57 `CROSS JOIN last_existing_date led WHERE nd.date > led.max_date`. If source empty → `max_date=NULL` → both UNION arms empty → zero rows, no error. `SELECT count(),min(date),max(date),uniqExact(toStartOfMonth(date)) FROM dbt.fct_esg_carbon_footprint_uncertainty` → `182` rows, `2025-12-01`..`2026-05-31`, `6` months.
- **C04 / C08** — `grep STABLECOIN seeds/tokens_whitelist.csv`: symbols `xDAI, EURe, GBPe, WxDAI, sDAI, USDC.e, USDC, USDT, BRLA, BRZ, ZCHF, svZCHF`. All 3 marts hardcode CASE `'WxDAI, sDAI, USDC, USDC.e, USDT'` / `'EURe, GBPe, BRLA, ZCHF, svZCHF'`. `SELECT count(),sum(holders),sum(supply_usd) FROM fct_execution_tokens_metrics_daily WHERE token_class='STABLECOIN' AND symbol='xDAI'` → `0`/`0`/`0` (all history). `peg_class` desc verbatim: `"Stablecoin peg classification: 'USD-pegged' or 'non-USD'."` (supply L13-14, holders L42-43, transfers L69-71).
- **C05 / C17** — `carbon_emissions.sql` L23 / `energy_consumption.sql` L23: `argMax(is_estimated, date)`; cutoff `WHERE toStartOfMonth(date) < toStartOfMonth(today())` (carbon/energy L25). `SELECT toStartOfMonth(date), countIf(is_estimated) FROM int_quarterly_esg_carbon_footprint_with_fallback WHERE date>=2026-04-01` → Apr `30`d/0 est, May `31`d/0 est, Jun `20`d/`20` est.
- **C09** — `api_quarterly_data_gpay_active_users.sql` L21-22 `max(mau) AS peak_monthly_active_users`; tags L4 `['production','quarterly_data','tier0','api:gpay_active_users','granularity:quarterly']` — the `api:` tag has no peak/max hint. schema.yml L35 model desc and L40-41 column desc both document "Peak"/"Highest".
- **C12** — `holders.sql` L30-31 `sum(holders)` token-level grain. Caveat only on `api_quarterly_data_stablecoin_holders` schema.yml L36-37.

### Warehouse cases
- **C06** — `SELECT quarter,volume_usd FROM api_quarterly_data_gpay_volume ORDER BY quarter DESC`: Q2-2026 `$19,799,305.64`; prior 4 quarters `$25.00M`/`$33.35M`/`$32.91M`/`$27.39M`, mean `$29.66M`; ratio `0.668`. `fct_execution_gpay_kpi_monthly`: April `$9,668,818.19` + May `$10,130,487.45`, June absent. No completeness column on any of 22 marts.
- **C07** — `SELECT max(date) FROM int_quarterly_esg_carbon_footprint_with_fallback WHERE Q2 AND toStartOfMonth(date)<toStartOfMonth(today())` → `20604` (`2026-05-31`); `SELECT max(date) FROM fct_execution_circles_v2_total_supply_daily WHERE Q2 AND date<today()` → `20624` (`2026-06-20`); gap `20` days.
- **C10** — `grep -rn api_quarterly_data semantic/` → `0`; `find semantic -type d -name 'quarterly*'` → none; `discover_metrics('gpay active users quarterly carbon emissions quarterly')` → 10 metrics, all non-quarterly root_models (e.g. `api_execution_gpay_active_users_7d`, `fct_execution_gpay_activity_daily`, `api_mixpanel_ga_overview_daily`).
- **C11** — `SELECT quarter,co2_tonnes_yr,is_estimated FROM api_quarterly_data_carbon_emissions ORDER BY quarter` → `3` rows: Q4-2025 (`20362`) `153.48`/false, Q1-2026 (`20454`) `118.56`/false, Q2-2026 (`20544`) `125.05`/false. Coverage spread: circles `7`, cohorts `24`, validators_active `19` distinct quarters.
- **C13** — `SELECT count, uniqExact(quarter), max(quarter), countIf(active_trusts IS NULL), countIf(active_trusts=0) FROM api_quarterly_data_circles_active_trusts` → `7`/`7`/`20544`/`0`/`0`. Values `1,285 / 7,352 / 92,541 / 181,724 / 279,563 / 302,330 / 414,746`.
- **C14** — `SELECT count, uniqExact(quarter), max(quarter), count-uniqExact((quarter,balance_bucket,peg_class)), count-uniqExact((quarter,balance_bucket)) FROM int_quarterly_stablecoin_cohorts_stats` → `383`/`24`/`20544`/`0`/`144`. 3 recent quarters each `20` rows = `10` buckets × `2` peg classes.
- **C16** — `SELECT count, max(quarter), countIf(staked_gno IS NULL OR staked_gno=0), count-uniqExact(quarter) FROM api_quarterly_data_staked_gno` → `19`/`20544`/`0`/`0`.
- **C18** — `SELECT max(date), countIf(date>=today()), countIf(date=today()), today() FROM int_quarterly_esg_carbon_footprint_with_fallback` → max `20624` (`2026-06-20`), today-rows `0`, exactly-today `0`, today `20625`. Q2 distribution Apr `30` + May `31` + Jun `20` est = `81` rows; mart excludes June. No off-by-one (max `< today`).
- **C19** — `SELECT slot, slot_timestamp, countIf(effective_balance<=32e9), countIf(effective_balance>32e9), max(effective_balance)/1e9, count() FROM stg_consensus__validators WHERE slot=28376691 AND status='active_ongoing'` → `2026-06-07`, `107,054` ≤32 GNO, `4,424` >32 GNO, max `2,048 GNO`, total `111,478`. Ratio progression `1.069 → 1.153 → 2.116 → 2.542 → 3.004`.
- **C20** — `SELECT staked_gno, validators_active FROM the two marts` Q2-2026: `334,875.9` and `111,478`; `334,875.9/111,478 = 3.004`; `api_consensus_info_staked_latest.value = 334,875` (as_of `2026-06-20`). `int_consensus_validators_balances_daily.effective_balance` documented as whole-GNO summed stake → `/32` = validator-equivalent.

## Review log (>=3 rounds per case)

- **C01** — R1 CONFIRMED (no guard; siblings read monthly fct) → challenge: quantify live impact. R2 CONFIRMED (today `182` vs Q2 peak `294`; dormant) → challenge: empirical activation likelihood over 30 days. R3 CONFIRMED (35-day window: no partial ever exceeded running max; overstatement essentially only on quarter days 1-2). Final high.
- **C02** — R1 CONFIRMED (`0` tests) → challenge: grain probe 3 marts. R2 CONFIRMED (4 marts clean) → challenge: broaden to supply/gpay/val_active. R3 CONFIRMED (all clean, pure coverage gap). Final medium.
- **C03** — R1 CONFIRMED (pattern present; source backfilled to `182`) → challenge: prove silent-zero failure mode. R2 CONFIRMED (SQL reasoning: NULL max_date → both arms empty) → challenge: any downstream alert? R3 CONFIRMED (wholly silent end-to-end; view, no test/elementary/semantic). Final medium.
- **C04** — R1 CONFIRMED (hardcoded in 3 marts) → challenge: prove live drift. R2 CONFIRMED (label still matches served set; latent) → challenge: list seed STABLECOINs absent from label. R3 CONFIRMED (xDAI, BRZ in seed, not representable). Final medium.
- **C05** — R1 CONFIRMED (`argMax` present) → challenge: trigger with current data. R2 CONFIRMED (est rows only in excluded June; latent) → challenge: state activation calendar. R3 CONFIRMED (flips at `2026-07-01` month boundary). Final low.
- **C06** — R1 CONFIRMED (`0` of 22 marts) → challenge: quantify on a sum metric. R2 CONFIRMED (gpay 2/3 months) → challenge: % under-count vs prior quarters. R3 CONFIRMED (`gpay_volume` `~33.2%` low). Final high.
- **C07** — R1 CONFIRMED (cutoff forms differ) → challenge: quantify served-data gap. R2 CONFIRMED (~20 days) → challenge: single side-by-side query. R3 CONFIRMED (`20604` vs `20624`, `20` days exact). Final medium.
- **C08** — R1 CONFIRMED (code-only) → challenge: check full-history xDAI rows. R2 CHANGED (xDAI `0` rows all history; inflation disproven) → challenge: quote peg_class desc. R3 CHANGED (desc omits xDAI; doc-half open). Final low.
- **C09** — R1 CONFIRMED (`max(mau)`, tag omits peak) → challenge: check schema.yml descriptions. R2 CHANGED (descriptions document peak) → challenge: verify endpoint tag. R3 CHANGED (`api:gpay_active_users` has no peak hint). Final low.
- **C10** — R1 CONFIRMED (no subdir, 0 grep) → challenge: discover_metrics. R2 CONFIRMED (returns non-quarterly only) → challenge: strongest discovery query. R3 CONFIRMED (no quarterly root_model surfaces). Final medium.
- **C11** — R1 CHANGED (earliest Q3-2025→Q4-2025) → challenge: quote coverage-start note; explain Q4. R2 CHANGED (no note; Dec-only feeds Q4) → challenge: cross-check 7/24/19 spread. R3 CHANGED (spread holds). Final low.
- **C12** — R1 CONFIRMED (token-level; caveat only on holders) → challenge: quantify over-count. R2 CONFIRMED (no address-level source) → challenge: bound from per-token data. R3 CONFIRMED (`2.64x` upper bound). Final low.
- **C13** — R1 CONFIRMED (`7`/2026-Q2/0 nulls) → challenge: grain integrity. R2 CONFIRMED (dup `0`, contiguous) → challenge: tie to upstream. R3 CONFIRMED (source `7` quarters, monotonic). Final low.
- **C14** — R1 CONFIRMED (`383`/`24`/true-grain dup `0`) → challenge: rows-per-quarter. R2 CONFIRMED (144 = 2 peg/bucket) → challenge: confirm recent quarters. R3 CONFIRMED (`20` = 10×2). Final low.
- **C15** — R1 CONFIRMED (`3`/2026-Q2) → challenge: tie to C01 peak date. R2 CONFIRMED (peak `294` complete past day) → challenge: confirm mart serves same values. R3 CONFIRMED (served peaks tie to fct max). Final low.
- **C16** — R1 CONFIRMED (`19`/2026-Q2) → challenge: grain/continuity. R2 CONFIRMED (dup `0`, contiguous `2021-Q4`..) → challenge: confirm no null/zero. R3 CONFIRMED (`0` null/zero). Final low.
- **C17** — R1 CHANGED (`is_estimated` 2→0; Q3→Q4) → challenge: confirm data-driven. R2 CHANGED (in-window months all-real) → challenge: confirm argMax byte-identical. R3 CHANGED (code unchanged; flip is backfill). Final low.
- **C18** — R1 CHANGED (`61`→`81` rows) → challenge: reconcile int-vs-mart. R2 CHANGED (mart excludes June) → challenge: check off-by-one. R3 CHANGED (max `20624` < today `20625`, no leak). Final low.
- **C19** — R1 CONFIRMED (ratio →3.0; consolidation) → challenge: validator-balance distribution. R2 CONFIRMED (max `2048`, avg >32) → challenge: single clean slot. R3 CONFIRMED (slot `28376691`: 107,054/4,424). Final low.
- **C20** — R1 CONFIRMED (`334,875.9` ≈ `334,875`) → challenge: confirm /32 from source. R2 CONFIRMED (`/32` of whole-GNO stake) → challenge: cross-surface reconciliation. R3 CONFIRMED (`3.004`/validator consistent). Final low.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P1 (KEEP) | Add a `date < today()` guard to the `peak_swappers` read from `fct_execution_gnosis_app_swaps_daily` to match the 3 monthly-sourced siblings, eliminating partial-day peak risk | `models/quarterly_data/gnosis_app/marts/api_quarterly_data_gnosis_app_peak_swappers.sql` |
| P1 (KEEP) | Add a completeness signal (`is_complete`/`is_partial`/`quarter_end_date`) to all quarterly marts so consumers can programmatically exclude in-progress quarters; `gpay_volume` Q2-2026 currently reads `~33%` low with no flag | all 22 marts under `models/quarterly_data/*/marts` |
| P2 (KEEP) | Unify ESG cutoff with the rest of the sector — replace `toStartOfMonth(date) < toStartOfMonth(today())` with `date < today()` (or vice-versa platform-wide) to close the `20`-day cross-subsector window mismatch | `models/quarterly_data/esg/marts/api_quarterly_data_carbon_emissions.sql`, `.../api_quarterly_data_energy_consumption.sql` |
| P2 (KEEP) | Add dbt `unique`/`not_null` tests on quarter (or true grain) for the 20 marts to lock in the currently-clean grain and catch future regressions | all 8 `models/quarterly_data/**/schema.yml` |
| P2 (KEEP) | Author semantic models / register the 20 tier0 quarterly endpoints so they are MCP-reachable via `query_metrics`/`discover_metrics` | `semantic/authoring/` (new `quarterly_data/` subdir) |
| P3 (KEEP) | Replace hardcoded `tokens_included` CASE strings with a dynamic `groupArray(distinct symbol)` so the label cannot drift from `tokens_whitelist.csv` (seed already carries unrepresentable `xDAI`, `BRZ`) | `.../stablecoins/marts/api_quarterly_data_stablecoin_{supply,holders,transfers}.sql` |
| P3 (KEEP) | Add a row-count / freshness guard on the ESG fallback view so the silent-zero failure mode (`CROSS JOIN last_existing_date` with empty source) surfaces an error instead of emitting empty results | `models/quarterly_data/esg/intermediate/int_quarterly_esg_carbon_footprint_with_fallback.sql` |
| P3 (KEEP) | Fix the latent `argMax(is_estimated, date)` worst-case collapse before `2026-07-01` (when June's estimated tail enters the window and stamps the whole 2026-Q2 row estimated); derive the flag from a share threshold instead of the last date | `.../esg/marts/api_quarterly_data_carbon_emissions.sql`, `.../api_quarterly_data_energy_consumption.sql` |
| P4 (KEEP, doc) | Document xDAI's would-be peg classification and add the double-count caveat to supply/transfers schema.yml (currently only on holders); document the ESG coverage-start quarter | `.../stablecoins/marts/schema.yml`, `.../esg/marts/schema.yml` |
| P4 (KEEP, doc) | Surface "peak" in the `gpay_active_users` endpoint identifier (the only surface a name-only consumer sees), e.g. rename the `api:` tag to `gpay_peak_active_users` | `.../gnosis_pay/marts/api_quarterly_data_gpay_active_users.sql` |

No DROP recommendations: nothing from the baseline was resolved in code. The four CHANGED cases were down-graded to low because of an upstream ESG carbon-source backfill, but their underlying code defects (silent-empty fallback, late coverage with no doc, `argMax` worst-case collapse) all persist and are retained above.
