# Model review (revisit 2026-06-21): consensus

Baseline `docs/model_review/consensus.md` (2026-06-11); 30 cases re-verified over 3 rounds. Headline: **0 resolved, 4 changed (all severity downgrades / mechanism corrections), 26 still confirmed** — the live `apy_30d` ~day-count inflation, the `/32` Staked GNO understatement, the unweighted APY KPI path, and the Pectra `0x02` cross-sector gap all remain in the served data.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | conf | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| CONSENSUS-C01 | P0-03 | `apy_30d` = SUM(income)/AVG(eff_bal)*365*100, no /window divisor (~day-count x inflation) | critical | CONFIRMED | critical | high | none | 3 |
| CONSENSUS-C02 | P0-04 | `int_consensus_validators_labels` is `tags=['dev']` + bare-table FROM; breaks withdrawal ref chain in prod | critical | CONFIRMED | critical | high | none | 3 |
| CONSENSUS-C03 | | `int_consensus_validators_status_daily` 4 ghost cols in schema.yml w/ column_anomalies tests | high | CONFIRMED | high | high | none | 3 |
| CONSENSUS-C04 | | `stg_consensus__validators` unique test on `validator_index` alone (grain is slot,vi) | high | CHANGED | medium | high | none | 3 |
| CONSENSUS-C05 | | `stg_consensus__withdrawals` unique tests on block_hash + validator_index (grain slot,withdrawal_index) | high | CONFIRMED | high | high | none | 3 |
| CONSENSUS-C06 | | `stg_consensus__blocks` unique test on `eth1_block_hash` (near-constant) | high | CONFIRMED | high | high | none | 3 |
| CONSENSUS-C07 | | `fct_consensus_info_latest` INNER JOIN info_7d drops new-from-zero status classes | high | CONFIRMED | high | medium | none | 3 |
| CONSENSUS-C08 | | `int_consensus_validators_withdrawal_addresses` 0x01-only CASE; 0x02 excluded | high | CONFIRMED | high | high | none | 3 |
| CONSENSUS-C09 | | performance views read RMT int models w/o FINAL; merge-window doubling risk | high | CONFIRMED | high | high | none | 3 |
| CONSENSUS-C10 | | `int_consensus_entry_queue_daily` 4 ghost cols; unique+not_null on validator_index | medium | CONFIRMED | medium | high | none | 3 |
| CONSENSUS-C11 | | `int_consensus_deposits_withdrawals_daily` typo CTEs + total_amount vs schema 'amount' | medium | CONFIRMED | medium | high | none | 3 |
| CONSENSUS-C12 | | `int_consensus_blocks_daily` schema documents wrong column set | medium | CONFIRMED | medium | high | none | 3 |
| CONSENSUS-C13 | | `int_consensus_validators_income_daily` INNER JOIN network_state; recent days truncated | medium | CHANGED | medium | high | other | 3 |
| CONSENSUS-C14 | | five `api_consensus_info_*_latest` as_of_date from depwith (fresher than value tables) | medium | CONFIRMED | medium | high | none | 3 |
| CONSENSUS-C15 | | `fct_consensus_info_latest` schema declares 5 cols, SQL projects 3 | medium | CONFIRMED | medium | high | none | 3 |
| CONSENSUS-C16 | | `stg_consensus__validators_all` description copies 'positive balance' wording (no filter) | medium | CONFIRMED | medium | high | none | 3 |
| CONSENSUS-C17 | | consensus api endpoints on check_api_tags allow list (no typed columns) | medium | CHANGED | medium | high | none | 3 |
| CONSENSUS-C18 | | `api_consensus_validators_status_daily` has no `meta.api` block | medium | CONFIRMED | medium | high | none | 3 |
| CONSENSUS-C19 | | `int_consensus_graffiti_daily` schema has garbage column entries | low | CONFIRMED | low | high | none | 3 |
| CONSENSUS-C20 | | api: tag typos ('dististribution', space-after-colon) | low | CONFIRMED | low | high | none | 3 |
| CONSENSUS-C21 | P0-03 | `api_consensus_forks` today() over static fork data | low | CONFIRMED | low | high | none | 3 |
| CONSENSUS-C22 | P0-03 | live dashboard serves apy_30d ~day-count x inflated (UI/members/MCP) | critical | CONFIRMED | critical | high | none | 3 |
| CONSENSUS-C23 | | Staked GNO 334k vs 10.7M (effective_balance/32) | high | CONFIRMED | high | high | none | 3 |
| CONSENSUS-C24 | | APY KPI card reads unweighted dists path, not balance-weighted mean | high | CONFIRMED | high | high | none | 3 |
| CONSENSUS-C25 | | 6,712 0x02 validators absent from cross-sector user_pseudonym graph | high | CONFIRMED | high | high | none | 3 |
| CONSENSUS-C26 | | 4-day lag, 6-day snapshot gap, all freshness tests severity:warn | high | CHANGED | medium | high | both | 3 |
| CONSENSUS-C27 | | `fct_consensus_info_latest` change_pct = -100% for new-from-zero classes | medium | CONFIRMED | medium | high | none | 3 |
| CONSENSUS-C28 | | `fct_consensus_validators_explorer_daily` INNER JOIN status_latest drops income-history validators | medium | CONFIRMED | medium | high | none | 3 |
| CONSENSUS-C29 | | `fct_consensus_validators_dists_last_30_days` single-row, described as 30-day series | medium | CONFIRMED | medium | high | none | 3 |
| CONSENSUS-C30 | | `fct_consensus_forks` hardcoded 7-tuple array; silent zero for future forks | medium | CONFIRMED | medium | high | none | 3 |

Totals: **26 CONFIRMED, 4 CHANGED, 0 RESOLVED, 0 NEW, 0 UNVERIFIABLE/UNRESOLVED.** (The orchestrator's last-round rollup counts C04 as CHANGED, making 4 CHANGED / 26 CONFIRMED.)

## Delta vs baseline

### RESOLVED (0)
None. No defect from the baseline has been fixed in code or in the served data.

### CHANGED (4) — all are downgrades or mechanism corrections, not fixes

- **CONSENSUS-C04** (high -> medium): Downgraded to a **latent** grain-declaration defect. The unique test on `validator_index` alone in `models/consensus/staging/schema.yml` is wrong-by-grain (true grain is `(slot, validator_index)`), but the source `consensus.validators` currently emits exactly one snapshot per day — `SELECT toDate(slot_timestamp), uniqExact(slot) ... GROUP BY d HAVING s>1` returned **0 rows** across retained history. The error-severity test passes today only because of the one-snapshot-a-day source cadence; it would fire on any multi-intraday-snapshot day.
- **CONSENSUS-C13** (high -> medium, incident=other): Mechanism pinned and reframed. Baseline framed an INNER-JOIN missing-day risk; the observed reality is a **partial-stage microbatch**. `int_consensus_validators_income_daily` for `2026-06-03..06-07` carries **only validators `500000-558312` (~58,312 rows)** while `int_consensus_validators_snapshots_daily` holds the full **~558,313** for the same days. This is the `500k-600k` validator_index slice having re-run alone — NOT the `insert_overwrite` REPLACE-PARTITION wipe (which hits whole months) and NOT an INNER JOIN on `network_state`. The blast radius: served `apy_30d`/income for validators `0-500000` is missing the last ~3 days.
- **CONSENSUS-C17** (still medium, count corrected): Baseline said "12 live endpoints"; it is **6 consensus endpoints with 12 allow-list entries** (2 rules each: `::columns_missing` + `::no_grain_col`) in `scripts/checks/check_api_tags.allow` lines 6-17. The exemption (no typed column schemas) is real; only the endpoint count was an entry/endpoint conflation.
- **CONSENSUS-C26** (high -> medium, incident=both): The specific **6-day snapshot gap is RESOLVED** by incident-A recovery — `int_consensus_validators_snapshots_daily` is now full contiguous **~558,297-558,313/day** through `2026-06-07` (no `06-01..06-06` absence, no `58,313` partial on `06-07`). BUT the underlying lag **worsened from 4 days to 14 days** (everything still pinned at `2026-06-07`, the consensus indexer source max), the income residual (partial-stage microbatch, C13) persists, and **all `freshness_anomalies` tests remain `severity: warn`**. Mixed outcome -> CHANGED. The income residual is expected to self-heal on the next full microbatch cycle (all 6 validator-index stages are declared in `range_template`).

### STILL CONFIRMED (26) — key served-data defects with proving numbers

- **CONSENSUS-C01 / CONSENSUS-C22** (critical): `apy_30d` inflation reproduced as exactly `countDistinct(date)`-x, not a fixed `/30`. Validator `4808`: `days_in_window=12`, `served_apy_30d=117.638`, `per_day_implied=9.803` (~true ~10% network APY), `9.803 x 12 = 117.6`. Served distribution in `fct_consensus_validators_explorer_latest`: `max=3,322,693.3%`, `p90=266.5%`. The API view `api_consensus_validators_explorer_latest` returns the **same** distribution (`n=3462, max=3,322,693.3%, p90=266.5%`) — inflated values reach the consumer unmodified.
- **CONSENSUS-C23** (high): `api_consensus_info_staked_latest` serves `value=334,875`; `int_consensus_validators_balances_daily.effective_balance` total = `10,716,030 GNO`; `10,716,030/32 = 334,875.94` (exact). `git blame -L 74,74 fct_consensus_info_latest.sql` -> commit `f34819b1` ("ESG and decoping updates", 2025-08-29) with **no rationale/comment** for the `/32`. Schema description (`schema.yml` l1320) still reads "total GNO currently staked" — factually wrong by 32x.
- **CONSENSUS-C24** (high): Served APY KPI = `8.8` from `int_consensus_validators_dists_daily.avg_apy=8.799` (unweighted), vs canonical `fct_consensus_validators_apy_mean_daily.apy=9.634` (balance-weighted) — **+9.49% divergence materialized** in the served row.
- **CONSENSUS-C14** (medium): `as_of_date` sourced from `int_consensus_deposits_withdrawals_daily` (max `2026-06-20`) while APY/staked/status values derive from tables maxing at `2026-06-07` — **13-day freshness overstatement** materialized; `api_consensus_info_staked_latest` serves `value=334,875` with `as_of_date=2026-06-20`.
- **CONSENSUS-C08 / CONSENSUS-C25** (high, Pectra gap): `6,712` validators carry `0x02` credentials; `int_consensus_validators_withdrawal_addresses` (CASE only `startsWith('0x01')`) contains **0** distinct `0x02` addresses; `fct_consensus_validators_withdrawal_addresses_distinct` = `873` rows, **none `0x02`**. The mart's `user_pseudonym` is the cross-sector join key (same `sipHash64` space as revenue/gpay/gnosis_app/Circles), so the `0x02` cohort never enters the graph.
- **CONSENSUS-C02** (critical): `int_consensus_validators_labels.sql` still `tags=['dev']` (l6) + bare `FROM stg_consensus__validators` (l14) in both FROM and `MAX(slot)` subquery; `int_consensus_validators_withdrawal_addresses.sql` l22 `ref()`s it. `dbt ls ... ,tag:production` returns **EMPTY** for labels; `+fct_consensus_validators_withdrawal_addresses_distinct,tag:production` returns only the mart itself. Active failure mode today is **stale-data** (object exists from a prior dev/full run; the view returns 873 rows), not missing-relation — a clean prod-only env would hit missing-relation.
- **CONSENSUS-C09** (high): `api_consensus_validators_performance_daily.sql` l115 / `_latest.sql` l81 read `int_consensus_validators_income_daily` (engine `ReplacingMergeTree()`) with no FINAL, no argMax/GROUP BY dedup, no uniqueness test. `0` dupes today (latent merge-window doubling risk).
- **CONSENSUS-C05 / C06** (high, grain): withdrawals last-7d `1,018,829` rows vs `127,431` unique block_hash / `91,265` unique validator_index (both error-severity unique tests fail; true grain `(slot, withdrawal_index)`). blocks 30d: `2` distinct `eth1_block_hash` vs `~494k` blocks (one real value `0x374bee...` 455,961 rows + one zero-default 38,738).
- **CONSENSUS-C03 / C10 / C11 / C12 / C15 / C19** (schema-contract ghosts): SQL projection != schema.yml. C03 and C10 carry real test stanzas on non-existent columns (C03 two `column_anomalies` severity:warn; C10 unique+not_null default-error on `validator_index`); C11/C12/C15/C19 are doc-only with no failing test.
- **CONSENSUS-C07 / C28 / C27** (latent join/arithmetic): C07 INNER JOIN unchanged, no new-from-zero label in retained history. C28 left-anti `income vs status_latest = 0` today (status_latest is current-snapshot-only, so a crawler gap would drop income-history validators). C27 `change_pct` deterministically yields `-100.0` for `t2=0,t1>0`; no live row hits it.
- **CONSENSUS-C16 / C18 / C20 / C21 / C29 / C30** (contract/wording/static-data hygiene): all confirmed unchanged at code level; C18 missing `meta.api` is uncaught because `check_api_tags.py` never reads `node['meta']`; C20 `res=t[4:]` publishes the misspelled/space-prefixed slug verbatim; C21 `api_consensus_forks` has no freshness test attached (monitor-confusion clause hypothetical); C30 live output is 7 forks `Phase0..Fulu` (silent-zero risk strictly future-fork).

### NEW (0)
None.

### UNVERIFIABLE / UNRESOLVED (0)
None.

## Evidence appendix

**CONSENSUS-C01 / C22 (apy_30d inflation = countDistinct(date)-x).** Round 3:
```sql
WITH mx AS (SELECT max(date) m FROM dbt.int_consensus_validators_income_daily)
SELECT validator_index,
       countDistinct(date) days_in_window,
       round(sum(consensus_income_amount_gno)/nullIf(avg(effective_balance_gno),0)*365*100,3) served,
       round((sum(consensus_income_amount_gno)/nullIf(avg(effective_balance_gno),0)*365*100)/nullIf(countDistinct(date),0),3) per_day
FROM dbt.int_consensus_validators_income_daily
WHERE date >= (SELECT m FROM mx)-INTERVAL 30 DAY AND effective_balance_gno>0
GROUP BY validator_index HAVING days_in_window BETWEEN 5 AND 20
```
Returned validator `4808`: `days_in_window=12`, `served=117.638`, `per_day=9.803`. C22 served-surface check:
```sql
SELECT 'table', median(apy_30d), max(apy_30d), quantile(0.9)(apy_30d) FROM dbt.fct_consensus_validators_explorer_latest
UNION ALL
SELECT 'api', median(apy_30d), max(apy_30d), quantile(0.9)(apy_30d) FROM dbt.api_consensus_validators_explorer_latest
```
Both rows: `n=3462, median=0, max=3,322,693.3, p90=266.5`. Code unchanged: `models/consensus/marts/fct_consensus_validators_explorer_latest.sql` l85-87 `SUM/NULLIF(AVG,0)*365*100`; `fct_consensus_validators_explorer_members_table.sql` l58-60 same SUMIf/AVGIf pattern.

**CONSENSUS-C02 (dev-tag prod exclusion).** `dbt ls --select <sel>,tag:production --resource-type model --output name`: `int_consensus_validators_labels,tag:production` = EMPTY; `+fct_consensus_validators_withdrawal_addresses_distinct,tag:production` = only `fct_consensus_validators_withdrawal_addresses_distinct`; sanity `stg_consensus__validators,tag:production` resolves. `models/consensus/intermediate/int_consensus_validators_labels.sql` l6 `tags=['dev','consensus','validators']`, l14/l15 bare `stg_consensus__validators`.

**CONSENSUS-C03.** `describe_table int_consensus_validators_status_daily` = `(date, status, cnt)`. `schema.yml` l95 `total_validators`, l112 `active_validators`, l129 `exited_validators`, l133 `slashed_validators`; total/active each carry `elementary.column_anomalies` (severity: warn).

**CONSENSUS-C04.** `SELECT toDate(slot_timestamp) d, uniqExact(slot) s FROM consensus.validators GROUP BY d HAVING s>1 ORDER BY d DESC LIMIT 5` = **0 rows**. `schema.yml` unique stanza on `validator_index` (config.where = recent 7d) with no severity override (default error); no slot/date dedup in `stg_consensus__validators.sql`.

**CONSENSUS-C05.** Single full day: `SELECT count(*), uniqExact(block_hash), uniqExact(validator_index), uniqExact((slot,withdrawal_index))` = `134,664 / 16,845 / 82,827 / 134,664` (composite key == count). 7d window: `1,018,829 / 127,431 / 91,265`.

**CONSENSUS-C06.**
```sql
SELECT eth1_block_hash, count(*), min(slot), max(slot)
FROM dbt.stg_consensus__blocks WHERE slot_timestamp >= now()-INTERVAL 30 DAY
GROUP BY eth1_block_hash ORDER BY count(*) DESC
```
= 2 rows: `0x374bee...e6e4` (455,961 rows, slots 28098100-28615698), `0x0000...0000` (38,738 rows). `schema.yml` l172 unique test on `eth1_block_hash`.

**CONSENSUS-C07.** `models/consensus/marts/fct_consensus_info_latest.sql` l160-162 `FROM info_latest t1 INNER JOIN info_7d t2 ON t2.label=t1.label`. status_daily labels stable across the 7-day boundary in retained history; no new-from-zero transition observed (latent).

**CONSENSUS-C08 / C25.** `6,712` validators with `0x02` credentials (status_latest). `SELECT count(DISTINCT lower(withdrawal_address)) FROM dbt.int_consensus_validators_withdrawal_addresses WHERE startsWith(withdrawal_credentials,'0x02')` = `0`. `fct_consensus_validators_withdrawal_addresses_distinct` = `873` rows (all 0x01). Code: `int_consensus_validators_withdrawal_addresses.sql` l17-21 CASE only `startsWith('0x01')`; distinct mart reads it at l39, pseudonymizes at l45.

**CONSENSUS-C09.** `SELECT date, uniqExact(validator_index), count(*), count(*)-uniqExact(validator_index) dup FROM dbt.int_consensus_validators_income_daily WHERE date>=2026-06-05 GROUP BY date` = `dup=0` all days. Engine `ReplacingMergeTree()`. `performance_daily.sql` l115 / `performance_latest.sql` l81 plain FROM, no FINAL/argMax.

**CONSENSUS-C10.** SQL final SELECT (l35-45) = `date, validator_count, q05..q95, mean`. `schema.yml` l470 `validator_index` (unique l474 + not_null l477), l487 `epoch_eligibility`, l491 `epoch_activation`, l495 `activation_days`.

**CONSENSUS-C11.** `int_consensus_deposits_withdrawals_daily.sql` CTEs `deposists` (l14), `deposists_requests` (l26); output `total_amount` (l55), `cnt` (l56). `schema.yml` l788 documents `amount` (ghost) with a `column_anomalies` test. Only consumer `fct_consensus_info_latest` reads `cnt`/`total_amount` and works — contract-only impact.

**CONSENSUS-C12.** SQL final SELECT (l22-31) = `date, blocks_produced, total_blob_commitments, blocks_with_zero_blob_commitments, blocks_missed`. `schema.yml` l326 `genesis_time_unix` + l330 `seconds_per_slot` (CTE-only intermediates, no test stanza); blob columns undocumented. Doc-only.

**CONSENSUS-C13.**
```sql
SELECT toDate(date), count(*), min(validator_index), max(validator_index)
FROM int_consensus_validators_income_daily WHERE date>='2026-05-25' GROUP BY date ORDER BY date
```
`2026-05-25..06-02` ~558,294-558,302 rows (vi 0..max); `2026-06-03..06-07` only 58,302-58,313 rows (vi `500000-558312`). Snapshots full ~558k for the same days. `income_daily.sql` daily_raw INNER JOIN network_state on date (l332); microbatch sliced by `validator_index_start/end` (range_template `schema.yml` l1740-1773).

**CONSENSUS-C14.** `int_consensus_deposits_withdrawals_daily` max = `2026-06-20`; `dists_daily` / `balances_daily` / `status_daily` max = `2026-06-07`. All five `api_consensus_info_*_latest` l8 set `as_of_date = (SELECT toDate(max(date)) FROM int_consensus_deposits_withdrawals_daily)`. Served: `staked value=334,875, as_of_date=2026-06-20`.

**CONSENSUS-C15.** `fct_consensus_info_latest.sql` l156-159 projects `label, value, change_pct`. `schema.yml` l1287-1305 declares `label, cnt (l1291), total_amount (l1295), value, change_pct`; ghosts carry no test stanza (only model-level `elementary.schema_changes` l1310). Live `SELECT label, value, change_pct` = 9 rows.

**CONSENSUS-C16.** `stg_consensus__validators.sql` l24 `WHERE balance > 0`; `stg_consensus__validators_all.sql` has no WHERE clause. `schema.yml` l642 and l712 byte-identical: "focusing on active validators with a positive balance."

**CONSENSUS-C17.** `scripts/checks/check_api_tags.allow` l6-17: 6 endpoints x 2 rules = 12 entries (`api_consensus_consolidations_daily`, `_validators_apy_dist_income_daily`, `_validators_apy_mean_daily`, `_validators_explorer_apy_dist_daily`, `_validators_explorer_daily`, `_validators_income_total_daily`). `check_api_tags.py` l60-62 `fail()` skips allow-listed `uid::rule`/`name::rule`; removing entries surfaces violations -> `sys.exit(1)`.

**CONSENSUS-C18.** `api_consensus_validators_status_daily.sql` config l1-6 = tags only, no `meta={...}`. Peer `api_consensus_validators_performance_daily.sql` l5-82 has full `meta.api` (require_any_of, pagination). `check_api_tags.py` reads only `node.tags` (l52-79) and `node.columns` (l82-98), never `node['meta']`.

**CONSENSUS-C19.** `schema.yml` ghost columns `in` (l592), `precedence` (l596), `separator-agnostic` (l604), `above` (l612), all `data_type: ''`, description "This column is not present in the provided SQL"; no `tests:` under any. int_ model exempt from `check_api_tags`.

**CONSENSUS-C20.** `api_consensus_deposits_withdrawls_volume_daily.sql` l4 `'api: deposits_and_withdrawals_volume'` (space after colon). `_apy_dist_last_30_days.sql` l4 `'api:validators_apy_dististribution'`; `_balance_dist_last_30_days.sql` l4 `'api:validators_balance_dististribution'` (typo is `dististribution`, double-ti — NOT `dississribution` as baseline quoted). `check_api_tags.py` `res=t[4:]` preserves leading space, publishes slug verbatim.

**CONSENSUS-C21.** `SELECT count(*), groupArray(fork_name), any(as_of_date) FROM dbt.api_consensus_forks` = 7 rows (Phase0..Fulu), `as_of_date=2026-06-21` (== today()). `api_consensus_forks.sql` l8 `today() AS as_of_date`. `schema.yml` l533-562 only `elementary.schema_changes`, no freshness test.

**CONSENSUS-C23.** `api_consensus_info_staked_latest.value=334,875`; `balances_daily.effective_balance total=10,716,030`, `/32=334,875.94`. `git blame -L 74,74 models/consensus/marts/fct_consensus_info_latest.sql` -> `f34819b1` (2025-08-29, "ESG and decoping updates"), no rationale. `schema.yml` l1320 description "The total amount of GNO currently staked."

**CONSENSUS-C24.** `SELECT value FROM fct_consensus_info_latest WHERE label='APY'` = `8.8`; `dists_daily.avg_apy=8.799461`; `fct_consensus_validators_apy_mean_daily.apy=9.634420`. Gap `(9.634-8.799)/8.799 = +9.49%`. `fct_consensus_info_latest.sql` l34-38 sources `dists_daily.avg_apy`.

**CONSENSUS-C26.**
```sql
SELECT date, count(*), uniqExact(validator_index)
FROM int_consensus_validators_snapshots_daily WHERE date>=2026-05-30 GROUP BY date
```
`2026-05-30..06-07` contiguous, full ~558,297-558,313/day (6-day gap + 58k partial RESOLVED). income_daily recent days ~58,312 (500k-600k slice). Both max = `2026-06-07` (14-day lag vs today). All `freshness_anomalies` `severity: warn` (`intermediate/schema.yml` l159, l164, l371, l376, ...).

**CONSENSUS-C27.** `fct_consensus_info_latest.sql` l159: `IF(t1.value=0 AND t2.value=0, 0, ROUND((COALESCE(t1.value/NULLIF(t2.value,0),0)-1)*100,1))`. For `t1=5,t2=0`: `NULLIF(0,0)=NULL`; `5/NULL=NULL`; `COALESCE(NULL,0)=0`; `(0-1)*100=-100.0`. No live row has `change_pct=-100 AND value>0`.

**CONSENSUS-C28.** Left-anti `DISTINCT income validator_index NOT IN status_latest` = `0` (both `558,313` distinct). `fct_consensus_validators_explorer_daily.sql` l57-59 INNER JOIN `fct_consensus_validators_status_latest` (current-snapshot-only, MAX(slot)).

**CONSENSUS-C29.** `SELECT count(*), uniqExact(toDate(date)) FROM dbt.fct_consensus_validators_dists_last_30_days` = `1 row, 1 distinct date (2026-06-07)`. `schema.yml` l1419/l1466 descriptions say "distribution of validator APYs/balances over the last 30 days... trends" (implies a time series). SQL (l24-35) emits one as-of cross-validator quantile row over the trailing 30 days. Wording-only.

**CONSENSUS-C30.** `SELECT count(*), groupArray(fork_name) FROM dbt.api_consensus_forks` = 7 rows (Phase0, Altair, Bellatrix, Capella, Deneb, Electra, Fulu). `fct_consensus_forks.sql` l15-23 hardcoded 7-tuple arrayJoin; INNER JOINs to fork_version (l65) / fork_epoch (l68). Live output complete today; silent-zero risk strictly future-fork.

## Review log (>=3 rounds per case)

- **C01**: R1 CONFIRMED (median 231.2%, max 3,322,693.3%) -> challenge: prove ratio==days_in_window on one credential -> R2 validator-class trace, served/correct=26=days_in_window -> challenge: show non-30 window -> R3 validator 4808, 12-day window, served=117.6=9.803x12. CONFIRMED/critical.
- **C02**: R1 CONFIRMED (dev tag + bare FROM) -> challenge: prove prod exclusion via dbt ls -> R2 dbt ls intersection EMPTY -> challenge: built-but-stale vs never-built -> R3 stale-data (873 rows exist from prior dev/full run). CONFIRMED/critical.
- **C03**: R1 CONFIRMED -> challenge: catalog describe + test config -> R2 describe = (date,status,cnt), two column_anomalies on ghosts -> challenge: severity / error vs no-op -> R3 both severity:warn, errors surface as warnings. CONFIRMED/high.
- **C04**: R1 CONFIRMED (latent, 1 snapshot/day) -> challenge: find multi-slot day in source -> R2 CHANGED to latent (0 multi-slot days, downgrade to medium) -> challenge: quote test stanza + confirm no dedup -> R3 error-severity unique, no dedup. CHANGED/medium.
- **C05**: R1 CONFIRMED (1.02M vs 127k/91k) -> challenge: quote test stanzas + single-day grain -> R2 error-severity confirmed -> challenge: composite key unique on single day -> R3 134,664==count on one full day. CONFIRMED/high.
- **C06**: R1 CONFIRMED (2 distinct eth1_block_hash) -> challenge: 30d window + characterize -> R2 30d still 2 vs 509k -> challenge: slow-view vs null-default -> R3 1 dominant real value + 1 zero-default. CONFIRMED/high.
- **C07**: R1 CONFIRMED (INNER JOIN l161) -> challenge: any new-from-zero label? -> R2 no new-from-zero today (latent) -> challenge: bound historical blast radius -> R3 no zero->nonzero transition in retained window. CONFIRMED/high (medium last round).
- **C08**: R1 CONFIRMED (4,734 0x02) -> challenge: prove downstream drop -> R2 189 0x02-only addresses absent -> challenge: prove in final mart -> R3 0 of 0x02 in source int model (salt-independent). CONFIRMED/high.
- **C09**: R1 CONFIRMED (no FINAL) -> challenge: measure dupes / confirm RMT -> R2 0 dupes, engine=RMT (latent) -> challenge: confirm no argMax/GROUP BY in FROM -> R3 plain FROM confirmed. CONFIRMED/high.
- **C10**: R1 CONFIRMED -> challenge: catalog describe -> R2 SQL projection confirmed -> challenge: catalog close-loop -> R3 validator_index is CTE-only, unique+not_null target absent column. CONFIRMED/medium.
- **C11**: R1 CONFIRMED (typo CTEs) -> challenge: clarify consumer impact -> R2 contract-only (consumer reads total_amount) -> challenge: grep for literal 'amount' consumer -> R3 none; locked contract-only. CONFIRMED/medium.
- **C12**: R1 CONFIRMED -> challenge: catalog describe + test on ghosts -> R2 inverse column set confirmed -> challenge: any test on genesis_time_unix/seconds_per_slot -> R3 no test stanza, doc-only. CONFIRMED/medium.
- **C13**: R1 CONFIRMED then 558k->58k observed -> challenge: CHANGED, pin mechanism (INNER JOIN vs partial-stage) -> R2 CHANGED: partial-stage microbatch (500k-600k slice only), incident=other -> challenge: quantify blast radius -> R3 58,313 of 558,313 for recent days. CHANGED/medium.
- **C14**: R1 CONFIRMED (depwith 06-20 vs 06-07) -> challenge: all five views -> R2 all 5 file:line cited (13-day overstatement) -> challenge: served rows -> R3 staked served value=334,875 / as_of_date=06-20. CONFIRMED/medium.
- **C15**: R1 CONFIRMED -> challenge: describe_table 3 cols -> R2 3-col SELECT succeeds -> challenge: tests on ghosts -> R3 no column test, doc-only. CONFIRMED/medium.
- **C16**: R1 CONFIRMED (description copy) -> challenge: prove population gap -> R2 SQL diff dispositive (no WHERE) -> challenge: count both -> R3 code diff conclusive (_all selects all). CONFIRMED/medium.
- **C17**: R1 CONFIRMED (6 endpoints) -> challenge: reconcile 12-vs-6 -> R2 CHANGED: 6 endpoints / 12 entries -> challenge: confirm guard honors allow-list -> R3 fail() skips allow-listed rules. CHANGED/medium.
- **C18**: R1 CONFIRMED (no meta.api) -> challenge: peer comparison + convention -> R2 peer has full meta.api -> challenge: CI uncaught -> R3 check_api_tags.py never reads node['meta']. CONFIRMED/medium.
- **C19**: R1 CONFIRMED (4 garbage cols) -> challenge: downstream effect -> R2 int_ exempt, cosmetic -> challenge: confirm no test stanza -> R3 no tests under the four. CONFIRMED/low.
- **C20**: R1 CONFIRMED (actual typo 'dististribution') -> challenge: path derivation -> R2 slug published from tag -> challenge: trimmed vs verbatim -> R3 res=t[4:] preserves leading space, verbatim. CONFIRMED/low.
- **C21**: R1 CONFIRMED (today() over static) -> challenge: confirm static + monitor -> R2 static at code level -> challenge: any freshness test -> R3 none attached, monitor clause hypothetical. CONFIRMED/low.
- **C22**: R1 CONFIRMED (median 231%, max 3.3M%) -> challenge: trace serving surface -> R2 both explorer + members paths -> challenge: API view serves it -> R3 api view == table exactly. CONFIRMED/critical.
- **C23**: R1 CONFIRMED (334,875 vs 10.7M) -> challenge: resolve intentionality -> R2 upstream units = GNO, /32 unjustified -> challenge: git blame l74 -> R3 f34819b1, no rationale. CONFIRMED/high.
- **C24**: R1 CONFIRMED (dists vs apy_mean lineage) -> challenge: quantify gap -> R2 8.80 vs 9.63 (+9.5%) -> challenge: served KPI shows dists value -> R3 served=8.8==dists. CONFIRMED/high.
- **C25**: R1 CONFIRMED (4,734 excluded) -> challenge: cross-sector blast radius -> R2 189 0x02-only absent from pseudonym surface -> challenge: identify graph join point -> R3 user_pseudonym is canonical cross-sector key, 0 of 0x02 present. CONFIRMED/high.
- **C26**: R1 CHANGED (snapshot gap resolved, lag 4->14d) -> challenge: pin incident attribution -> R2 CHANGED: snapshots=incident-A recovery, income=partial-stage, root=source lag -> challenge: self-heal vs sticky -> R3 transient (all 6 ranges declared), incident=both. CHANGED/medium.
- **C27**: R1 CONFIRMED (formula l159) -> challenge: -100% in live data? -> R2 latent (no live row) -> challenge: demonstrate arithmetic -> R3 t2=0,t1>0 -> -100.0 deterministic. CONFIRMED/medium.
- **C28**: R1 CONFIRMED (INNER JOIN status_latest) -> challenge: left-anti check -> R2 left-anti=0 (latent) -> challenge: is status_latest current-snapshot-only -> R3 yes (MAX(slot)), risk real-but-latent. CONFIRMED/medium.
- **C29**: R1 CONFIRMED (1 row) -> challenge: quote descriptions + shape -> R2 single as-of cross-validator distribution -> challenge: wording-only? -> R3 verbatim descriptions, fixable by edit. CONFIRMED/medium.
- **C30**: R1 CONFIRMED (7-tuple literal) -> challenge: confirm silent-zero mechanism -> R2 INNER JOIN drops unlisted forks -> challenge: live output non-empty -> R3 7 forks live, risk future-fork. CONFIRMED/medium.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P0 KEEP | Divide `apy_30d` by `countDistinct(date)` (window day count) — served values are inflated by exactly the window length (max 3,322,693% live in the API view) | `models/consensus/marts/fct_consensus_validators_explorer_latest.sql` (l85-87), `fct_consensus_validators_explorer_members_table.sql` (l58-60), `api_consensus_validators_explorer_latest.sql` |
| P0 KEEP | Remove the `/32` from Staked GNO and fix the schema description — serves `334,875` vs true `10,716,030 GNO` (32x), introduced in `f34819b1` with no rationale | `models/consensus/marts/fct_consensus_info_latest.sql` (l74), `api_consensus_info_staked_latest.sql`, `marts/schema.yml` (l1320) |
| P0 KEEP | Drop `tags=['dev']` (add `production`) and replace bare `consensus.stg_consensus__validators` with `{{ ref() }}` — prod ref chain breaks, currently serving stale 873-row labels | `models/consensus/intermediate/int_consensus_validators_labels.sql` (l6, l14, l15) |
| P1 KEEP | Point the APY KPI card at balance-weighted `fct_consensus_validators_apy_mean_daily.apy` (9.634), not unweighted `dists_daily.avg_apy` (8.8) — +9.49% divergence in the served row | `models/consensus/marts/fct_consensus_info_latest.sql` (l34-38) |
| P1 KEEP | Add a `0x02` (EIP-7251) branch to the withdrawal-credential CASE — `6,712` validators / all 0x02-only addresses are absent from the cross-sector `user_pseudonym` graph, gap grows post-Pectra | `models/consensus/intermediate/int_consensus_validators_withdrawal_addresses.sql` (l17-21), `fct_consensus_validators_withdrawal_addresses_distinct.sql` |
| P1 KEEP | Source `as_of_date` per-KPI from each value's own table (or expose per-KPI freshness) — 13-day overstatement (`06-20` shown vs `06-07` data) | five `models/consensus/marts/api_consensus_info_*_latest.sql` (l8) |
| P1 KEEP | Promote `freshness_anomalies` from `severity: warn` to error for the consensus pipeline so 14-day source lag pages | `models/consensus/intermediate/schema.yml` (freshness stanzas), `models/consensus/staging/schema.yml` |
| P1 KEEP | Fix microbatch range orchestration so all 6 `validator_index` slices re-run together — partial-stage left `0-500000` stale at `06-07` (`58,313` of `558,313` for recent days) | `int_consensus_validators_income_daily.sql` (range_template `schema.yml` l1740-1773), refresh runner |
| P2 KEEP | Correct unique-test grain: `(slot, validator_index)` for validators (latent, source 1 snapshot/day), `(slot, withdrawal_index)` for withdrawals (fails every run), `slot` for blocks (fails every run) | `models/consensus/staging/schema.yml` (l653, l799, l816, l172) |
| P2 KEEP | Convert performance views to `FINAL`/`argMax` over the RMT int models and add a `(date, validator_index)` uniqueness test (latent merge-window doubling) | `api_consensus_validators_performance_daily.sql` (l115), `_latest.sql` (l81) |
| P2 KEEP | Switch the two latent INNER JOINs to LEFT JOIN + COALESCE/`join_use_nulls` — info_7d (new-from-zero status drop) and explorer_daily->status_latest (income-history drop) | `fct_consensus_info_latest.sql` (l161), `fct_consensus_validators_explorer_daily.sql` (l57-59) |
| P2 KEEP | Fix `change_pct` for `t2=0, t1>0` (emit NULL/`new` instead of `-100.0`) | `fct_consensus_info_latest.sql` (l159) |
| P3 KEEP | Reconcile schema.yml with SQL projections: delete ghost columns and document real ones; C03/C10 carry tests on non-existent columns | `int_consensus_validators_status_daily`, `int_consensus_entry_queue_daily`, `int_consensus_blocks_daily`, `fct_consensus_info_latest`, `int_consensus_deposits_withdrawals_daily`, `int_consensus_graffiti_daily` (+ schema.yml) |
| P3 KEEP | Add typed column schemas + remove 6 endpoints (12 entries) from the allow list; add a `meta.api` block to `status_daily`; add a CI assertion for `meta.api` presence | `scripts/checks/check_api_tags.allow` (l6-17), `check_api_tags.py`, `api_consensus_validators_status_daily.sql` |
| P3 KEEP | Fix api: tag typos (`dististribution` -> `distribution`, remove space after colon) — slugs published verbatim | `api_consensus_deposits_withdrawls_volume_daily.sql`, `api_consensus_validators_apy_dist_last_30_days.sql`, `api_consensus_validators_balance_dist_last_30_days.sql` (l4) |
| P3 KEEP | Fix `deposists` CTE typos; correct `stg_consensus__validators_all` description (drop "positive balance"); reword `*_dist_last_30_days` descriptions (as-of snapshot, not time series); replace `api_consensus_forks` `today()` with a static date; make `fct_consensus_forks` fail loudly on missing forks | `int_consensus_deposits_withdrawals_daily.sql`, `staging/schema.yml` (l712), `marts/schema.yml` (l1419, l1466), `api_consensus_forks.sql` (l8), `fct_consensus_forks.sql` (l15-23) |

No DROP recommendations — nothing from the baseline was resolved.
