# Model review (revisit 2026-06-21): execution/dao_treasury

Baseline: `docs/model_review/execution-dao_treasury.md` (dated `2026-06-11`); 13 cases re-verified over 3 rounds. Headline: `0` resolved, `1` changed (manifest -> latent), `12` still confirmed, `0` new — the `$0`/`-100%` partial-refresh harm cleared in the data but the unguarded `max(date)` code defect is unfixed, and all 12 code/business-logic findings reproduce verbatim.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONDAOTREASURY-C01 | — | Unguarded `max(date)` anchor serves lending-only `$0` day on partial refresh | critical | CHANGED | high | high | microbatch_insert_overwrite | 3 |
| EXECUTIONDAOTREASURY-C02 | — | No not_null/unique/accepted_values tests; grain clean but unprotected | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONDAOTREASURY-C03 | — | ETH branch matches only `WETH`; wstETH/SAFE/COW fall to `Other` (impl) | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONDAOTREASURY-C04 | — | Dev tags evade `check_api_tags.py`; marts fail guard on promotion | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONDAOTREASURY-C05 | — | `605` rows NULL `balance_usd` silently dropped by `sum()` | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONDAOTREASURY-C06 | — | CASE output alias `token_class` shadows source column (CH pitfall) | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONDAOTREASURY-C07 | — | `nullIf` NULL-case for `change_pct` undocumented in schema.yml | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONDAOTREASURY-C08 | — | `HAVING value > 0` over nullable sum vanishes a fully-unpriced class | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONDAOTREASURY-C09 | — | wstETH/SAFE/COW in `Other` is governance-visible misclassification (biz) | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONDAOTREASURY-C10 | — | `kpi_gno_held` sums GNO + LSD derivatives 1:1, no rate adjustment | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONDAOTREASURY-C11 | — | No semantic_models entry; treasury KPIs not MCP-servable | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONDAOTREASURY-C12 | — | Six treasury Safes only in team-labeled seed, no whitelist/on-chain ref | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONDAOTREASURY-C13 | — | sDAI/aGnosDAI bucketed as Stablecoins at NAV, undocumented yield uplift | low | CONFIRMED | low | high | none | 3 |

## Delta vs baseline

RESOLVED (`0`): none.

CHANGED (`1`):
- `EXECUTIONDAOTREASURY-C01` — manifest -> latent. The baseline `$0`/`-100%` harm is gone: `max(date)=2026-06-22` is a complete day (`37` wallet + `23` lending = `60` rows, `$117,694,055`), and `2026-06-05..06-22` is fully contiguous with both `position_type` values every day. Incident attribution: `microbatch_insert_overwrite`. But the data was restored, not the code: git log on `models/execution/dao_treasury/` shows the last touch is `fe8c9d94` (`2026-06-09` "large refactor"), which PREDATES both June incidents; the microbatch fix lived in `macros/db/get_incremental_filter.sql`, not these marts. The unguarded `WHERE date = (SELECT max(date) ...)` persists in all six marts (`api_dao_treasury_kpi_total_holdings.sql` L9-15) with no completeness/freshness/HAVING guard, and the lending-only subset is `~$5.94M`/day — so a wallet-absent day would again collapse each mart from `~$117M` to `~$5.9M`. Mitigated-by-data, not fixed-by-code; severity held at high for latent recurrence.

STILL CONFIRMED (`12`):
- `EXECUTIONDAOTREASURY-C02` (high) — `0` not_null/unique/accepted_values tests in either schema.yml; grain clean at `44,809` rows == `44,809` distinct over `(date,wallet_address,position_type,protocol,symbol)`. `protocol` is load-bearing: dropping it gives `41,081` distinct (delta `3,728`).
- `EXECUTIONDAOTREASURY-C03` (high) — ETH branch still `symbol IN ('WETH')` only in `allocation_latest` L18 and `by_class_ts` L15; on `2026-06-22` `Other={SAFE,COW,wstETH}=$2,579,843` vs `ETH={WETH}=$899,379`. Over `1,753` days of history, `Other > ETH` on `25` distinct days, flipping which line is larger on the chart.
- `EXECUTIONDAOTREASURY-C04` (medium) — all four KPI marts carry `dev` not `production`; `check_api_tags.py` L53 `if "production" not in tags: continue` skips them. On promotion they fail BOTH rule #4 `columns_untyped` (schema.yml has `0` data_type entries) AND rule #5 `no_as_of_date` (`granularity:latest` is a POINT_GRAN but marts output only `value`+`change_pct`, no FRESH_POINT column). Baseline "missing window:7d" wording is imprecise — the guard never enforces window:7d presence.
- `EXECUTIONDAOTREASURY-C05` (medium) — `605` NULL `balance_usd` rows (bCSPX `573`, GBPe `32`) = `1.35%` of `44,809`; `sum(balance_usd)` silently drops them. `0` NULL rows on `2026-06-22` so the live KPI is NOT understated; worst historical day had only `2` NULL rows (`11,732.32` native units).
- `EXECUTIONDAOTREASURY-C06` (low) — `allocation_latest` L21 output alias `token_class` identical to source column referenced in WHEN branches L16-17; `GROUP BY symbol, token_class` L25. Currently inert (no token under two labels; each symbol maps to exactly one CASE branch). `by_class_ts` uses alias `label` so it is safe there.
- `EXECUTIONDAOTREASURY-C07` (low) — `change_pct = round((cur-prior)/nullIf(prior,0)*100,1)` (`total_holdings` L24-25); NULL on zero/missing prior. schema.yml documents only "Percentage change vs 7 days ago" (L12/L21-22/L31-32/L41-42), no NULL-case note. Currently non-NULL (`+4.5%` total) since `(max-7)=2026-06-15` exists.
- `EXECUTIONDAOTREASURY-C08` (low) — `by_class_ts` L22 `HAVING value > 0` over a nullable `round(sum(balance_usd),0)`. Never materialized: `0` fully-unpriced class-days for RWA (`0/1,143`) and Stablecoins (`0/1,709`); single-symbol BTC=`{WBTC}`/ETH=`{WETH}` have no NULL rows in history.
- `EXECUTIONDAOTREASURY-C09` (high) — business-decision facet: wstETH `$474,307` + SAFE `$1,399,020` + COW `$706,516` all `token_class='OTHERS'` aggregated into governance-facing `Other=$2,579,843` vs `ETH=$899,379`. wstETH peak `$2,767,854`; implied price `~$2,106`/unit (genuine staked-ETH derivative). Reallocation under candidate rules moves `$0.47M`-`$2.1M` between buckets.
- `EXECUTIONDAOTREASURY-C10` (medium) — `kpi_gno_held.sql` L13-16/L20-22 sums `balance` for `GNO/sGNO/spGNO/aGnoGNO` with no rate adjustment; schema.yml L26-27 "in native GNO units", no LSD-drift caveat. On `2026-06-22` GNO=`819,115.69` native (`$85.04M`); LSD not held (`0`). Historical LSD-native peak `9,555.95` units (`~1.1%` of GNO core) = latent/structural.
- `EXECUTIONDAOTREASURY-C11` (medium) — `0` semantic authoring files reference any of the seven `api_dao_treasury_*` marts; `discover_metrics('dao treasury total holdings')` returns no treasury metric (top hit `revenue_potential_total_weekly`, score `65`); `query_metrics(['dao_treasury_total_holdings'])` unresolvable. Registry hits are model catalog/lineage only.
- `EXECUTIONDAOTREASURY-C12` (low) — all six Safe addresses return `0` hits in `seeds/contracts_whitelist.csv`; exist only in `seeds/dao_treasury_wallets.csv` with team labels. On-chain `getOwners()/getThreshold()` confirmed on two addresses (`0x509ad7...` = 3-of-6 multisig), so the attestation cross-check is feasible but not implemented in-repo.
- `EXECUTIONDAOTREASURY-C13` (low) — Stablecoins CASE branch includes sDAI (`allocation_latest` L16, `by_class_ts` L13); non-GNO KPI excludes only `GNO/sGNO/spGNO/aGnoGNO` so sDAI counts. On `2026-06-22` sDAI=`$11,156,296` = `45.2%` of the `$24,672,756` Stablecoins bucket; native `8,964,888.2` -> implied rate `1.2444` (>1.0), `~$2.19M` undocumented yield uplift inside the "Stablecoins" label. schema.yml carries no note.

NEW (`0`): none.

UNVERIFIABLE / UNRESOLVED (`0`): none.

## Evidence appendix

C01 (contiguity + collapse mechanism):
```sql
SELECT date, countIf(position_type='wallet') wallet_rows, countIf(position_type='lending') lending_rows,
       round(sum(balance_usd),0) total_usd, round(sumIf(balance_usd,position_type='lending'),0) lending_usd
FROM dbt.int_dao_treasury_holdings_daily WHERE date>='2026-06-05' GROUP BY date ORDER BY date
```
Returned: `2026-06-05..06-22` contiguous; every day `36-37` wallet + `23` lending rows; max date `2026-06-22` = `$117,694,055` total, lending-only `$5.94M`. No collapsed/`$0` day exists today. git log on `models/execution/dao_treasury/`: last touch `fe8c9d94` (`2026-06-09`), predates the incidents; no freshness/HAVING/contract added to any mart or to `int_dao_treasury_holdings_daily`.

C02 (grain uniqueness + protocol load-bearing):
```sql
SELECT count(*) total_rows,
       uniqExact((date,wallet_address,position_type,protocol,symbol)) grain_full,
       uniqExact((date,wallet_address,position_type,symbol)) grain_no_protocol
FROM dbt.int_dao_treasury_holdings_daily
```
Returned: `44,809` rows; `grain_full=44,809` (clean, `0` dups); `grain_no_protocol=41,081` (`3,728` fewer). Per-month check: excess `0` in all `59` months. Both schema.yml: `0` not_null/unique/accepted_values/tests; intermediate schema.yml has only `9` data_type docs; `0` test files under `tests/` reference the model.

C03 / C09 (asset-class misclassification — shared query, max date):
```sql
SELECT token_class, round(sum(value_usd),0) bucket_usd, groupArray(token) tokens
FROM dbt.api_dao_treasury_allocation_latest GROUP BY token_class ORDER BY bucket_usd DESC
```
Returned on `2026-06-22`: `ETH={WETH}=$899,379`; `Other={SAFE $1,399,020, COW $706,516, wstETH $474,307}=$2,579,843`; GNO `$85.0M`, Stablecoins `$24.67M`, RWA `$3.67M`, BTC `$0.83M`. Code: ETH CASE branch = `symbol IN ('WETH')` in `allocation_latest` L18 and `by_class_ts` L15. Time-series facet: `Other > ETH` on `25` of `1,753` days; wstETH peak `$2,767,854`, implied price `~$2,106`/unit. Reallocation magnitudes (max date): (a) wstETH->ETH: ETH=`$1,373,686`, Other=`$2,105,536`; (b) wstETH->'ETH Derivatives': ETH=`$899,379`, new=`$474,307`, Other=`$2,105,536`; (c) SAFE+COW->'Governance': Governance=`$2,105,536`, Other=`$474,307`.

C04 (guard skip + promotion failures): code-only. `check_api_tags.py` L53 `if "production" not in tags: continue`; KPI marts tagged `dev` (L4-5 each). `marts/schema.yml` has `0` data_type -> rule #4 `columns_untyped` (L86-88). `granularity:latest` in POINT_GRANS (L27); marts output only `value`+`change_pct`, neither in `FRESH_POINT={as_of_date,snapshot_date,date,block_date,block_timestamp,ts,timestamp,day}` -> rule #5 `no_as_of_date` (L93-96). `WINDOW_RE` (L25) only forbids window suffixes in `api:` names; never enforces window:7d presence.

C05 (NULL balance_usd):
```sql
SELECT symbol, count(*) null_rows,
       countIf(date=(SELECT max(date) FROM dbt.int_dao_treasury_holdings_daily)) null_on_maxdate
FROM dbt.int_dao_treasury_holdings_daily WHERE balance_usd IS NULL GROUP BY symbol
```
Returned: `605` NULL rows = bCSPX `573` + GBPe `32` = `1.35%` of `44,809`; `0` on max date `2026-06-22`. Worst historical day: `2` NULL rows, `11,732.32` native units.

C06 (alias shadow inert): code-only + served check. `allocation_latest` L21 alias `token_class` == source column (L16-17); `GROUP BY symbol, token_class` L25. `SELECT token, uniqExact(token_class) FROM dbt.api_dao_treasury_allocation_latest GROUP BY token HAVING ... > 1` returns `0` rows.

C07 (change_pct NULL undocumented): code-only. `change_pct = round((cur-prior)/nullIf(prior,0)*100,1)`; served `+4.5` total, `-0.7` non_gno, `-0.0` gno_held, `+0.1` lending. schema.yml has no NULL-case note. `(max-7)=2026-06-15` exists, so non-NULL today.

C08 (HAVING drop never fired):
```sql
WITH cls AS (SELECT date, <label>, count(*) n, countIf(balance_usd IS NOT NULL) priced
             FROM dbt.int_dao_treasury_holdings_daily WHERE <class membership> GROUP BY date,label)
SELECT label, countIf(priced=0) fully_unpriced_classdays, count(*) classdays FROM cls GROUP BY label
```
Returned: RWA `0/1,143`, Stablecoins `0/1,709` fully-unpriced class-days. `by_class_ts` L22 still `HAVING value > 0`.

C10 (GNO-family 1:1 sum):
```sql
SELECT symbol, round(sum(balance),2) native, round(sum(balance_usd),0) usd
FROM dbt.int_dao_treasury_holdings_daily
WHERE date=(SELECT max(date) FROM dbt.int_dao_treasury_holdings_daily)
  AND symbol IN ('GNO','sGNO','spGNO','aGnoGNO') GROUP BY symbol
```
Returned on `2026-06-22`: GNO `819,115.69` native (`$85.04M`); sGNO/spGNO/aGnoGNO not held (`0`). Historical combined LSD-native peak `9,555.95` units. `kpi_gno_held.sql` L13-16 sums raw native, no rate adjustment; schema.yml L26-27 no caveat.

C11 (no semantic coverage): `discover_metrics('dao treasury total holdings')` -> top hit `revenue_potential_total_weekly` (score `65`), no `api_dao_treasury_*` root_model in candidate set; `query_metrics(['dao_treasury_total_holdings'])` returns no resolvable metric. `grep dao_treasury semantic/` = `0` files.

C12 (Safe boundary): `grep` six addresses in `seeds/contracts_whitelist.csv` = `0` hits each; present only in `seeds/dao_treasury_wallets.csv` (team labels). `contract_call_function getOwners()/getThreshold()` on `0x509ad7278a2f6530bc24590c83e93faf8fd46e99` (Stables & Staking) -> `getThreshold()=3`, `getOwners()=6` (3-of-6 multisig). `0x458cd3...` confirmed GnosisSafeL2 proxy (impl `0x3E5c63644E683549055b9Be8653de26E0B4CD36E`).

C13 (yield-bearing stables at NAV):
```sql
SELECT sum(balance) native, sum(balance_usd) usd
FROM dbt.int_dao_treasury_holdings_daily WHERE date=(SELECT max(date) ...) AND symbol='sDAI'
```
Returned on `2026-06-22`: sDAI native `8,964,888.2`, USD `$11,156,296.42` -> implied rate `1.2444`. sDAI+aGnosDAI = `45.2%` of the `$24,672,756` Stablecoins bucket, `9.5%` of `$117.69M` total; `~$2.19M` yield above principal. schema.yml has no yield-bearing note.

## Review log (>=3 rounds per case)

- C01: R1 RESOLVED (data clean, max date complete `$117.69M`) -> orchestrator challenged CHANGED-not-RESOLVED, asked partial-refresh simulation -> R2 CHANGED/high (lending-only subset `~$5.9M`/day, guard absent in all six marts) -> R3 CHANGED/high (git log confirms marts never code-touched by incident fix; final).
- C02: R1 CONFIRMED/high (`0` tests, grain clean) -> challenge: verify per-partition + project-wide test search -> R2 CONFIRMED (per-month excess `0`, `0` test files) -> R3 challenge: is protocol redundant? -> CONFIRMED/high (protocol load-bearing, delta `3,728`).
- C03: R1 CONFIRMED/high (ETH=WETH only) -> challenge: historical blast radius -> R2 CONFIRMED (`6` days wstETH alone > ETH bucket, peak `$2.77M`) -> R3 challenge: time-series facet -> CONFIRMED/high (`25` days Other>ETH over `1,753`).
- C04: R1 CONFIRMED/medium (dev tags skip guard) -> challenge: prove the failure -> R2 CONFIRMED (rule #4 columns_untyped + found rule #5 no_as_of_date) -> R3 challenge: confirm rule #5 columns -> CONFIRMED/medium (BOTH violations fire).
- C05: R1 CONFIRMED/medium (`605` NULLs) -> challenge: served impact -> R2 CONFIRMED (`0` on max date, historical only) -> R3 challenge: worst historical day -> CONFIRMED/medium (`2` rows worst, intermittent).
- C06: R1 CONFIRMED/low (alias shadow) -> challenge: execute served view -> R2 CONFIRMED (no token under two labels) -> R3 challenge: benign by construction? -> CONFIRMED/low (functionally dependent on symbol).
- C07: R1 CONFIRMED/low (nullIf undocumented) -> challenge: query served values -> R2 CONFIRMED (currently non-NULL) -> R3 challenge: trigger NULL path -> CONFIRMED/low ((max-7) exists; reachable on history gaps).
- C08: R1 CONFIRMED/low (HAVING over nullable sum) -> challenge: widen probe to single-symbol classes -> R2 CONFIRMED (BTC/ETH no NULLs) -> R3 challenge: fully-unpriced multi-symbol class-day -> CONFIRMED/low (`0` ever).
- C09: R1 CONFIRMED/high (wstETH/SAFE/COW in Other) -> challenge: confirm ETH-correlation -> R2 CONFIRMED (wstETH price > WETH; SAFE/COW gov tokens) -> R3 challenge: bucket-shift magnitudes -> CONFIRMED/high (`$0.47M`-`$2.1M`).
- C10: R1 CONFIRMED/medium (1:1 LSD sum) -> challenge: quantify imprecision -> R2 CONFIRMED (LSD not held, ~0 error today) -> R3 challenge: historical worst case -> CONFIRMED/medium (LSD peak `~1.1%`, latent).
- C11: R1 CONFIRMED/medium (no semantic files) -> challenge: prove serve-failure -> R2 CONFIRMED (grep `0`) -> R3 challenge: empirical query_metrics -> CONFIRMED/medium (discover/query_metrics return no treasury metric).
- C12: R1 CONFIRMED/low (Safes only in seed) -> challenge: on-chain proxy check -> R2 CONFIRMED (GnosisSafeL2 proxy on `0x458cd3`) -> R3 challenge: generalize to 2nd address -> CONFIRMED/low (`0x509ad7` = 3-of-6).
- C13: R1 CONFIRMED/low (sDAI in Stablecoins) -> challenge: quantify share -> R2 CONFIRMED (`$11.16M`=`45.2%`) -> R3 challenge: yield component disclosed? -> CONFIRMED/low (rate `1.2444`, `~$2.19M` uplift).

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P0 (ESCALATE) | Add a completeness guard to the `max(date)` anchor: require BOTH `position_type IN ('wallet','lending')` present (or a source-freshness contract on `int_dao_treasury_holdings_daily`) before a date is eligible as the served snapshot — the incident recovered the DATA but the code defect is unfixed and recurs on the next misaligned wallet/lending refresh | all six marts; `int_dao_treasury_holdings_daily.sql` |
| P1 (KEEP) | Add `not_null(protocol)` + `unique_combination_of_columns` on the full 5-col grain `(date,wallet_address,position_type,protocol,symbol)` (NOT the narrower tuple — protocol is load-bearing, delta `3,728`) plus `accepted_values`/`not_null` on `balance_usd` to surface the silent NULL drop | `intermediate/schema.yml`, `marts/schema.yml` |
| P1 (KEEP) | Fix the ETH asset-class branch to include `wstETH` (or add an "ETH Derivatives" bucket) so the allocation pie and `by_class_ts` chart stop putting an ETH-correlated position in `Other` — needs a governance decision on wstETH/SAFE/COW bucketing | `api_dao_treasury_allocation_latest.sql`, `api_dao_treasury_holdings_by_class_ts.sql` |
| P2 (KEEP) | Before dev->production promotion, add `data_type` to all `marts/schema.yml` columns AND expose a freshness column (`as_of_date`/`date`) on the KPI marts so they pass rule #4 `columns_untyped` and rule #5 `no_as_of_date` | four KPI marts; `marts/schema.yml`; (guard: `scripts/checks/check_api_tags.py`) |
| P2 (KEEP) | Document the LSD treatment: `kpi_gno_held` sums GNO + sGNO/spGNO/aGnoGNO native 1:1 with no exchange-rate adjustment (or convert to GNO-equivalent); add schema caveat | `api_dao_treasury_kpi_gno_held.sql`, `marts/schema.yml` |
| P2 (KEEP) | Add semantic_models entries for the seven treasury marts so the KPIs are MCP-servable and governed | all seven `api_dao_treasury_*` marts; semantic authoring |
| P3 (KEEP) | Document the NAV/yield-bearing treatment of sDAI/aGnosDAI inside the "Stablecoins" line (`~$2.19M` uplift at rate `1.2444`); document the `change_pct` NULL-on-missing/zero-prior case | `marts/schema.yml` |
| P3 (KEEP) | Rename the `allocation_latest` CASE output alias `token_class` -> `asset_class` to remove the latent ClickHouse alias-shadow ambiguity | `api_dao_treasury_allocation_latest.sql` |
| P3 (KEEP) | Replace `HAVING value > 0` with a NULL-safe predicate so a fully-unpriced class shows zero rather than vanishing from the time series | `api_dao_treasury_holdings_by_class_ts.sql` |
| P3 (KEEP) | Add an on-chain attestation cross-check of the six treasury Safe addresses (`getOwners()`/`getThreshold()`) so the in-scope boundary is verifiable for governance reporting | `seeds/dao_treasury_wallets.csv` |
