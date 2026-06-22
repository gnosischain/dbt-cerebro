# Model review (revisit 2026-06-21): execution/mmm

Baseline: `docs/model_review/execution-mmm.md` (2026-06-11). Re-verified **17 cases** (16 baseline + 1 new) over **3 rounds**. Headline: **2 resolved**, **2 changed**, **12 still confirmed**, and **1 new CRITICAL** join-fanout bug that inflates every magnitude in the spine, API view, and baseline mart by a constant per-family factor (KPI x72, media x117, control x104).

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONMMM-N01 | — | Un-keyed week-only LEFT JOINs cross-product the 3 long-form intermediates; `sumIf` over the per-week cross product inflates every spine column (KPI x72, media x117, control x104) | — | **CONFIRMED** (NEW) | **critical** | high | none | 2 |
| EXECUTIONMMM-C01 | — | Collinearity emits NaN corr for 18/21 zero-variance media pairs; `is_high_collinearity` silently 0 for all (no insufficient-variance marker) | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONMMM-C09 | — | ~half the registry is permanently empty: 5/8 media, 6/13 KPIs land NULL/0 across all 105 weeks (no aggregator CTE) | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONMMM-C10 | — | Primary KPI `ga_active_users` under-powered: 6/104 non-null weeks, below guide floor | high | **CHANGED** | medium | high | other | 3 |
| EXECUTIONMMM-C02 | — | Adstock window `groupArray OVER (ORDER BY week ...)` has no PARTITION BY; source CTE has no ORDER BY | high | CONFIRMED | medium | high | none | 3 |
| EXECUTIONMMM-C04 | — | Both persona-consumed fct_ marts tagged only `['production','mmm','execution','mart']`, no api:/granularity:/tier | medium | CONFIRMED | low | high | none | 3 |
| EXECUTIONMMM-C11 | — | `validator_apr_proxy` registered as media but is a documented reverse-causation Bad pair; unguarded | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONMMM-C12 | — | Baseline mart yields ~18 rows (3 media x 6 KPIs) vs intended 8x13; no coverage/status marker | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONMMM-C03 | — | `api_execution_mmm_spine_weekly` omits `window:` tag claimed required by CI guard | medium | **CHANGED** | low | high | none | 3 |
| EXECUTIONMMM-C05 | — | `int_execution_mmm_media_weekly` incremental_strategy ternary always falsy at parse -> dead code | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONMMM-C06 | — | `ctrl_eth_price` WHERE has dead `'ETH'` branch; price source has 0 ETH rows (latent dup) | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONMMM-C07 | — | Media intermediate lacks `(week, media_name)` uniqueness test its 2 siblings carry | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONMMM-C08 | — | `hardfork_step` maps n>0 to 1; Cancun+Pectra collapse into one binary step | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONMMM-C13 | — | `dex_volume_usd_dedup` multi-hop overcount flagged but no magnitude bound documented | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONMMM-C14 | — | No semantic-layer metrics; no `semantic/authoring/execution/mmm` folder; raw-SQL-only | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONMMM-C15 | — | Round-1 "53-week stale spine" claim (day-ordinal misread) | critical | **RESOLVED** | resolved | high | none | 3 |
| EXECUTIONMMM-C16 | — | Round-1 "sumIf turns all-NULL weeks into false 0" claim | high | **RESOLVED** | resolved | high | none | 3 |

## Delta vs baseline

### NEW (1)
- **EXECUTIONMMM-N01 (critical)** — `models/execution/mmm/marts/fct_execution_mmm_spine_weekly.sql` lines 81-85 LEFT JOIN the three long-form intermediates `ON week` only, then `GROUP BY week` + `sumIf(...)`, forming a per-week cross product of `13 kpi x 8 media x 9 ctrl = 936` rows. Each `sumIf` sums a value across all duplicate rows, inflating each family by a constant factor. Measured **exactly**: KPI `kpi_pools_volume_usd` `864,599,919.54 / 12,008,332.22 = 72.000` (=8*9); media `media_validator_proposer_rewards_gno` `23,775.43 / 203.21 = 117.000` (=13*9); control `ctrl_chain_block_count` `249,619,032 / 2,400,183 = 104.000` (=13*8). Per-week row counts verified stable at 13/8/9 across all recent weeks. **Blast radius**: `api_execution_mmm_spine_weekly` is `SELECT *` over the spine (inherits all inflation) and `fct_execution_mmm_baseline_latest` reads the inflated `kpi_*`/`media_*` columns directly (lines 21-23). Every absolute magnitude served to the persona is wrong by a fixed multiplier. Not incident-related; pre-existing structural bug missed at baseline.

### RESOLVED (2)
- **EXECUTIONMMM-C15 (was critical)** — Spine is current, not 53-week stale. `fct_execution_mmm_spine_weekly`: 105 rows, max week ordinal 20612 (2026-06-08); ClickHouse server `today()` = 20625 (2026-06-28) -> 13-day lag = intentional `today() - INTERVAL 7 DAY` trailing exclusion rounded to the prior complete Monday-week. Round-1 "max(week)=2025-06-09" was a day-ordinal misread; retraction holds.
- **EXECUTIONMMM-C16 (was high)** — No false zeros from `sumIf`. `countIf(kpi_ga_active_users=0)=0`; last-value KPI has 31 non-null and 74 NULL weeks (not 0). ClickHouse `sumIf` over an all-NULL group returns NULL, not 0; confirmed across all fully-empty last-value KPIs (`kpi_gpay_active_users`, `kpi_gno_staked` also 0 zero-valued). Round-1 retraction holds.

### CHANGED (2)
- **EXECUTIONMMM-C10 (high -> medium)** — GA coverage materially improved by the June GA growth fix (incident attribution: `other`). `kpi_ga_active_users` non-null weeks went from `6/104` at baseline to **`31/105`**, contiguous (`2025-11-10`..`2026-06-15`, span 31, no gaps). The baseline's "30-week directional-only floor" phrasing is imprecise: the MMM User Guide decision tree (retrieved verbatim) is `n_weeks>=104 -> real MMM; 60-103 -> full SOP; 30-59 -> directional only; <30 -> skip`. 31 weeks now sits in the directional-only band, above the 30 lower bound but still below the 60-week full-SOP floor, and **no per-KPI `readiness_tier`/`n_weeks` column** was added to spine or baseline mart (`describe_table` confirms). The under-powered limitation holds at the corrected threshold; the specific "below 30" claim no longer does.
- **EXECUTIONMMM-C03 (medium -> low)** — The premise is false: `scripts/checks/check_api_tags.py` (lines 24-91) enforces a grain/window-free `api:` *name*, exactly one `granularity:`, a `tier{n}` tag, and typed columns + grain freshness col. It **never requires a separate `window:` tag**. `api_execution_mmm_spine_weekly` carries `api:mmm_spine` + `granularity:weekly` + `tier1` and **passes** the guard (and is absent from `check_api_tags.allow`). Residual is only the cosmetic absence of the `window:`/`tier:` tag families -> downgraded to low.

### STILL CONFIRMED (12)
- **EXECUTIONMMM-C01 (high)** — `fct_execution_mmm_collinearity_latest`: 21 pairs, `isNaN(pearson_corr)=18`, `is_high_collinearity=1 -> 0` pairs, `=0 -> 21` (incl. all 18 NaN), `IS NULL -> 0`. Code (line 43) still emits `toUInt8(abs(corr)>0.9)`; no `insufficient_variance`/status column (`describe_table`: only `col_a,col_b,pearson_corr,n_weeks,is_high_collinearity,computed_at`). No `schema.yml` test gates `is_high_collinearity` on `isNaN`, and no api_/semantic wrapper re-derives it — the persona's direct read is the only consumption path.
- **EXECUTIONMMM-C09 (high)** — 5/8 media (`validator_income_gno`, `validator_apr_proxy`, `gpay_cashback_outlay_usd`, `lm_rewards_outlay_usd`, `bridge_incentive_outlay_usd`) and 6/13 KPIs (`gpay_active_users`, `gpay_payment_volume_usd`, `chain_tx_count`, `gno_staked`, `bridge_inflow_usd`, `bridge_outflow_usd`) are NULL/0 across all 105 weeks. Registry advertises 8 media x 13 KPIs; only 3 media + 7 KPIs have aggregator CTEs. Implementation gap, not empty upstreams: `api_consensus_staked_daily` holds 107 recent daily rows (last value 334,875.94 GNO) and `fct_execution_gpay_activity_weekly` holds 16 weekly rows (4,569 active users), yet their spine columns are entirely empty.
- **EXECUTIONMMM-C02 (medium, latent)** — Code unchanged: `fct_execution_mmm_baseline_latest.sql` lines 38-51 still use `groupArray OVER (ORDER BY week ROWS BETWEEN 8 PRECEDING AND CURRENT ROW)` with no PARTITION BY; source CTE (lines 21-25) has no ORDER BY. But the source is provably one row per week (spine 105 rows / 105 unique weeks; trailing-420d duplicate-week query returns 0), so ORDER BY week alone deterministically orders each per-column series. Live adstock is correct today; fragile only if refactored to long form -> latent-only medium.
- **EXECUTIONMMM-C04 (low)** — Both `fct_execution_mmm_baseline_latest.sql` (line 6) and `fct_execution_mmm_collinearity_latest.sql` (line 6) still tag only `['production','mmm','execution','mart']`. No api_* wrapper over either (only the spine has `api_execution_mmm_spine_weekly`); no semantic exposure. They ARE discoverable models (`get_model_details`/`describe_table` return full schemas). Internal-mart tag-cosmetic gap -> low.
- **EXECUTIONMMM-C11 (medium)** — `seeds/mmm_media_registry.csv` line 4: `validator_apr_proxy,weighted_avg,apr_pct,0,api_consensus_info_apy_latest` (is_outlay=0). MMM User Guide section 4 (verbatim): `KPI=deposits, media=APR | APR is computed from deposits - inverse causation (Guidebook p.91)`. Currently empty (`media_validator_apr_proxy` 0 non-null weeks), but a grep across `models/execution/mmm/` + seeds found no exclusion/allowlist/warning. Unguarded registry presence invites the documented Bad-pair misuse once wired.
- **EXECUTIONMMM-C12 (medium)** — `fct_execution_mmm_baseline_latest`: 18 rows, 3 distinct media (`ga_token_offer_emissions_usd`, `pools_lp_fee_apr_avg`, `validator_proposer_rewards_gno`), 6 distinct KPIs, vs intended 8x13=104. The 5 empty media (no inline adstock CTE) contribute zero rows; the `n_low_spend_weeks>5` HAVING drops only 3 of 21 implemented pairs, so "not implemented" dominates "dropped by HAVING". No coverage/status/implemented column (`describe_table`).
- **EXECUTIONMMM-C05 (low)** — `int_execution_mmm_media_weekly.sql` line 4 `incremental_strategy=('append' if start_month else 'delete+insert')` still sits before `{% set start_month %}` on line 13, so it is always falsy at parse -> always `delete+insert`, inconsistent with the two siblings' unconditional `insert_overwrite`. No live data defect: `uniqExact((week,media_name))=count()=840` (and 96=96 trailing 90d).
- **EXECUTIONMMM-C06 (low)** — `int_execution_mmm_controls_weekly.sql` line 53 still filters `symbol IN ('WETH','ETH')`. `int_execution_token_prices_daily`: WETH=3066, GNO=3324, WXDAI=2813, EURE=1572, **ETH=0**. Controls emit exactly 1 `eth_usd_price_avg` row per week (105 weeks, max-per-week=1). Latent dup gated on an ETH symbol that never occurs.
- **EXECUTIONMMM-C07 (low)** — `models/execution/mmm/intermediate/schema.yml`: `int_execution_mmm_kpis_weekly` tests `(week,kpi_name)` (lines 72-77), `int_execution_mmm_controls_weekly` tests `(week,control_name)` (lines 191-196), but `int_execution_mmm_media_weekly` (lines 131-134) has only `elementary.schema_changes` — no `unique_combination_of_columns` on `(week, media_name)`. Grain currently unique (840=840), so this is a missing guard, not an active dup.
- **EXECUTIONMMM-C08 (low)** — `int_execution_mmm_controls_weekly.sql` lines 145-157 still compute `hardfork_step = toFloat64(if(hf_count.n > 0, 1, 0))`. No spine week predates Cancun (min ~2024-06-17 > 2024-03-13), so the pre-fork 0 is never observable, and the value is identical across the Pectra (2025-05-07) boundary — both forks collapse to one binary step. Unsuitable for segmented multi-fork analysis.
- **EXECUTIONMMM-C13 (low)** — `int_execution_mmm_kpis_weekly.sql` lines 52-66 + 124,137: `is_dedup_safe=false` carried from `mmm_kpi_registry.csv` and surfaced per row, with a comment that per-tx dedup OOMs at the 10 GiB cap. No magnitude bound added; measured overcount for week 2026-06-08 was ~1.27x (first-hop $6.27M vs per-tx-deduped proxy $4.95M over 69,534 txs) — real, bounded (<2x), acceptable MCP-internal but undocumented for external exposure.
- **EXECUTIONMMM-C14 (low)** — No `semantic/authoring/execution/mmm` folder (17 unit folders, none `mmm`; `find` for `*mmm*` under `semantic/` is empty). `discover_metrics('mmm marketing mix media spend adstock collinearity baseline')` returns only 3 gpay-volume "spend" metrics rooted at `api_execution_gnosis_app_gpay_volume_daily` — zero MMM-backed metrics. Raw-SQL-only consumption; discoverability/governance gap.

## Evidence appendix

**N01 (join fanout)** — `models/execution/mmm/marts/fct_execution_mmm_spine_weekly.sql` lines 81-85 (read): `FROM weeks w LEFT JOIN kpis k ON k.week=w.week LEFT JOIN media m ON m.week=w.week LEFT JOIN ctrls c ON c.week=w.week GROUP BY w.week`.
```sql
-- per-week long-form row counts (trailing 60d, all weeks identical): kpis=13, media=8, ctrls=9
SELECT s.kpi_pools_volume_usd,
       (SELECT sum(kpi_value) FROM dbt.int_execution_mmm_kpis_weekly WHERE kpi_name='pools_volume_usd' AND week='2026-06-01') t,
       s.kpi_pools_volume_usd / t
FROM dbt.fct_execution_mmm_spine_weekly s WHERE s.week='2026-06-01';
-- 1,411,582,616.04 / 19,605,314.11 = 72.0000  (=8*9)
-- media_validator_proposer_rewards_gno 271,045.36 / 2,316.63 = 117.0000  (=13*9)
-- ctrl_chain_block_count 249,619,032 / 2,400,183 = 104.0000  (=13*8)
```

**C01 (collinearity NaN)**
```sql
SELECT count() total_pairs, countIf(pearson_corr IS NULL) null_corr, countIf(isNaN(pearson_corr)) nan_corr,
       countIf(is_high_collinearity=1) high, countIf(is_high_collinearity=0) not_high,
       countIf(is_high_collinearity IS NULL) null_flag, uniqExact(col_a), uniqExact(col_b)
FROM dbt.fct_execution_mmm_collinearity_latest;
-- 21 total; null_corr=0; nan_corr=18; high=0; not_high=21; null_flag=0; col_a=6; col_b=6
```

**C09 (empty registry)**
```sql
SELECT countIf(kpi_gpay_active_users<>0), countIf(kpi_chain_tx_count<>0), countIf(kpi_gno_staked<>0),
       countIf(kpi_bridge_inflow_usd<>0), countIf(kpi_bridge_outflow_usd<>0),
       countIf(media_validator_income_gno<>0), countIf(media_validator_apr_proxy<>0),
       countIf(media_gpay_cashback_outlay_usd<>0), countIf(media_lm_rewards_outlay_usd<>0),
       countIf(media_bridge_incentive_outlay_usd<>0), count()
FROM dbt.fct_execution_mmm_spine_weekly;
-- all listed = 0; total = 105.  Source check: api_consensus_staked_daily 107 daily rows (334,875.94 GNO);
-- fct_execution_gpay_activity_weekly 16 weekly rows (4,569 active users) -> upstreams non-empty.
```

**C10 (GA coverage)**
```sql
SELECT uniqExact(week), min(week), max(week), dateDiff('week',min(week),max(week))+1
FROM dbt.fct_execution_mmm_spine_weekly WHERE kpi_ga_active_users IS NOT NULL;
-- 31 ; 2025-11-10 ; 2026-06-15 ; span 31 (contiguous).  Baseline was 6/104.
-- describe_table(spine, baseline): no readiness_tier / n_weeks column.
-- MMM User Guide decision tree (verbatim): >=104 real MMM; 60-103 full SOP; 30-59 directional only; <30 skip.
```

**C02 (adstock window)** — code unchanged (lines 38-51, no PARTITION BY; source CTE lines 21-25, no ORDER BY).
```sql
SELECT count() FROM (SELECT week, count() cnt FROM dbt.fct_execution_mmm_spine_weekly
  WHERE week >= today() - INTERVAL 420 DAY GROUP BY week HAVING cnt>1);  -- 0 (one row/week)
```

**C03 (api tags)** — `scripts/checks/check_api_tags.py` lines 24-91: enforces grain/window-free `api:` name, one `granularity:`, a `tier{n}`, typed cols + grain col; no `window:` enforcement. Guard run exit 0 ("API tag/schema convention OK ..."); `api_execution_mmm_spine_weekly.sql` line 4 tags `['production','mmm','execution','tier1','api:mmm_spine','granularity:weekly']`; not in `check_api_tags.allow`.

**C04** — `fct_execution_mmm_baseline_latest.sql` line 6 and `fct_execution_mmm_collinearity_latest.sql` line 6: tags `['production','mmm','execution','mart']`. No api_* wrapper over either.

**C05 / C07 (media grain)**
```sql
SELECT count(), uniqExact((week,media_name)) FROM dbt.int_execution_mmm_media_weekly;  -- 840, 840
-- trailing 90d variant: 96, 96.  schema.yml: media block has only elementary.schema_changes.
```

**C06 (eth price)**
```sql
SELECT upper(symbol) sym, count() cnt FROM dbt.int_execution_token_prices_daily
WHERE upper(symbol) IN ('WETH','ETH','GNO','WXDAI','EURE') GROUP BY sym ORDER BY sym;
-- EURE 1572; GNO 3324; WETH 3066; WXDAI 2813; (no ETH row)
```

**C08 (hardfork step)** — `int_execution_mmm_controls_weekly.sql` lines 145-157: `toFloat64(if(hf_count.n>0,1,0))`. Spine: 0 weeks pre-Cancun; `hardfork_step` min=max on both sides of Pectra (single constant; raw value carries the N01 inflation but is constant).

**C11 (validator_apr_proxy)** — `seeds/mmm_media_registry.csv` line 4 row present; grep for `exclude/allowlist/default_media/recommended/bad_pair/reverse_caus` across `models/execution/mmm/` + seeds: 0 hits. `countIf(media_validator_apr_proxy IS NOT NULL AND <>0)=0`. Guide section 4 quote above.

**C12 (baseline coverage)**
```sql
SELECT count() total, uniqExact(media_name), uniqExact(kpi_name), groupArray(DISTINCT media_name)
FROM dbt.fct_execution_mmm_baseline_latest;
-- 18 ; 3 ; 6 ; [ga_token_offer_emissions_usd, pools_lp_fee_apr_avg, validator_proposer_rewards_gno]
-- describe_table: kpi_name, media_name, baseline_kpi_median/q05/q95, bottom_decile_threshold, n_low_spend_weeks, computed_at (no status col)
```

**C13 (dedup)** — `mmm_kpi_registry.csv`: `dex_volume_usd_dedup=0`; `int_execution_mmm_kpis_weekly.sql` lines 52-66/124/137 carry the flag + comment; grep found no magnitude bound. Week 2026-06-08: first-hop $6,269,841 vs per-tx-deduped $4,951,371 over 69,534 txs -> 1.266x.

**C14 (semantic)** — `ls semantic/authoring/execution/` -> 17 folders, none `mmm`; `find semantic -iname '*mmm*'` empty. `discover_metrics(mmm...)` -> 3 gpay metrics, zero MMM-backed.

**C15 (freshness)**
```sql
SELECT count(), min(week), max(week), today(), toInt32(today()-max(week)) FROM dbt.fct_execution_mmm_spine_weekly;
-- 105 ; 2024-06-17 ; 2026-06-08 (ord 20612) ; server today() 2026-06-28 (ord 20625) ; lag 13d (= today()-7d to prior Monday)
```

**C16 (sumIf NULL)**
```sql
SELECT countIf(kpi_ga_active_users=0), countIf(kpi_ga_active_users IS NOT NULL), count()
FROM dbt.fct_execution_mmm_spine_weekly;  -- 0 ; 31 ; 105  (remaining 74 are NULL, not 0)
```

## Review log (>=3 rounds per case)

- **N01**: r2 NEW (ratios 72/117/104 measured exact) -> orch challenge: confirm un-keyed JOINs + blast radius + integer stability -> r3 CONFIRMED (read lines 81-85, api_ view is SELECT *, baseline reads inflated cols; ratios reproduced exact, per-week counts 13/8/9). 2 rounds of decisive evidence; orchestrator collapsed into CONFIRMED/critical at final rollup.
- **C01**: r1 CONFIRMED -> challenge: quote persona SOP + check for isNaN downstream guard -> r2 CONFIRMED (SOP reads flag as ground truth, no NaN guard) -> challenge: confirm no schema.yml test / api_ wrapper guards it -> r3 CONFIRMED (only accepted_range on pearson_corr; no wrapper). Final high.
- **C02**: r1 CONFIRMED high -> challenge: prove live impact (dual-order experiment) -> r2 CONFIRMED, downgraded to medium (latent) on one-row-per-week structural argument -> challenge: prove source is 1 row/week -> r3 CONFIRMED (dup-week query = 0). Final medium.
- **C03**: r1 RESOLVED (guard never enforces window:) -> challenge: actually run the guard -> r2 RESOLVED (exit 0, not in allow) -> r3 orchestrator reclassified CHANGED/low (premise false but cosmetic window:/tier: absence remains). Final CHANGED/low.
- **C04**: r1 CONFIRMED medium -> challenge: confirm no api_ wrapper / consumption angle -> r2 CONFIRMED, downgraded low -> challenge: confirm discoverable as models -> r3 CONFIRMED (get_model_details returns full schema). Final low.
- **C05**: r1 CONFIRMED low -> challenge: prove grain integrity -> r2 CONFIRMED (840=840) -> r3 CONFIRMED (unchanged). Final low.
- **C06**: r1 CONFIRMED low -> challenge: confirm 1 eth row/week + upstream can't emit ETH -> r2 CONFIRMED (max-per-week=1; upstream normalizes to WETH) -> r3 CONFIRMED (ETH=0). Final low.
- **C07**: r1 CONFIRMED low -> challenge: confirm grain currently unique -> r2 CONFIRMED (840=840) -> r3 CONFIRMED (asymmetry persists). Final low.
- **C08**: r1 CONFIRMED low -> challenge: confirm no spine week predates Cancun + Pectra produces no change -> r2 CONFIRMED (pre-Cancun=0; min=max both sides of Pectra) -> r3 CONFIRMED (code unchanged). Final low.
- **C09**: r1 CONFIRMED high -> challenge: cross-check missing CTEs + named sources hold data -> r2 CONFIRMED (no CTEs; sources named) -> challenge: query the source tables directly -> r3 CONFIRMED (staked_daily/gpay_weekly non-empty; spine cols empty). Final high.
- **C10**: r1 CHANGED (6->31 weeks) -> challenge: retrieve guide floor verbatim -> r2 CHANGED (guide gives 60/30-59 bands; baseline "30-week floor" imprecise) -> challenge: confirm no readiness column added -> r3 CHANGED (describe_table: none). Final medium.
- **C11**: r1 CONFIRMED medium -> challenge: quote guide section 4 -> r2 CONFIRMED (verbatim Bad-pair text) -> challenge: confirm not excluded from any default set -> r3 CONFIRMED (grep: no exclusion flag). Final medium.
- **C12**: r1 CONFIRMED medium -> challenge: confirm no status col + quantify HAVING-drop -> r2 CONFIRMED (no col; 3 of 21 pairs dropped by HAVING, 5 media unimplemented dominate) -> r3 CONFIRMED (18 rows/3 media/6 KPIs). Final medium.
- **C13**: r1 CONFIRMED low -> challenge: estimate overcount magnitude -> r2 CONFIRMED (~1.27x for one week) -> r3 CONFIRMED (flag present, no bound). Final low.
- **C14**: r1 CONFIRMED low -> challenge: confirm via discover_metrics -> r2 CONFIRMED (filesystem) -> r3 CONFIRMED (discover_metrics returns zero MMM-backed metrics). Final low.
- **C15**: r1 RESOLVED -> challenge: reconcile 6-day vs 13-day lag (server today() = 2026-06-28) -> r2 RESOLVED (13d = today()-7d to prior Monday) -> r3 RESOLVED (max week 2026-06-08). Final resolved.
- **C16**: r1 CONFIRMED (no false zeros) -> challenge: confirm holds for other empty last-value KPIs -> r2 CONFIRMED (gpay/gno_staked also 0 zero-valued) -> r3 RESOLVED (round-1 false-zero claim retracted; sumIf returns NULL). Final resolved.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P0 (ESCALATE / NEW) | Fix the join fanout: the LEFT JOINs key only on `week` against long-form intermediates, cross-producting them. Pivot each intermediate to one row per week BEFORE joining (e.g. per-family conditional aggregation / `argMax` to a wide row), or join on full grain. Every served magnitude is currently inflated by KPI x72, media x117, control x104. Re-materialize spine + downstream after the fix. | `models/execution/mmm/marts/fct_execution_mmm_spine_weekly.sql`, `api_execution_mmm_spine_weekly.sql`, `fct_execution_mmm_baseline_latest.sql` |
| P1 (KEEP) | Add an `insufficient_variance`/status column to the collinearity mart and stop collapsing NaN to `is_high_collinearity=0`; add a schema.yml test gating the flag on non-NaN corr. | `fct_execution_mmm_collinearity_latest.sql`, `marts/schema.yml` |
| P1 (KEEP) | Resolve scope over-advertisement: either wire aggregator CTEs for the 5 empty media + 6 empty KPIs (sources exist), or add a `readiness`/`coverage` marker distinguishing "not implemented" from "insufficient data" across spine and baseline marts. | `int_execution_mmm_kpis_weekly.sql`, `int_execution_mmm_media_weekly.sql`, `fct_execution_mmm_spine_weekly.sql`, `fct_execution_mmm_baseline_latest.sql`, `seeds/mmm_*_registry.csv` |
| P2 (KEEP) | Add a per-media coverage/status flag to the baseline mart (18 rows / 3 media vs intended 104). | `fct_execution_mmm_baseline_latest.sql` |
| P2 (KEEP) | Add a reverse-causation guard/exclusion for `validator_apr_proxy` (documented Bad pair) before it is wired. | `seeds/mmm_media_registry.csv`, `int_execution_mmm_media_weekly.sql` |
| P3 (KEEP) | Make the adstock window deterministic-by-construction: add `PARTITION BY media_name` and an explicit `ORDER BY week` to the source CTE (latent today). | `fct_execution_mmm_baseline_latest.sql` |
| P3 (KEEP) | Code-hygiene cluster: remove the dead `incremental_strategy` ternary (C05), drop the dead `'ETH'` branch (C06), add the `(week, media_name)` uniqueness test (C07), and either emit per-fork step columns or document the binary collapse (C08). | `int_execution_mmm_media_weekly.sql`, `int_execution_mmm_controls_weekly.sql`, `intermediate/schema.yml` |
| P3 (KEEP) | Document/bound the `dex_volume_usd_dedup` multi-hop overcount (~1.27x measured) before any external API exposure. | `int_execution_mmm_kpis_weekly.sql` |
| P4 (KEEP) | Add `window:`/`tier:` tag families to `api_execution_mmm_spine_weekly` and api:/granularity:/tier tags to the two persona-consumed fct_ marts (cosmetic governance). | `api_execution_mmm_spine_weekly.sql`, `fct_execution_mmm_baseline_latest.sql`, `fct_execution_mmm_collinearity_latest.sql` |
| P4 (KEEP) | Consider semantic-layer metrics for MMM (currently raw-SQL-only). | `semantic/authoring/execution/` (no `mmm` folder) |
| — (DROP) | Spine staleness (C15) — RESOLVED, spine is current (13-day intentional trailing exclusion). No action. | — |
| — (DROP) | sumIf false-zero (C16) — RESOLVED, `sumIf` returns NULL over all-NULL groups. No action. | — |
