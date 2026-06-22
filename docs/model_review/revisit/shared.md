# Model review (revisit 2026-06-21): shared

Baseline `docs/model_review/shared.md` (2026-06-11) re-verified on `2026-06-21` across `16` cases (15 baseline + 1 new) over 3 rounds: `0` resolved, `3` changed (the daily spine got a partial forward rebuild that left the staleness intact and broke the cross-grain horizon invariant), `11` still confirmed, `1` new regression — every defect from baseline persists and the only material movement made things worse, not better.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| SHARED-C01 | - | All 3 spines short of declared 5y horizon; static snapshot absent from pipeline | medium | CHANGED | medium | high | none | 3 |
| SHARED-C02 | - | No `engine`/`order_by` config -> `ORDER BY tuple()` / no PK index | medium | CONFIRMED | low | medium | none | 3 |
| SHARED-C03 | - | No `tags:` on any spine; `tag:time_spine` selection impossible | low | CONFIRMED | low | high | none | 3 |
| SHARED-C04 | - | No rebuild cadence; spines in no pipeline selection | low | CONFIRMED | low | high | none | 3 |
| SHARED-C05 | - | Genesis `2018-10-08` a bare literal, not a `var()` | low | CONFIRMED | low | high | none | 3 |
| SHARED-C06 | - | Bridge key mismatch `day` vs `date` (only 1 of 13) | medium | CHANGED | low | high | none | 3 |
| SHARED-C07 | - | Monthly bucket starts `2018-10-01`, 7 days pre-genesis; caveat absent from schema.yml | low | CONFIRMED | low | high | none | 3 |
| SHARED-C08 | - | No CI guard enforcing spine-bridge registration | low | CONFIRMED | low | high | none | 3 |
| SHARED-C09 | - | Daily row count `4,575` vs expected `4,630` (warehouse) | medium | CHANGED | low | high | none | 3 |
| SHARED-C10 | - | Weekly `654` unique weeks, max `2031-04-14` (warehouse) | medium | CONFIRMED | medium | high | none | 3 |
| SHARED-C11 | - | Monthly `151` unique months, max `2031-04-01` (warehouse) | medium | CONFIRMED | medium | high | none | 3 |
| SHARED-C12 | - | Daily spine dense / gap-free (warehouse) | low | CONFIRMED | low | high | none | 3 |
| SHARED-C13 | - | Weekly all Monday-aligned (warehouse) | low | CONFIRMED | low | high | none | 3 |
| SHARED-C14 | - | Monthly all first-of-month (warehouse) | low | CONFIRMED | low | high | none | 3 |
| SHARED-C15 | - | No hourly/quarterly spine; p2p + ESG grains cannot compose | low | CONFIRMED | low | high | none | 3 |
| SHARED-N01 | - | Isolated daily rebuild broke daily<->weekly/monthly horizon agreement | (new) | NEW | medium | high | none | 3 |

## Delta vs baseline

### RESOLVED (0)
None. No defect from the baseline was fixed.

### CHANGED (3)
- **SHARED-C01** (horizon staleness): The daily spine received a partial forward rebuild — `dim_time_spine_daily` max `2031-04-17` -> `2031-06-12` (`4,575` -> `4,631` rows), narrowing its shortfall from 55 days to `~9` days vs a today-rebuild (`2031-06-21` / `4,640` rows). But `dim_time_spine_weekly` (max `2031-04-14`) and `dim_time_spine_monthly` (max `2031-04-01`) did **not** move, so the previously-uniform 55-day shortfall is now split per-grain (daily `~9d`, weekly `~9wk`, monthly `2mo`). Root cause intact: zero spine references in `cron.sh` / `cron_preview.sh` / `refresh.py` / `scripts/refresh/`. Blast radius confined to the spine axis — `api_consensus_validators_active_daily` (the only daily-bridged mart) is forecast-free (`1,643` rows, `0` at `date >= '2031-04-01'`, max `2026-06-07`), so no served metric is affected today. Not incident A (contiguous, dup-free series). Severity held at medium.
- **SHARED-C06** (bridge key mismatch): The structural `day` vs `date` asymmetry on `daily_spine_to_consensus_validators_active` (`semantic/relationships/time_spines.yml` L29-41) is unchanged and still the only mismatch of 13 bridges, but the asserted functional consequence was refuted: `scripts/semantic/build_registry.py` `validate_registry` (L670-705) inspects only `left_model`/`right_model` existence and `allow_any_join` approval — `left_keys`/`right_keys` are never read (repo-wide grep finds them only in `semantic/relationships/*.yml`, no Python loader/manifest/MetricFlow config consumes them); the bridge routes on `via_entity: day` (valid on the spine). Cosmetic authoring inconsistency, not a break. Severity medium -> low.
- **SHARED-C09** (daily row count): Warehouse re-measure shows `count(*)=4,631`, `uniqExact(day)=4,631` (no dupes), min `2018-10-08`, max `2031-06-12` — a clean `+56` contiguous forward extension vs baseline `4,575`/`2031-04-17`, still `9` short of a today-rebuild (`4,640`/`2031-06-21`). Clean-but-stale, so CHANGED. Severity medium -> low.

### STILL CONFIRMED (11)
- **SHARED-C02**: All three `config()` blocks in `models/shared/marts/dim_time_spine_{daily,weekly,monthly}.sql` set only `materialized='table'`; no `engine`/`order_by`. `dbt_project.yml` has no `models.gnosis_dbt.shared` block, so no directory default injects a key — dbt-clickhouse falls through to `ORDER BY tuple()` (no PK). DDL unobservable here (MCP guard blocks `CREATE`/`SYSTEM`/`SHOW`), so the `tuple()` outcome stays inferred; severity lowered to low (cosmetic at `~4,600` rows).
- **SHARED-C03**: Zero `tags:` on any spine in `models/shared/marts/schema.yml` or the three `config()` blocks (grep `0` matches across `models/shared/`); the project tags pervasively (per-model and `dbt_project.yml` `+tags`), and there is no `models: shared: +tags` block, so the absence is total. `tag:time_spine` selection impossible.
- **SHARED-C04**: No pipeline selection and no documented runbook for `dim_time_spine_*`; grep over `scripts/`, `docs/`, `Makefile` returns only `scripts/semantic/generate_graph_diagram.py` (pure visualization). The `~9`-day daily shortfall against frozen weekly/monthly corroborates exclusion from regular runs. Build-time metadata unavailable (`SYSTEM` guard).
- **SHARED-C05**: `dim_time_spine_daily.sql` L4/L6 hardcode `toDate('2018-10-08')` twice (the only executable occurrences; weekly/monthly reference genesis only in comments). `dbt_project.yml` `vars:` has `circles_target_group_start_date` (L16) and `gnosis_app_wau_floor_date` (L20) but no `gnosis_chain_genesis_date`. One-line fix available against the established var pattern.
- **SHARED-C07**: `min(month)=2018-10-01`, 7 days pre-genesis (`2018-10-08`); `schema.yml` monthly description (L48-58) omits the partial-period caveat that `monthly.sql` L4-5 carries. The daily spine has `0` rows in `2018-10-01..2018-10-07` (its min is `2018-10-08`), so the phantom days carry zero joinable rows — defect is purely the missing doc note.
- **SHARED-C08**: No CI guard enforces spine-bridge registration; `build_registry.py` `load_relationships` (L211-227) only globs+copies, and `validate_registry` (L670-705) has no inverse loop asserting each weekly/monthly mart is registered. The gap is live: `152` `*_weekly`/`*_monthly` marts exist but only `~11-12` appear as `right_model` in `time_spines.yml` (e.g. `api_execution_gpay_active_users_weekly`, `api_execution_gnosis_app_swaps_monthly` are unbridged).
- **SHARED-C10**: `dim_time_spine_weekly` byte-for-byte unchanged from baseline — `count=654`, `uniqExact=654`, max `2031-04-14`, `0` rows beyond baseline max. A re-projection from current daily would reach `toMonday(2031-06-12)=2031-06-09` (absent today), so weekly is `~9` weeks short. Pure persisting staleness; held at medium (larger live shortfall than the partially-rebuilt daily).
- **SHARED-C11**: `dim_time_spine_monthly` unchanged — `count=151`, `uniqExact=151`, max `2031-04-01`, `0` new tail rows. Missing `2031-05-01` and `2031-06-01` (both `count=0`), `2` months short. Same isolated-daily-rebuild root cause as N01; held at medium.
- **SHARED-C12**: Daily spine dense/gap-free — `lagInFrame` diff yields `0` gaps `>1` day (the lone `diff>1` is the first-row null default `=` genesis epoch `17812`, not a missing date); `count==uniqExact==4,631`; the rebuilt tail `2031-04-18..2031-06-12` = `56` rows, dense and dup-free. Invariant holds across the rebuild seam.
- **SHARED-C13**: Weekly all Monday-aligned — `countIf(toDayOfWeek(week)!=1)=0` over `654/654` rows; consecutive-week spacing uniformly 7 days (`countIf(diff!=7)=0` excl. first). No skips/dupes.
- **SHARED-C14**: Monthly all first-of-month — `countIf(toDayOfMonth(month)!=1)=0` over `151/151` rows; consecutive-month `dateDiff('month')=1` (`countIf(!=1)=0` excl. first). No skips/dupes.
- **SHARED-C15**: Only daily/weekly/monthly spines+bridges exist (`models/shared/marts/` and `time_spines.yml`). Real `22` `api_quarterly_data_*` ESG marts and p2p sub-day marts (`models/p2p/marts/api_p2p_discv5_clients_daily.sql` etc.) have no spine bridge of any grain — grep `quarterly|esg|hourly` over all of `semantic/relationships/` returns `0` matches. Live gap.

### NEW (1)
- **SHARED-N01** (cross-grain horizon divergence): Weekly and monthly are `SELECT DISTINCT toMonday(day)` / `toStartOfMonth(day) FROM ref(daily)` projections whose own headers (`weekly.sql` L5-7, `schema.yml`) assert "granularity boundaries always agree." At baseline all three were frozen together at `~2031-04` so the invariant **held**. The isolated daily-only rebuild (`+56d` to `2031-06-12`, weekly/monthly `0` tail rows) **broke** it: `toMonday(2031-06-12)=2031-06-09` is absent from weekly (`count=0`), and `2031-06-01`/`2031-05-01` are absent from monthly (`count=0`). A cross-grain MetricFlow join near `2031-05`/`2031-06` resolves on the daily axis but finds no weekly/monthly bucket. Distinct regression layered on top of the shared staleness; not incident A. Severity medium.

### UNVERIFIABLE / UNRESOLVED (0)
None. All 16 cases reached evidentiary closure with `all_sufficient=true` in round 3.

## Evidence appendix

Spine horizon and row counts (SHARED-C01 / C09 / C10 / C11), via `mcp__cerebro-dev__execute_query`:
```sql
SELECT count(*), uniqExact(day), min(day), max(day) FROM dbt.dim_time_spine_daily;
-- daily: count=4631, uniqExact=4631, min=2018-10-08, max=2031-06-12 (baseline 4575 / 2031-04-17)
SELECT count(*), uniqExact(week), max(week) FROM dbt.dim_time_spine_weekly;
-- weekly: count=654, uniqExact=654, max=2031-04-14 (unchanged from baseline)
SELECT count(*), uniqExact(month), max(month) FROM dbt.dim_time_spine_monthly;
-- monthly: count=151, uniqExact=151, max=2031-04-01 (unchanged from baseline)
```
Expected-today computations: `addYears(today(),5)=2031-06-21` -> daily today-rebuild `4,640` rows (daily `9` short); `toMonday(2031-06-21)=2031-06-16`; `toStartOfMonth(2031-06-21)=2031-06-01`.

Blast-radius confinement (SHARED-C01):
```sql
SELECT count(*), countIf(date>='2031-04-01'), max(date) FROM dbt.api_consensus_validators_active_daily;
-- 1643 rows, 0 at date>=2031-04-01, max_date=2026-06-07 (forecast-free mart; shortfall confined to spine axis)
```

N01 boundary-agreement violation (SHARED-N01):
```sql
SELECT count(*) FROM dbt.dim_time_spine_weekly  WHERE week='2031-06-09';  -- 0  (=toMonday(daily max))
SELECT count(*) FROM dbt.dim_time_spine_monthly WHERE month='2031-06-01'; -- 0  (=toStartOfMonth(daily max))
SELECT count(*) FROM dbt.dim_time_spine_monthly WHERE month='2031-05-01'; -- 0
SELECT count(*) FROM dbt.dim_time_spine_weekly  WHERE week  > toDate('2031-04-14'); -- 0 tail rows
SELECT count(*) FROM dbt.dim_time_spine_monthly WHERE month > toDate('2031-04-01'); -- 0 tail rows
```

Gap-free / alignment invariants (SHARED-C12 / C13 / C14):
```sql
-- C12: daily gaps
SELECT countIf(diff>1), max(diff) FROM (SELECT day - lagInFrame(day) OVER (ORDER BY day) AS diff FROM dbt.dim_time_spine_daily);
-- countIf=1 (max(diff)=17812 = first-row lagInFrame default = genesis epoch, artifact); all real adjacent diffs = 1
-- seam 2031-04-18..2031-06-12 = 56 rows, dense, dup-free
-- C13: weekly Monday-alignment
SELECT count(*) FROM dbt.dim_time_spine_weekly WHERE toDayOfWeek(week)!=1;  -- 0 (654/654)
-- C14: monthly first-of-month
SELECT count(*) FROM dbt.dim_time_spine_monthly WHERE toDayOfMonth(month)!=1; -- 0 (151/151)
```

Pre-genesis bucket (SHARED-C07):
```sql
SELECT min(month) FROM dbt.dim_time_spine_monthly; -- 2018-10-01 (7d pre-genesis 2018-10-08)
SELECT count(*) FROM dbt.dim_time_spine_daily WHERE day>='2018-10-01' AND day<='2018-10-07'; -- 0
```

Source / code checks (code_only):
- **C02**: `models/shared/marts/dim_time_spine_{daily,weekly,monthly}.sql` L1 = `config(materialized='table')` only; `dbt_project.yml` has no `models.gnosis_dbt.shared` engine/order_by default. SHOW CREATE / `system.tables` blocked by MCP keyword guard (`Forbidden keyword: CREATE` / `SYSTEM`).
- **C03**: `grep tags models/shared/` -> `0` matches; `dbt_project.yml` has no `models: shared:` block.
- **C04**: `grep dim_time_spine|time_spine|spine` over `scripts/`, `docs/`, `Makefile` -> only `scripts/semantic/generate_graph_diagram.py`.
- **C05**: `dim_time_spine_daily.sql` L4/L6 `toDate('2018-10-08')`; `dbt_project.yml` `vars:` L16/L20 (no genesis var).
- **C06**: `time_spines.yml` L29-41 `left_keys:[day]`/`right_keys:[date]`; `build_registry.py` L670-705 never reads keys; grep `left_keys|right_keys` -> only `semantic/relationships/*.yml`.
- **C08**: `152` `*_weekly`/`*_monthly` marts vs `~11-12` `right_model` registrations in `time_spines.yml`; `build_registry.py` L211-227 / L670-705 (no inverse coverage loop); nothing under `scripts/checks/`.
- **C15**: `models/shared/marts/` holds only the three spines; grep `quarterly|esg|hourly` over `semantic/relationships/` -> `0`.

## Review log (>=3 rounds per case)

- **SHARED-C01**: R1 CONFIRMED (daily `2031-06-12`, weekly/monthly frozen) -> challenge: prove downstream blast radius on a bridged metric -> R2 CHANGED (per-grain split demonstrated; daily axis retains 59 rows in window vs weekly/monthly 0) -> challenge: confirm the daily-bridged mart is empty up there -> R3 CHANGED (`api_consensus_validators_active_daily` `0` rows `>=2031-04-01`, blast radius confined to spine axis). Final CHANGED/medium.
- **SHARED-C02**: R1 CONFIRMED (no order_by in source) -> challenge: run SHOW CREATE for actual DDL -> R2 CONFIRMED, DDL blocked by `SYSTEM`/`CREATE` guard, `tuple()` stays inferred, sev -> low -> challenge: cite adapter default / sibling order_by -> R3 CONFIRMED (no `models.shared` order_by default; siblings set `+order_by` explicitly). Final CONFIRMED/low.
- **SHARED-C03**: R1 CONFIRMED (no tags) -> challenge: show other models that DO carry tags -> R2 CONFIRMED (multiple tagged consensus models + sector `+tags`) -> challenge: confirm no `models: shared: +tags` default -> R3 CONFIRMED (no shared block at all). Final CONFIRMED/low.
- **SHARED-C04**: R1 CONFIRMED (no selection) -> challenge: check table modification metadata -> R2 CONFIRMED, `SYSTEM` blocked, row-state timing argument stands -> challenge: grep docs/Makefile for a runbook -> R3 CONFIRMED (only graph-diagram generator; no runbook/target/cron). Final CONFIRMED/low.
- **SHARED-C05**: R1 CONFIRMED (hardcoded literal) -> challenge: size edit-points across repo -> R2 CONFIRMED (only executable site is daily.sql L4/L6) -> challenge: cite existing var pattern -> R3 CONFIRMED (var anchors at L16/L20 consumed via `var()`). Final CONFIRMED/low.
- **SHARED-C06**: R1 CONFIRMED (structural mismatch) -> challenge: read build_registry.py to determine join keying -> R2 CHANGED (keys never consumed; via_entity routes; sev medium -> low) -> challenge: repo-wide grep for any key consumer -> R3 CONFIRMED stance (keys only in source yml). Final CHANGED/low.
- **SHARED-C07**: R1 CONFIRMED (`min=2018-10-01`, no caveat) -> challenge: prove phantom days contribute zero -> R2 CONFIRMED (no since-genesis mart joins the bucket) -> challenge: concrete `0`-row check on the 7 days -> R3 CONFIRMED (daily `0` rows `2018-10-01..07`). Final CONFIRMED/low.
- **SHARED-C08**: R1 CONFIRMED (no guard) -> challenge: read build_registry.py for inverse loop -> R2 CONFIRMED (no inverse coverage loop) -> challenge: prove a live unbridged mart exists -> R3 CONFIRMED (`152` marts vs `~11` registered). Final CONFIRMED/low.
- **SHARED-C09**: R1 CHANGED (`4,631`/`2031-06-12`, `9` short) -> challenge: verify clean contiguous extension not corrupt -> R2 CHANGED (`count==uniqExact`, seam dense) -> R3 CHANGED (settled, re-measured identical). Final CHANGED/low.
- **SHARED-C10**: R1 CONFIRMED (`654`/`2031-04-14` unchanged) -> challenge: confirm `=N01` divergence (would reach `2031-06-09` if re-projected) -> R2 CONFIRMED (`2031-06-09` absent) -> challenge: reconcile medium vs C09 low -> R3 CONFIRMED (frozen -> larger shortfall). Final CONFIRMED/medium.
- **SHARED-C11**: R1 CONFIRMED (`151`/`2031-04-01` unchanged) -> challenge: pin shortfall at 2 months / tie to N01 -> R2 CONFIRMED (`2031-05-01` and `2031-06-01` absent) -> challenge: confirm severity basis matches C10 -> R3 CONFIRMED (internally consistent). Final CONFIRMED/medium.
- **SHARED-C12**: R1 CONFIRMED (`0` gaps) -> challenge: re-confirm gap-free across rebuilt tail + no dupes -> R2 CONFIRMED (seam `56`/`56`, `count==uniqExact`) -> R3 CONFIRMED (lone `diff>1` is first-row artifact). Final CONFIRMED/low.
- **SHARED-C13**: R1 CONFIRMED (non-Monday `0`) -> challenge: verify uniform 7-day spacing -> R2 CONFIRMED (`countIf(diff!=7)=0`) -> R3 CONFIRMED (settled). Final CONFIRMED/low.
- **SHARED-C14**: R1 CONFIRMED (non-first `0`) -> challenge: verify exact 1-month spacing -> R2 CONFIRMED (`countIf(dateDiff!=1)=0`) -> R3 CONFIRMED (settled). Final CONFIRMED/low.
- **SHARED-C15**: R1 CONFIRMED (only 3 spines) -> challenge: name real p2p + ESG marts needing a bridge -> R2 CONFIRMED (quarterly ESG + p2p marts exist) -> challenge: prove no bridge of any grain anywhere -> R3 CONFIRMED (`0` matches across all relationship files). Final CONFIRMED/low.
- **SHARED-N01**: R1 NEW (daily diverged from weekly/monthly) -> challenge: show daily-implied boundaries absent from weekly/monthly -> R2 CONFIRMED mechanism (`2031-06-09`/`2031-06-01`/`2031-05-01` all `count=0`) -> challenge: confirm newly-introduced (baseline agreed) -> R3 NEW (before-agree -> after-diverge justifies NEW). Final NEW/medium.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P1 (ESCALATE) | Add the three spines to a scheduled rebuild and **rebuild all three together** — the isolated daily-only rebuild caused N01. A joint nightly/weekly run closes C01, C09, C10, C11, N01 at once. | `models/shared/marts/dim_time_spine_{daily,weekly,monthly}.sql`; `cron.sh` / `cron_preview.sh` / `refresh.py` |
| P1 (NEW) | Re-derive weekly/monthly from the current daily immediately to restore the boundary-agreement invariant (eliminates the `2031-05`/`2031-06` cross-grain join gap). | `dim_time_spine_weekly.sql`, `dim_time_spine_monthly.sql` |
| P2 (KEEP) | Add a CI guard (analogue of `check_api_tags.py`) asserting every `*_weekly`/`*_monthly` mart is registered as a `right_model` in `time_spines.yml`; `152` marts vs `~11` registered today. | `scripts/checks/`, `semantic/relationships/time_spines.yml` |
| P3 (KEEP) | Promote genesis `2018-10-08` to a `gnosis_chain_genesis_date` var (matches `circles_target_group_start_date` / `gnosis_app_wau_floor_date`); swap the two literals in daily.sql. | `dbt_project.yml` `vars:`, `dim_time_spine_daily.sql` L4/L6 |
| P3 (KEEP) | Add `tags: [shared, time_spine]` to the three spines so `tag:time_spine` selection works and CI tooling can categorise them. | `models/shared/marts/schema.yml` + the three `config()` blocks |
| P3 (KEEP) | Set explicit `order_by` (e.g. `order_by='day'` / `week` / `month`) in each spine `config()` to give a PK index for cross-grain range scans (cosmetic at current scale). | `dim_time_spine_{daily,weekly,monthly}.sql` |
| P4 (KEEP) | Add an hourly and/or quarterly spine + bridges so p2p (sub-day) and the `22` `api_quarterly_data_*` ESG marts can compose cross-sector. | `models/shared/marts/`, `semantic/relationships/time_spines.yml` |
| P4 (KEEP, downgraded) | Document the `day` vs `date` bridge mismatch or rename to `date` for consistency (cosmetic — keys are inert; `via_entity` routes the join). | `semantic/relationships/time_spines.yml` L29-41 |
| P4 (KEEP) | Add the partial-period caveat to the monthly `schema.yml` description (2018-10 bucket starts `2018-10-01`, 7 days pre-genesis; carries zero joinable rows). | `models/shared/marts/schema.yml` |

No DROP rows: nothing was resolved.
