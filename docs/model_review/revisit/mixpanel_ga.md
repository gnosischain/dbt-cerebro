# Model review (revisit 2026-06-21): mixpanel_ga

Baseline: `docs/model_review/mixpanel_ga.md` (dated `2026-06-11`); re-verified on `2026-06-21` across `3` rounds. All `21` baseline cases were re-checked: `2` RESOLVED (`C16`, `C17`), `2` CHANGED (`C07`, `C20`), and `17` STILL CONFIRMED — including both critical join-default bugs (`C13`/`C19`, `matched_mp=1` for 100% of `23,239` rows vs true 43%) and the ~7x device over-count on the public overview API (`C15`/`C21`). PRIVACY-TIERED sector: data queries were available in this cluster (not blocked as the unit note feared), so most data-state claims were measured directly.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| MIXPANELGA-C01 | — | Jinja eval-order: `config()` reads `start_month` before `{% set %}`; append path is dead, always delete+insert | high | CONFIRMED | high | high | none | 3 |
| MIXPANELGA-C02 | — | `coalesce(first_touch_campaign,'')` over LEFT JOIN w/o `join_use_nulls` → blank attribution | medium | CONFIRMED | low | high | none | 3 |
| MIXPANELGA-C03 | — | 3 row-level pseudonym intermediates lack `internal_only`/`expose_to_mcp:false` | medium | CONFIRMED | medium | high | none | 3 |
| MIXPANELGA-C04 | — | `int_..._traffic_daily` grain includes lifetime `initial_referrer_domain` (combinatorial) | medium | CONFIRMED | low | high | none | 3 |
| MIXPANELGA-C05 | — | unified model emits 13 cols, `schema.yml` documents 8; `contract.enforced:false` | medium | CONFIRMED | low | high | none | 3 |
| MIXPANELGA-C06 | — | `client_first_events` + `gpay_campaign_cohorts` have no `schema.yml` entry / no tests | medium | CONFIRMED | low | high | none | 3 |
| MIXPANELGA-C07 | — | 8 weekly marts had no `schema.yml`/tests; 4 `api_` views lack api/granularity tags | medium | CHANGED | low | high | none | 3 |
| MIXPANELGA-C08 | — | per-user fct tables lack `expose_to_mcp:false` (MCP-exposable pseudonym grain) | medium | CONFIRMED | medium | high | none | 3 |
| MIXPANELGA-C09 | — | `today()`-relative windows baked into full-rebuild tables, no staleness caveat | medium | CONFIRMED | low | medium | none | 3 |
| MIXPANELGA-C10 | — | seed `mixpanel_conversion_events.csv` 3 rows, INNER JOIN drops new events, no test | low | CONFIRMED | low | high | none | 3 |
| MIXPANELGA-C11 | — | no unique-key tests on any mart (RMT dup detection gap) | low | CONFIRMED | low | high | none | 3 |
| MIXPANELGA-C12 | — | `stg_..._events` versionless `ReplacingMergeTree`, nondeterministic survivor | low | CONFIRMED | low | high | none | 3 |
| MIXPANELGA-C13 | P0-20 | `matched_mp` always 1 (`0 IS NOT NULL`=TRUE on UInt64 default) | critical | CONFIRMED | critical | high | none | 3 |
| MIXPANELGA-C14 | P0-20 | crossdomain activity flags always 1 (`'' IS NOT NULL`=TRUE on String gp_safe) | critical | CONFIRMED | medium | high | none | 3 |
| MIXPANELGA-C15 | — | `unique_devices = sum(per-group uniqExact)` over-counts devices N× | high | CONFIRMED | high | high | none | 3 |
| MIXPANELGA-C16 | — | `cumulative_users` has no hard `>=2025-11-12` filter | medium | RESOLVED | low | high | none | 3 |
| MIXPANELGA-C17 | — | semantic `cumulative_accounts` registered `agg:sum` (multiplies across weeks) | medium | RESOLVED | resolved | high | none | 3 |
| MIXPANELGA-C18 | — | `onboarding_conversion_rate` can exceed 100% (uncapped) | low | CONFIRMED | low | high | none | 3 |
| MIXPANELGA-C19 | P0-20 | data-state: `matched_mp=1` for all stored rows; 70-83% rate unobservable | critical | CONFIRMED | critical | high | none | 3 |
| MIXPANELGA-C20 | P0-20 | data-state: crossdomain delay/allowance flags inflated to full matched set | critical | CHANGED | low | high | none | 3 |
| MIXPANELGA-C21 | — | data-state: `unique_devices` over-counted ~N× | high | CONFIRMED | high | high | none | 3 |

## Delta vs baseline

### RESOLVED (2)
- **C16** (`fct_mixpanel_ga_gnosis_app_daily.sql`): the missing `date>='2025-11-12'` hard filter is moot in current data. Upstream `int_execution_gnosis_app_user_events` has `min(block_timestamp)` date `= 2025-11-12` and `countIf(block_timestamp<'2025-11-12')=0`. The running total seeds exactly at the baseline: first row `cumulative_users=852 = first new_users=852` (no carried-in accumulation). Downgraded to `low` — the absent filter remains a latent footgun if pre-baseline data is ever backfilled. Incident: none.
- **C17** (`semantic/authoring/mixpanel_ga/semantic_models.yml`): the `agg:sum` defect on cumulative measures is fixed. All five cumulative/running-total measures now register `agg:max` — `cumulative_accounts` on both acquisition_weekly models (L346, L386), `cumulative_users` on client_conversions_weekly (L1013) and gnosis_app_daily (L1049), and `cumulative_mp_matched` (L1052). Fully resolved. Incident: none.

### CHANGED (2)
- **C07** (8 weekly marts): the documentation/test half is RESOLVED — all 8 now have `marts/schema.yml` entries and the 4 `fct_` carry `elementary.schema_changes` tests. The residual: the 4 `api_` weekly views still carry only `tags=['production','mixpanel_ga']` (no `granularity:weekly`/tier tags), whereas the `api_` daily views (e.g. `api_mixpanel_ga_overview_daily.sql` L4: `tags=[...,'tier3','granularity:daily']`) do — making the weekly views in-sector outliers. The `api:` prefix is intentionally absent (excluded via `api.exclude_from_api`), so `check_api_tags.py` is bypassed by design; the real gap is the missing `granularity:weekly` tag. Severity `low`. Incident: none.
- **C20** (`fct_mixpanel_ga_gpay_crossdomain_daily.sql`): the baseline data-state claim ("inflated to the full matched-user set for ALL historical dates") is RETRACTED by data — across the full 122-day history `sum(matched_users_any)=17`, `sum(users_with_delay_activity_7d)=0`, `sum(users_with_allowance_changes_30d)=1`, and `0` days show inflation. This is correct-by-coincidence: the GP/Safe migration collapsed the upstream match universe. The code defect itself is unchanged and latent-armed (tracked at `medium` on C14). Data-state severity dropped to `low`. Incident: none (migration-driven, attribution `other`).

### STILL CONFIRMED (17)
Critical (data-active):
- **C13 / C19** (`fct_mixpanel_ga_gnosis_app_users.sql`): `matched_mp=1` for all `23,239` rows (`sum(matched_mp)=23,239`). `13,253` rows have `mp_user_id_hash=0` (join default), all flagged matched; only `9,986` are true matches (`mp_user_id_hash=user_pseudonym`). Membership cross-check: all `13,253` hash=0 pseudonyms are genuinely absent from the distinct mp set (`0/13,253` present); all `9,986` hash!=0 are present. True match rate `= 9,986/23,239 = 43%` vs reported 100%. `getSetting('join_use_nulls')=false`, no `join_use_nulls=1` hook. The corrupt diagnostic is MCP-reachable (registered `quality_tier:approved` with `matched_mp` dimension + `agg:sum` measure). Needs `join_use_nulls=1` fix + table rebuild. Incident: none.

High:
- **C01** (`int_execution_gnosis_app_events_mixpanel_unified.sql`): `config()` at L4 reads `start_month` inside `incremental_strategy=('append' if start_month else 'delete+insert')`; `{% set start_month = var(...) %}` is L15 (after config). At config-eval `start_month` is Undefined/falsy → strategy is ALWAYS `delete+insert`; the append branch is dead code. Body branch L51-53 emits a whole-month date-windowed delete predicate, so the write is partition-grain-aligned and SAFE — no data loss, the bug only renders the append path unreachable and the config misleading. Incident: none.
- **C15 / C21** (`fct_mixpanel_ga_overview_daily.sql`, `api_mixpanel_ga_overview_daily.sql`): `unique_devices = sum(unique_devices)` from `int_mixpanel_ga_events_daily` (which is `uniqExact(device_id_hash)` per `(date,event_name,event_category)`), so a device firing N event types is counted ~N times. Measured ~7x: `2026-06-18` `17,595` vs `2,151` true (8.2x); `2026-06-19` `15,499` vs `2,295` (6.8x); `2026-06-20` `14,583` vs `2,079` (7.0x). The fan-out factor (`count()/uniqExact(device)` per `(date,device,event_name)`) matches the inflation ratio to two decimals. Flows verbatim to the public overview API (`SELECT *`, `agg:average` over inflated dailies); the `schema.yml` description still reads "Distinct device_id_hash count". Fix well-defined: re-aggregate `uniqExact(device_id_hash)` from staging. Incident: none.

Medium:
- **C03** (`int_mixpanel_ga_user_lifecycle.sql`, `int_mixpanel_ga_users_daily.sql`, `int_execution_gnosis_app_events_mixpanel_unified.sql`): all three row-level pseudonym intermediates still lack `internal_only`/`privacy:tier_internal`/`expose_to_mcp:false` that peer models (e.g. `int_mixpanel_ga_client_first_events` L7-8) carry. All three ARE registered in `semantic_models.yml` at `quality_tier:candidate` with `user_id_hash`/`user_pseudonym` dimensions. The `dbt_project.yml` claim of a `quality_tier:blocked` gate is FALSE — no such tier exists; `scaffold_metrics.py:137` skips a model ONLY when `meta.expose_to_mcp is False`, and `build_registry.py` serves both approved and candidate tiers. Gap is fully MCP-reachable. Incident: none.
- **C08** (`fct_mixpanel_ga_gpay_users.sql`, `fct_mixpanel_ga_gnosis_app_users.sql`): both per-user-grain facts lack `expose_to_mcp:false` (unlike `api_mixpanel_ga_users_daily` which sets it). Both registered `quality_tier:approved` with `user_pseudonym` as primary entity. Same tier→exposure rule as C03 → live, reachable exposure of pseudonym grain. Incident: none.
- **C14** (`fct_mixpanel_ga_gpay_crossdomain_daily.sql` L49-50): `max(if(da.gp_safe IS NOT NULL,1,0))` over a String LEFT JOIN, no `join_use_nulls=1`. `getSetting('join_use_nulls')=false` confirmed; synthetic LEFT JOIN proves an unmatched String returns `''` whose `IS NOT NULL` is TRUE → flag 1. Currently masked (match universe collapsed to `sum(matched_users_any)=17`/122 days), but latent-armed: re-inflates the instant `fct_mixpanel_ga_gpay_users` repopulates at pre-migration scale. Incident: none (migration-driven masking, attribution `other`).

Low:
- **C02** (`int_mixpanel_ga_client_first_events.sql` L36-44): no `join_use_nulls` hook; `coalesce(a.first_touch_campaign,'unknown')` yields `''` for unmatched. Latent only — `countIf(first_touch_campaign='')=0` because conv ⊆ acquisition by construction (conv side adds an INNER JOIN to the seed atop the same `is_production=1 AND is_identified=1` filter as `int_mixpanel_ga_user_acquisition`). Incident: none.
- **C04** (`int_mixpanel_ga_traffic_daily.sql` L30): grain still `(date, referrer_domain, initial_referrer_domain)`; fan-out 3.2-4.0x rows per referrer_domain per day. No registered measure mis-aggregates (`event_count` is `agg:sum` additive; `unique_users` is `agg:average`); only a raw-table footgun. Incident: none.
- **C05** (unified model + `intermediate/schema.yml`): SQL emits 13 cols, schema documents 8 (missing `amount_usd, event_dedup_key, provenance_model, device_type, country_code`), `contract.enforced:false`. The single downstream consumer `int_execution_gnosis_app_user_events_unified.sql` selects all 5 by EXPLICIT name (L56-61), so data propagates correctly; pure doc-completeness gap. Incident: none.
- **C06** (`int_mixpanel_ga_client_first_events.sql`, `int_mixpanel_ga_gpay_campaign_cohorts.sql`): neither has a `schema.yml` entry or any dbt test, though both feed live marts (e.g. `fct_mixpanel_ga_gpay_campaign_metrics_weekly` returned `589` rows). Both now semantic-registered + carry `internal_only`/`expose_to_mcp:false` in their own configs. Live-but-untested. Incident: none.
- **C09** (`fct_mixpanel_ga_gpay_users.sql`, `fct_mixpanel_ga_gpay_crossdomain_daily.sql`): `today()-30`/`today()-7` windows baked into full-rebuild tables, no `schema.yml` staleness caveat. Mitigated by daily `tag:production` cron (crossdomain `max(date)=2026-06-20`, fresh). Residual is the missing doc caveat. Incident: none.
- **C10** (`seeds/mixpanel_conversion_events.csv`): 3 rows, INNER JOIN drives capture, no test. `'Mint'` (`88,334` events / `5,649` users) is an uncaptured event but does NOT supersede the seed's `'Success - Circles mint'` (`76,687` / `5,891` users) — both fire concurrently for ~96% of overlapping users (`both=5,427`), so `crc_minted` is not silently undercounting. Coverage-latency note. Incident: none.
- **C11** (`marts/schema.yml`): no `unique`/`unique_combination_of_columns` test on any mart. Probes clean: `fct_mixpanel_ga_overview_daily` `122=122`; `fct_mixpanel_ga_gnosis_app_users` `23,239=23,239`; `fct_mixpanel_ga_gpay_campaign_metrics_weekly` weekly composite `589=589`, 0 dupes. Latent design gap. Incident: none.
- **C12** (`stg_mixpanel_ga__events.sql`): versionless `ReplacingMergeTree()`, `order_by=(project_id,event_name,event_time,insert_id)`. Probe: `5,770,533` rows, `uniqExact(tuple)=5,770,533`, `0` collisions. No uniqueness test exists, so the "intermittent test failure" framing is moot; pure latent dedup-correctness risk. Incident: none.
- **C18** (`fct_mixpanel_ga_funnel_daily.sql` L54-58): `onboarding_conversion_rate = passkey_logins/greatest(welcome_visitors,1)`, uncapped. `passkey_logins` and `welcome_visitors` are independent `uniqExactIf` predicates (no funnel join), so >1.0 is structurally reachable; currently `0/122` days exceed 1.0, max `0.8705`. CONFIRMED-latent. Incident: none.

### NEW (0)
None.

### UNVERIFIABLE / UNRESOLVED (0)
None — all 21 cases reached agreed status at `>=3` rounds.

## Evidence appendix

**C01 / C05 (code-only, `int_execution_gnosis_app_events_mixpanel_unified.sql`)** — `config()` L4 `incremental_strategy=('append' if start_month else 'delete+insert')`; `{% set start_month %}` L15, `end_month` L16. Body L51-53 `{% if start_month and end_month %} AND toStartOfMonth(event_date) >= start_month AND <= end_month`. SQL SELECT = 13 cols (`event_ts,event_date,user_pseudonym,event_source,event_kind,event_subkind,amount_usd,event_dedup_key,provenance_model,device_type,country_code,page_path,bottom_sheet`); `intermediate/schema.yml` L499-517 = 8 cols; `contract.enforced:false`. Downstream `int_execution_gnosis_app_user_events_unified.sql` L56-61 selects all 5 "undoc" cols by name.

**C02 (sql + code)** — `int_mixpanel_ga_client_first_events`: `count()=14,325`; `first_touch_campaign=''` → `0`; `'unknown'` → `12,262`; real campaign → `2,063`. Conv users `8,595`, all present in `int_mixpanel_ga_user_acquisition` (`conv_no_acq=0`). `getSetting('join_use_nulls')=false`.

**C03 / C08 (code-only)** — `int_mixpanel_ga_user_lifecycle` L1350, `int_mixpanel_ga_users_daily` L905, unified L481 registered `quality_tier:candidate`. `fct_mixpanel_ga_gpay_users` L435 / `fct_mixpanel_ga_gnosis_app_users` L474 registered `quality_tier:approved`, `user_pseudonym` primary entity. Zero `quality_tier:blocked` in `semantic_models.yml`. `scaffold_metrics.py:137` skips only on `expose_to_mcp is False`; `build_registry.py:402-406` maps approved→approved, others→candidate (both served).

**C04 (sql)** — `int_mixpanel_ga_traffic_daily` `2026-06-08`: `56` rows at grain, `14` distinct `referrer_domain` → 4.0x; recent days fan-out `3.16-4.0x`; `18-26` distinct `initial_referrer_domain`. `GROUP BY date, referrer_domain, initial_referrer_domain` (L30).

**C06 (code-only)** — grep of `intermediate/schema.yml` + `marts/schema.yml` for `int_mixpanel_ga_client_first_events` / `int_mixpanel_ga_gpay_campaign_cohorts`: no `model:` entry (EXIT 1). Both registered `semantic_models.yml` L1016 / L1102. `fct_mixpanel_ga_gpay_campaign_metrics_weekly` materialized (`589` rows).

**C07 (code-only)** — `marts/schema.yml` entries: campaign_funnel_weekly L1076/1122, client_conversions_weekly L1156/1203, gpay_campaign_metrics_weekly L1240/1286, gpay_campaign_retention_weekly L1326/1371. 4 `api_` weekly `.sql`: `tags=['production','mixpanel_ga']` only. Daily `api_mixpanel_ga_overview_daily.sql` L4 `tags=[...,'tier3','granularity:daily']`.

**C09 (sql + code)** — `fct_mixpanel_ga_gpay_users.sql` L47/L54 `WHERE date>=today()-30`; `fct_mixpanel_ga_gpay_crossdomain_daily.sql` L27/L39 `today()-7`/`today()-30`. `SELECT max(date) FROM fct_mixpanel_ga_gpay_crossdomain_daily` → `2026-06-20`.

**C10 (sql)** — seed = 3 rows. `'Success - Circles mint'`: `76,687` events / `5,891` users; `'Mint'`: `88,334` / `5,649`; both-fire users `5,427`; either `6,113`.

**C11 (sql)** — `fct_mixpanel_ga_overview_daily` `count()=122`, `uniqExact(date)=122`; `fct_mixpanel_ga_gnosis_app_users` `23,239=23,239`; `fct_mixpanel_ga_gpay_campaign_metrics_weekly` `count()=589`, `uniqExact((week,utm_campaign,utm_source,utm_medium))=589`, dupes `0`.

**C12 (sql + code)** — `engine='ReplacingMergeTree()'` (no version), `order_by=(project_id,event_name,event_time,insert_id)`. `count()=5,770,533`, `uniqExact(tuple)=5,770,533`, collisions `0`.

**C13 / C19 (sql, `fct_mixpanel_ga_gnosis_app_users`)** — `count()=23,239`; `sum(matched_mp)=23,239` (avg `1.0`); `countIf(mp_user_id_hash=0)=13,253` (all matched_mp=1); `countIf(mp_user_id_hash IS NULL)=0`; `countIf(matched_mp=1 AND mp_user_id_hash=user_pseudonym)=9,986`. Membership: `13,253` hash=0 pseudonyms `0/13,253` present in distinct mp set; `9,986` hash!=0 all present. True rate `9,986/23,239 = 43%`. `max(last_seen_at)=2026-06-13`.

**C14 / C20 (sql + code, `fct_mixpanel_ga_gpay_crossdomain_daily`)** — L49-50 `max(if(da.gp_safe IS NOT NULL,1,0))` over String LEFT JOIN, no hook. Full 122-day history (`2026-02-11..2026-06-20`): `sum(matched_users_any)=17`, `sum(users_with_delay_activity_7d)=0`, `sum(users_with_allowance_changes_30d)=1`; `0` days where flags `>` or `=` matched (matched>1). `getSetting('join_use_nulls')=false`; synthetic String LEFT JOIN → unmatched flag `1`. `delay_active_safes_7d` CTE non-empty (`418` safes); upstream `fct_mixpanel_ga_gpay_users` collapsed to ~2 rows.

**C15 / C21 (sql, `fct_mixpanel_ga_overview_daily` vs stg)** — `int_mixpanel_ga_events_daily` computes `uniqExact(device_id_hash)` per `(date,event_name,event_category)`. Stored vs true `uniqExact(device_id_hash)` from `stg_mixpanel_ga__events WHERE is_production=1`: `2026-06-18` `17,595/2,151` (8.2x, +15,444); `2026-06-19` `15,499/2,295` (6.8x, +13,204); `2026-06-20` `14,583/2,079` (7.0x, +12,504). Per-device event-type fan-out (`count()/uniqExact(device)` over `(date,device,event_name)`) = `7.22/6.6/7.06/8.18/6.75/7.01` — matches inflation ratios. `api_mixpanel_ga_overview_daily.sql` = `SELECT *`; description "Distinct device_id_hash count".

**C16 (sql + code, `fct_mixpanel_ga_gnosis_app_daily`)** — L70 `sum(d.new_users) OVER (ORDER BY d.date)`, no `>=2025-11-12` filter. Upstream `int_execution_gnosis_app_user_events`: `count()=532,212`, `min(block_timestamp)` date `2025-11-12`, `countIf(<'2025-11-12')=0`. Fct: `min(date)=2025-11-12`, first `cumulative_users=852 = first new_users=852`, `max(cumulative_users)=24,020`.

**C17 (code-only, `semantic_models.yml`)** — `agg:max` on: gpay_acquisition `cumulative_accounts` L346, gnosis_app_acquisition `cumulative_accounts` L386, client_conversions `cumulative_users` L1013, gnosis_app_daily `cumulative_users` L1049, `cumulative_mp_matched` L1052. No cumulative measure remains `agg:sum`.

**C18 (sql + code, `fct_mixpanel_ga_funnel_daily`)** — L49 `welcome_visitors = uniqExactIf(user_id_hash, page_path='/welcome')`; L51 `passkey_logins = uniqExactIf(user_id_hash, event_name='Login with Passkey')`; L54-58 rate uncapped. `count()=122`, `countIf(onboarding_conversion_rate>1.0)=0`, `max=0.8705`; `swap>1.0=0`; `circles>1.0=0`. Lowest-`welcome_visitors` days (262/284/286/302/306) → rates `0.3473/0.3592/0.3252/0.2848/0.2908`.

## Review log (>=3 rounds per case)

- **C01**: R1 CONFIRMED (line order L4 vs L15) → challenge: prove compiled strategy resolves delete+insert despite `--vars` → R2 CONFIRMED (couldn't run dbt compile; provable by Jinja eval order) → challenge: pin blast radius of body branch L51-53 → R3 CONFIRMED, high (body emits whole-month delete; bug = dead append branch only, no data loss).
- **C02**: R1 CONFIRMED medium (no hook) → challenge: query `''` vs `'unknown'` buckets → R2 CHANGED low (`''`=0, conv⊆acquisition today) → challenge: is conv⊆acquisition guaranteed by construction? → R3 CONFIRMED low (guaranteed: conv adds INNER JOIN atop identical filter).
- **C03**: R1 CONFIRMED medium → challenge: registry treatment, is gap neutralized by blanket block? → R2 CONFIRMED (no `quality_tier:blocked` exists; registered candidate) → challenge: does MCP filter candidate tier? → R3 CONFIRMED medium (only `expose_to_mcp:false` gates; candidate is served).
- **C04**: R1 CONFIRMED medium → challenge: demonstrate over-count, which op breaks → R2 CONFIRMED (3.2-4.0x fan-out; sum is additive, row-cardinality breaks) → challenge: any measure mis-aggregating? → R3 CONFIRMED low (no measure mis-aggregates; raw-table footgun).
- **C05**: R1 CONFIRMED medium (13 vs 8) → challenge: are the 5 cols consumed downstream? → R2 CONFIRMED low (no consumer reads them) → challenge: explicit list vs `SELECT *`? → R3 CONFIRMED low (explicit list reads all 5 correctly; pure doc gap).
- **C06**: R1 CONFIRMED medium → challenge: confirm zero tests + live downstream → R2 CHANGED low (no tests; now semantic-registered) → challenge: prove ref chain + materialized → R3 CONFIRMED low (feeds materialized `589`-row mart, zero tests).
- **C07**: R1 CHANGED low (schema added; api tags missing) → challenge: run guard, confirm peer convention → R2 CHANGED low (guard skips no-`api:` models) → challenge: confirm daily api_ views carry tags → R3 CHANGED low (daily views carry `granularity:daily`/tier; weekly are outliers).
- **C08**: R1 CONFIRMED medium → challenge: registry treatment of fct tables → R2 CONFIRMED (approved tier, user_pseudonym entity, no blocked tier) → challenge: are approved per-user models MCP-served? → R3 CONFIRMED medium (served; no grain gate; only `expose_to_mcp:false` blocks).
- **C09**: R1 CONFIRMED medium → challenge: are tables on daily refresh? → R2 CHANGED low (production tag + daily cron; crossdomain fresh) → challenge: prove gpay_users (user-grain) also fresh → R3 CONFIRMED low (rebuilt same cron; residual = missing caveat).
- **C10**: R1 CONFIRMED low → challenge: events in data absent from seed? → R2 CONFIRMED low (`'Mint'` outside seed) → challenge: is `'Mint'` a rename superseding the seed? → R3 CONFIRMED low (co-occurs ~96%, not a supersession; coverage-latency note).
- **C11**: R1 CONFIRMED low → challenge: probe duplicates on a grain → R2 CONFIRMED low (daily/user grains clean) → challenge: probe a weekly composite grain → R3 CONFIRMED low (`589=589`, 0 dupes).
- **C12**: R1 CONFIRMED low → challenge: does a uniqueness test exist that could flake? → R2 CHANGED low (no test; framing moot, restated as dedup-correctness) → challenge: do collisions exist? → R3 CONFIRMED low (0 collisions / 5.77M rows).
- **C13**: R1 CONFIRMED critical (`matched_mp=1` everywhere) → challenge: prove false-positives are join defaults not legit hash=0 → R2 CONFIRMED (13,253+9,986 partition exactly) → challenge: cross-check against mp_users set → R3 CONFIRMED critical (13,253 genuinely absent from mp set).
- **C14**: R1 CHANGED low (no inflation in data, N=2-3, unexplained) → challenge (insufficient): run mp_user_flags probe + `getSetting` → R2 CHANGED medium (mechanism alive: 418 safes, join_use_nulls=false, synthetic flag=1; masked by collapse) → challenge: forward-looking re-inflation proof → R3 CONFIRMED medium (latent-armed).
- **C15**: R1 CONFIRMED high (7-8x) → challenge: confirm reaches API unchanged → R2 CONFIRMED high (`SELECT *`, desc wrong) → challenge: confirm correct value computable + isolate vs systemic → R3 CONFIRMED high (computable from stg; isolated to unique_devices).
- **C16**: R1 CONFIRMED medium (no filter; data clean) → challenge: any pre-baseline rows upstream? → R2 RESOLVED low (`countIf(<2025-11-12)=0`) → challenge: does window seed at baseline? → R3 RESOLVED low (first cumulative=852=first new_users).
- **C17**: R1 RESOLVED (agg:max) → challenge: any cumulative measure still agg:sum? → R2 RESOLVED (all 5 agg:max) → R3 RESOLVED (re-measured, confirmed).
- **C18**: R1 RESOLVED low (0/122 >1.0) → challenge: probe worst-case low-traffic day → R2 CONFIRMED low (worst 0.36, code uncapped) → challenge: settle R1/R2 via structural independence → R3 CONFIRMED low (independent uniqExactIf predicates; >1.0 reachable).
- **C19**: R1 CONFIRMED critical → challenge: current data-state not stale artifact? → R2 CONFIRMED critical (`max(last_seen_at)=2026-06-13`, recent) → challenge: consumer reachability → R3 CONFIRMED critical (matched_mp dimension + agg:sum measure MCP-served).
- **C20**: R1 RESOLVED (flags 0-1, not inflated) → challenge (insufficient): mechanism probe; tiny N → R2 CHANGED medium (correct-by-coincidence; mechanism alive) → challenge: retract historical claim + size latent → R3 CHANGED low (0 inflated rows / 122 days; latent risk carried on C14).
- **C21**: R1 CONFIRMED high (7-8x) → challenge: is the ratio stable not transient? → R2 CONFIRMED high (6.6-8.2x stable, = per-device fan-out) → challenge: re-measure → R3 CONFIRMED high (~7x on latest 3 days).

## Refreshed recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| P0 (ESCALATE) | Add `pre_hook/post_hook SET join_use_nulls=1` (or rewrite the match flag to test membership, not `IS NOT NULL`) and REBUILD; `matched_mp=1` for 100% of rows masks the true `43%` match rate on an MCP-served diagnostic | `fct_mixpanel_ga_gnosis_app_users.sql` (C13/C19) |
| P1 (KEEP) | Fix `unique_devices` to `uniqExact(device_id_hash)` per date re-aggregated from staging (not `sum()` of per-group uniqExact); ~7x over-count flows to the public overview API; correct the `schema.yml` "Distinct device_id_hash count" description | `fct_mixpanel_ga_overview_daily.sql`, `api_mixpanel_ga_overview_daily.sql` (C15/C21) |
| P1 (KEEP) | Move `{% set start_month/end_month %}` ABOVE the `config()` block so the append strategy can be selected; current code silently forces `delete+insert` (no data loss, but the append path is dead and the config is misleading) | `int_execution_gnosis_app_events_mixpanel_unified.sql` (C01) |
| P2 (KEEP) | Apply the same `join_use_nulls=1` fix to the crossdomain activity flags before the GP/Safe match universe repopulates (latent-armed; will re-inflate `users_with_delay_activity_7d`/`users_with_allowance_changes_30d`) | `fct_mixpanel_ga_gpay_crossdomain_daily.sql` (C14/C20) |
| P2 (KEEP) | Add `expose_to_mcp:false` to the per-user/row-level pseudonym models; the `dbt_project.yml` `quality_tier:blocked` gate does not exist, so these are live MCP exposures | `int_mixpanel_ga_user_lifecycle.sql`, `int_mixpanel_ga_users_daily.sql`, `int_execution_gnosis_app_events_mixpanel_unified.sql`, `fct_mixpanel_ga_gpay_users.sql`, `fct_mixpanel_ga_gnosis_app_users.sql` (C03/C08) |
| P3 (KEEP) | Add `schema.yml` entries + `unique`/`unique_combination_of_columns` tests on mart grains; document the 5 missing unified columns; add `granularity:weekly`/tier tags to the 4 weekly `api_` views | `marts/schema.yml`, `intermediate/schema.yml`, 4 `api_*_weekly.sql` (C05/C06/C07/C11) |
| P3 (KEEP) | Add a `least(...,1)` cap (or schema caveat) on `onboarding_conversion_rate`; add a `today()`-window staleness caveat; add a row-count/accepted_values test on the conversion seed | `fct_mixpanel_ga_funnel_daily.sql`, `fct_mixpanel_ga_gpay_users.sql`, `seeds/mixpanel_conversion_events.csv` (C18/C09/C10) |
| P4 (KEEP) | Consider a version column on `stg_mixpanel_ga__events` `ReplacingMergeTree` (0 collisions today, latent); drop `initial_referrer_domain` from the traffic_daily grain or document the fan-out | `stg_mixpanel_ga__events.sql`, `int_mixpanel_ga_traffic_daily.sql` (C12/C04) |
| — (DROP) | No action needed — `agg:sum`→`agg:max` already applied to all cumulative measures | `semantic_models.yml` (C17, RESOLVED) |
| — (DROP) | No action needed as data matter — cumulative running total seeds correctly at the `2025-11-12` baseline (keep the missing hard filter as a low latent footgun if backfilling) | `fct_mixpanel_ga_gnosis_app_daily.sql` (C16, RESOLVED) |
