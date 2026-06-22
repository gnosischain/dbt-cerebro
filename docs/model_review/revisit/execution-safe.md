# Model review (revisit 2026-06-21): execution/safe

Baseline `docs/model_review/execution-safe.md` (dated 2026-06-11); 16 cases re-verified over 3 rounds. Headline: 2 resolved (`C04`, `C15`), 4 changed (`C01` root-cause re-attributed, `C05`/`C06`/`C11` severities lowered), and 10 still confirmed — the two unresolved criticals (`C01` v1.4.1 owner-drop, `C02` 8-dup fan-out into a served API view) remain the remediation priorities.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONSAFE-C01 | — | v1.4.1/v1.4.1L2 AddedOwner/RemovedOwner decode to NULL owner (~107k events drop) | critical | CHANGED | critical | high | none | 3 |
| EXECUTIONSAFE-C02 | — | `int_execution_safes` RMT without FINAL: 8 dup `safe_address`, fans out to marts | critical | CONFIRMED | critical | high | none | 3 |
| EXECUTIONSAFE-C03 | — | `int_execution_safes_module_events_v2` dead artifact still production-tagged | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONSAFE-C04 | — | `contracts_safe_registry` RMT/no-partition/no-FINAL can fan out decode | high | RESOLVED | low | high | none | 3 |
| EXECUTIONSAFE-C05 | — | 16.7% NULL `current_threshold` from pre-v1.1.0 floor + fixable v1.4.1 cohort | high | CHANGED | medium | high | none | 3 |
| EXECUTIONSAFE-C06 | — | API marts missing `data_type` + `window:` tags, CI-enforced | medium | CHANGED | low | high | none | 3 |
| EXECUTIONSAFE-C07 | — | `owner_events` `append` + `unique_key` gives no query-time dedup | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONSAFE-C08 | — | No grain/uniqueness tests on pseudonym bridge or latest mart | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONSAFE-C09 | — | `join_use_nulls` not set on LEFT JOINs, may mask genuine NULLs | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONSAFE-C10 | — | `current_owners` heavy 1-thread/2GB caps vs 'cheap to rebuild' note | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONSAFE-C11 | — | Singleton-upgrade blind spot: `abi_source_address` frozen at creation | high | CHANGED | low | medium | none | 3 |
| EXECUTIONSAFE-C12 | — | Pseudonym bridge `mixpanel`-tagged, no `privacy_tier`/`expose_to_mcp` | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONSAFE-C13 | — | `int_execution_safes` `authoritative:false` on foundational catalog | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONSAFE-C14 | — | All three API views `tier2` despite widely-depended upstream | low | CONFIRMED | low | medium | none | 3 |
| EXECUTIONSAFE-C15 | — | `int_execution_safes` ~3-day data lag | low | RESOLVED | resolved | high | none | 3 |
| EXECUTIONSAFE-C16 | — | Owner-events grain test passes only because `block_timestamp` is keyed | low | CONFIRMED | low | high | none | 3 |

## Delta vs baseline

### RESOLVED (2)
- **C04** (`contracts_safe_registry.sql`) — high -> low. The registry shows **zero** propagated dups: `count()=678,621 = uniqExact(address)=678,621`. Two durable independent guards: the model is `materialized='table'` (full DROP/CREATE each run, so unmerged dups can't accumulate), and `macros/decoding/decode_logs.sql` uses `ANY LEFT JOIN` (lines `261`, `557`) which caps to <=1 ABI row per proxy. The structural RMT/no-partition/no-FINAL risk is real in code but not realizable. No incident.
- **C15** (`int_execution_safes.sql`) — lag closed. `max(block_timestamp)` moved from `2026-06-08T21:33:15` (~3-day lag) to `2026-06-21T07:31:10` (~0-day lag), with 8 contiguous daily partitions each spanning `00:xx`-`23:xx`. Steady-state catch-up via normal scheduling, **not** an incident wipe (table holds full multi-year history, `678,629` rows). No incident.

### CHANGED (4)
- **C01** (`seeds/event_signatures.csv`, `models/execution/safe/intermediate/int_execution_safes_owner_events.sql`) — stays critical, root cause re-attributed. Data symptom persists and grew: `~107k` estimated -> `115,731` measured NULL owner-delta rows on v1.4.1+v1.4.1L2 (100% of the cohort) vs `0.02%` on same-ABI v1.3.0. The baseline's "seed `indexed:false` should be `true`" theory is **disproven** — `contract_decode_receipt_logs` decodes the same logs correctly using that flag. The NULL is born at the inline `decode_logs()` CTE: for a real v1.4.1L2 AddedOwner (tx `ed5fab15b328956cd8457461f77d2619097ef05b2735ff8762cc413fa7245a33`, topic0 `9465fa0c...`) the owner address is on-chain in `topic1` (`...4072ffa6b5b29076172ee36d3e18fda225139635`) with `data` NULL / `n_data_bytes=0`; because the seed flags owner `indexed:false`, `decode_logs` reads owner from the empty data slot. No incident.
- **C05** (`int_execution_safes_current_owners.sql`) — high -> medium. NULL `current_threshold` reproduced at `127,748/772,693 = 16.53%`, but the baseline's causal split is wrong: by `creation_version`, `1.1.1Circles=124,048` (97.2%), `1.2.0=3,542`, `1.1.1=126`, `1.3.0=28`, `1.0.0=5`, and **v1.4.1/v1.4.1L2 = 0**. Of 1,986 sampled `1.1.1Circles` NULL-threshold safes, **0** have any `safe_setup`/`changed_threshold` event — an expected floor (the Circles fork emits no decodable threshold), not the fixable v1.4.1 cohort the baseline tied to C01. No incident.
- **C06** (`models/execution/safe/marts/schema.yml`) — medium -> low. The `columns_untyped` violation is real (only `as_of_date` is typed) but `scripts/checks/check_api_tags.py` has **no `window:` rule at all**, and all three endpoints are allowlisted in `check_api_tags.allow` (`api_execution_account_safes_latest::columns_untyped` line `24`, `api_execution_safe_details_latest::columns_untyped` line `118`, `api_execution_safes_current_owners::columns_untyped` line `119`). CI is green -> allowlist debt, not an active gate failure. No incident.
- **C11** (`contracts_safe_registry.sql`, owner/module events) — high -> low. Design defect confirmed (`abi_source_address` frozen at creation singleton; **no** `ChangedMasterCopy` signature in `seeds/event_signatures.csv`; `decode_logs` ANY-joins ABI on the frozen address), but every scannable window for `changeMasterCopy` (selector `0x7de7edef`) / `ChangedMasterCopy` (topic0 `0x75e41bc...d0b8`) returns **0** calls — 2021, 2022, Jan-2025, June-2026 all 0. The baseline `~5,000`-upgraded-Safe magnitude is refuted; realized blast radius is ~0 in measured data. Confidence medium: a single full-history aggregate could not be obtained (the `substring(action_input,1,10)` predicate cannot prune partitions; aggregates exceed the 30s MCP cap), so a small residual in unscanned high-density quarters cannot be fully excluded. No incident.

### STILL CONFIRMED (10)
- **C02** (`int_execution_safes.sql`, `api_execution_safe_details_latest.sql`, `fct_execution_account_safes_latest.sql`) — critical. Source `count()=678,629` vs `uniqExact(safe_address)=678,621` = 8 dups (baseline 8). The no-FINAL view `api_execution_safe_details_latest` inherits the inflation (`678,629` vs `678,621`; dup safe `0xaf94a1179403645ec79b104775408e5d710fe735` returns 2 rows), and the two served rows **disagree** on `deployment_timestamp` (`2025-11-26T22:15:15` vs `2026-02-05T18:38:55`) and `deployment_tx_hash` — a non-deterministic served value, not cosmetic. The `fct_execution_account_safes_latest` table is merge-collapsed clean (`772,693=772,693`).
- **C03** (`int_execution_safes_module_events_v2.sql`, `intermediate/schema.yml`) — high. `v2`: `645` rows frozen at `2022-08-31T20:30:55`; live original: `563,614` rows fresh to `2026-06-21T07:31:10`. Zero `ref()` in models/semantic/exposures; the relation is materialized/queryable and still carries `tags=['production','execution','safe','microbatch']`. Safe-to-drop dead artifact in the production build.
- **C07** (`int_execution_safes_owner_events.sql`) — medium. `incremental_strategy='append'` (line 4) coexists with `unique_key='(transaction_hash, log_index, owner)'` (line 8); dbt-clickhouse ignores `unique_key` under append. Grain holds only via RMT background merge (`count()=1,256,748=uniqExact((transaction_hash, log_index, owner))`). Risk stays low in steady state because the incremental filter is forward-only (no lookback overlap).
- **C08** (`fct_execution_safe_owner_pseudonyms.sql`, `marts/schema.yml`) — medium. `tests:[]` on the bridge, only `not_null` on the latest mart. Both grains currently clean (`772,693=772,693` each). Coverage gap only — the address->pseudonym map is deterministic 1:1 by salted hash, so no realizable collision today.
- **C09** (`fct_execution_account_safes_latest.sql`, `api_execution_safe_details_latest.sql`) — low. Neither model sets `join_use_nulls`; the fct `pre_hook` sets `grace_hash`/`max_bytes_in_join` but not `join_use_nulls`, and the api view has no hook. Practical masking is nil: `current_threshold` is `Nullable(UInt32)` upstream and `current_owner_count` is `COALESCE(...,0)` in the view, so no consumed unmatched column is a bare non-nullable numeric.
- **C10** (`int_execution_safes_current_owners.sql`) — low. `max_threads=1`, `max_memory_usage=2000000000` (2GB), `max_bytes_before_external_group_by=20000000` (20MB) coexist with the `schema.yml` "Cheap to fully rebuild every run" note and are tighter than the upstream owner-events model's 4GB. `git log -L` dates the block to commit `e769ae78` (2026-05-14, subject "semantic and tests") with no OOM/incident reference — defensive boilerplate, safe to relax.
- **C12** (`fct_execution_safe_owner_pseudonyms.sql`, `marts/schema.yml`, semantic models) — medium. `tags=['production','execution','safe','mixpanel']`, no `privacy_tier`/`expose_to_mcp`, vs sibling `int_execution_gpay_user_identity_bridge.sql` `meta={'expose_to_mcp':False,'privacy_tier':'internal'}`. The semantic entry (`semantic/authoring/execution/safe/semantic_models.yml`) is also ungated (`config.meta.cerebro` carries only `quality_tier: candidate`), so owner<->safe linkage is reachable by the MCP planner.
- **C13** (`int_execution_safes.sql` / schema.yml) — low. `authoritative:false` still set (`intermediate/schema.yml` line `81`). Cross-sector deps: `accounts=6`, `gnosis_app=1` (narrower than baseline's "every sector"). No CI check in `scripts/checks/` reads `node.meta.authoritative` — it is a latent/documentary flag, so the remediation (flip to true or wire a check) is cosmetic today.
- **C14** (`api_execution_*` configs, `marts/schema.yml`) — low. All three views carry `tier2`. `check_api_tags.py` `TIER_RE=^tier\d+$` requires only that *some* tier tag is present and makes no exposure distinction between tier levels; peer external api endpoints across gpay/gnosis_app use the same convention. tier2 is convention-compliant.
- **C16** (`int_execution_safes_owner_events.sql`, schema.yml) — low. `count()=1,256,748 = uniqExact((safe_address, transaction_hash, log_index)) = uniqExact((safe_address, block_timestamp, log_index))`; `(safe_address, event_kind, log_index)` collapses to `1,189,258` (gap `67,490`, benign cross-block `log_index` reuse: `added_owner=20,984`, `removed_owner=2,541`, `changed_threshold=453`, `safe_setup=0`). The schema test keys on `(safe_address, block_timestamp, log_index)` which stays unique — correctly specified, not accidentally passing.

### NEW (0)
None.

### UNVERIFIABLE / UNRESOLVED (0)
None left open. C11's full-history magnitude carries a residual open question (a single full-history aggregate exceeds the MCP query cap), but the case is settled at CHANGED/low on the consistent ~0 measured signal; an offline ClickHouse scan would close it definitively.

## Evidence appendix

### C01 — v1.4.1 owner-drop
```sql
SELECT s.creation_version v, count() n_delta,
       countIf(oe.owner IS NULL OR oe.owner='') null_owner
FROM dbt.int_execution_safes_owner_events oe
INNER JOIN dbt.int_execution_safes s ON s.safe_address=oe.safe_address
WHERE oe.event_kind IN ('added_owner','removed_owner') GROUP BY v;
```
Returned: `v1.4.1L2 108,419/108,419 NULL (100%)`, `v1.4.1 7,312/7,312 (100%)`, `v1.3.0 69/309,532 (0.02%)`, `v1.3.0L2 3,791/180,964 (2.1%)`, `v1.1.1Circles 15,642/140,109 (11%)`. Total v1.4.1+v1.4.1L2 NULL owner-delta = `115,731`. Raw log for tx `ed5fab15...` (v1.4.1L2 AddedOwner, topic0 `9465fa0c...`): owner in `topic1=...4072ffa6b5b29076172ee36d3e18fda225139635`, `data=NULL`, `n_data_bytes=0`. On-chain decode via `contract_decode_receipt_logs` resolves owner correctly with the same `indexed:false` ABI. `event_signatures.csv` lines `192-193` (v1.4.1L2) and `324-325` (v1.4.1) still carry `indexed:false` on the owner param.

### C02 — dup fan-out into served view
```sql
SELECT count(), uniqExact(safe_address) FROM dbt.int_execution_safes;
SELECT count(), uniqExact(safe_address) FROM dbt.api_execution_safe_details_latest;
SELECT safe_address,creation_version,current_owner_count,current_threshold,
       deployment_timestamp,deployment_tx_hash
FROM dbt.api_execution_safe_details_latest
WHERE safe_address='0xaf94a1179403645ec79b104775408e5d710fe735';
```
Returned: `int_execution_safes 678,629` vs `678,621` (8 dups); `api_execution_safe_details_latest 678,629` vs `678,621` (8 dups inherited). Dup safe both rows v1.4.1L2, `owner_count=1`, `threshold=1` (identical) but `deployment_timestamp` differs `2025-11-26T22:15:15` vs `2026-02-05T18:38:55` and `deployment_tx_hash` differs. `fct_execution_account_safes_latest` clean: `772,693=772,693`.

### C03 — dead v2 artifact
```sql
SELECT 'v2',count(),max(block_timestamp) FROM dbt.int_execution_safes_module_events_v2
UNION ALL SELECT 'orig',count(),max(block_timestamp) FROM dbt.int_execution_safes_module_events;
```
Returned: `v2: 645 rows, max 2022-08-31T20:30:55`; `orig: 563,614 rows, max 2026-06-21T07:31:10`. `grep -rn "int_execution_safes_module_events_v2"` across models/semantic/exposures returns only its own schema.yml definition (`intermediate/schema.yml` line `300`). Config `tags=['production','execution','safe','microbatch']`.

### C04 — registry dedup
```sql
SELECT count(), uniqExact(address) FROM dbt.contracts_safe_registry;
```
Returned: `678,621 = 678,621` (0 dups). `contracts_safe_registry.sql` config `materialized='table'`, `engine='ReplacingMergeTree()'`, `order_by='(address)'`, no `partition_by`. `decode_logs.sql` lines `261`, `557` use `ANY LEFT JOIN`.

### C05 — NULL threshold decomposition
```sql
SELECT s.creation_version v, count() n, countIf(co.current_threshold IS NULL) null_thr
FROM dbt.int_execution_safes_current_owners co
INNER JOIN dbt.int_execution_safes s ON s.safe_address=co.safe_address GROUP BY v;
```
Returned: total `127,748/772,693 = 16.53%` NULL. By version: `1.1.1Circles 124,048` (97.2% of nulls), `1.2.0 3,542`, `1.1.1 126`, `1.3.0 28`, `1.0.0 5`; `v1.4.1=0`, `v1.4.1L2=0`. Sample of 1,986 `1.1.1Circles` NULL-threshold safes: `0` with any `safe_setup`/`changed_threshold` event, `0` with a non-null threshold event.

### C06 — API tags / allowlist
Code read: `check_api_tags.py` enforces grain-free `api:` name, exactly one `granularity:`, a `tier{0|1|2}`, typed columns, granularity-aware freshness column — **no `window:` rule**. `check_api_tags.allow` lines `24`/`118`/`119` allowlist all three `::columns_untyped`. `marts/schema.yml`: only `as_of_date` typed; `became_owner_at`/`current_threshold`/`current_owner_count`/`creation_version`/`is_l2`/`deployment_timestamp`/`deployment_tx_hash` untyped.

### C07 — append + unique_key, grain
```sql
SELECT count() AS cnt,
       uniqExact((transaction_hash, log_index, owner)) AS uk
FROM dbt.int_execution_safes_owner_events;
```
Returned: `cnt=1,256,748 = uk=1,256,748`. Config: `incremental_strategy='append'` (line 4) + `unique_key='(transaction_hash, log_index, owner)'` (line 8). Model body has no outer WHERE on `block_timestamp`; `decode_logs()` owns the forward-only window via `incremental_column='block_timestamp'`, `start_blocktime='2020-05-21'`.

### C08 — grain tests
```sql
SELECT count(), uniqExact(safe_user_pseudonym,owner_user_pseudonym) FROM dbt.fct_execution_safe_owner_pseudonyms;
SELECT count(), uniqExact(owner_address,safe_address) FROM dbt.fct_execution_account_safes_latest;
```
Returned: both `772,693 = 772,693`. `marts/schema.yml`: `fct_execution_safe_owner_pseudonyms tests:[]` (line 40), no `unique_combination_of_columns`; `fct_execution_account_safes_latest` only `not_null` on `owner_address`/`safe_address`.

### C09 — join_use_nulls
Code read: `fct_execution_account_safes_latest.sql` `pre_hook` (lines 8-16) sets `max_threads`/`max_block_size`/`max_memory_usage`/`join_algorithm='grace_hash'`/`grace_hash_join_initial_buckets`/`max_bytes_in_join`/`max_bytes_before_external_sort` — **not** `join_use_nulls`. `api_execution_safe_details_latest.sql` (`materialized='view'`) has no pre/post hook. Consumed unmatched columns: `current_threshold` is `Nullable(UInt32)` upstream; `current_owner_count` is `COALESCE(o.current_owner_count,0)`.

### C10 — query_settings
Code read: `int_execution_safes_current_owners.sql` `query_settings` (lines 14-26): `max_threads='1'`, `max_memory_usage='2000000000'`, `max_bytes_before_external_group_by='20000000'`. `schema.yml` line `268`: "Cheap to fully rebuild every run; no incremental strategy needed." Upstream `int_execution_safes_owner_events` uses `max_memory_usage=4000000000`.
```sql
git log --date=short -L '/query_settings/,/}/:models/execution/safe/intermediate/int_execution_safes_current_owners.sql'
```
Returned: block introduced in commit `e769ae78` (2026-05-14, "semantic and tests"), no OOM reference.

### C11 — changeMasterCopy blast radius
```sql
SELECT count() FROM execution.traces
WHERE block_timestamp IN [window]
  AND lower(substring(action_input,1,10))='0x7de7edef';
```
Returned: `2022-06 = 0`, `2026-06-01..21 = 0`, plus round-2 `0` across 2021/2022/Q1-2025/2026. `seeds/event_signatures.csv` has no `ChangedMasterCopy` row for any GnosisSafe singleton. `contracts_safe_registry.abi_source_address = creation_singleton` (frozen); `decode_logs` ANY-joins ABI at lines `261`/`557`. Full-history single-pass aggregate exceeds the 30s MCP cap (no partition pruning on the substring predicate).

### C12 — privacy gating
Code read: `fct_execution_safe_owner_pseudonyms.sql` `tags=['production','execution','safe','mixpanel']`, no meta block. `int_execution_gpay_user_identity_bridge.sql` line 8 `meta={'expose_to_mcp':False,'privacy_tier':'internal'}`. `semantic/authoring/execution/safe/semantic_models.yml` entry `execution_safe_owner_pseudonyms`: `config.meta.cerebro` carries only `quality_tier: candidate`; `safe_user_pseudonym` primary entity, `owner_user_pseudonym` foreign entity.

### C13 — authoritative flag
Code read: `intermediate/schema.yml` meta for `int_execution_safes` (lines 79-84): `owner: analytics_team`, `authoritative: false`. `grep 'authoritative' scripts/checks/` returns only `migrate_api_tags.py:11` and `verify_migration.py:118` (both descriptive, neither gates on the flag). `ref('int_execution_safes')`: accounts=6, gnosis_app=1, safe sector=4.

### C14 — tier classification
Code read: `api_execution_safe_details_latest.sql` line 4, `api_execution_account_safes_latest.sql` line 4, `api_execution_safes_current_owners.sql` line 4 all carry `tier2`. `check_api_tags.py` `TIER_RE=^tier\d+$` (presence-only). Peer api endpoints (gpay/gnosis_app) under the same convention appear in the allowlist.

### C15 — freshness
```sql
SELECT toDate(block_timestamp) d,count() n,min(block_timestamp) mn,max(block_timestamp) mx
FROM dbt.int_execution_safes WHERE block_timestamp>=toDateTime('2026-06-14') GROUP BY d ORDER BY d;
```
Returned: contiguous `2026-06-14..2026-06-21`, each day spanning `00:xx`-`23:xx` (e.g. 06-20: `00:10:20`-`23:58:30`, 330 rows), `max(block_timestamp)=2026-06-21T07:31:10`, `678,629` rows total. No dbt source-freshness block or freshness test in `intermediate/schema.yml` (only trailing-7-day `not_null` and `unique_combination_of_columns`).

### C16 — grain test
```sql
SELECT count(),
       uniqExact(safe_address,transaction_hash,log_index),
       uniqExact(safe_address,block_timestamp,log_index),
       uniqExact(safe_address,event_kind,log_index)
FROM dbt.int_execution_safes_owner_events;
```
Returned: `count()=1,256,748`; `(safe,tx,log)=1,256,748`; `(safe,bts,log)=1,256,748`; `(safe,event_kind,log)=1,189,258` (gap `67,490`). `intermediate/schema.yml`: `dbt_utils.unique_combination_of_columns = [safe_address, block_timestamp, log_index]` (trailing-7-day where).

## Review log (>=3 rounds per case)

- **C01**: r1 CONFIRMED (symptom holds, root cause questioned) -> challenge: localize where the NULL is introduced -> r2 CHANGED (decoder reads owner correctly; NULL inside dbt decode_logs) -> challenge: show decoded_params['owner'] in immediate upstream -> r3 CONFIRMED-as-CHANGED (raw log proves owner in topic1, data empty; seed `indexed:false` makes decode_logs read empty slot). Final CHANGED/critical.
- **C02**: r1 CONFIRMED (8 source dups) -> challenge: prove downstream blast radius -> r2 CONFIRMED (api view fans out, fct clean) -> challenge: prove a served value is corrupted, not just row count -> r3 CONFIRMED (deployment_timestamp/tx diverge across the 2 rows). Final CONFIRMED/critical.
- **C03**: r1 CONFIRMED (645 rows frozen) -> challenge: confirm dead in lineage -> r2 CONFIRMED (0 refs via grep) -> challenge: confirm materialized + tagged at runtime -> r3 CONFIRMED (queryable relation, production tag). Final CONFIRMED/high.
- **C04**: r1 CONFIRMED (code defect) -> challenge: prove dup propagates into registry + decode path -> r2 CHANGED (0 dups, ANY LEFT JOIN) -> challenge: confirm dedup durable not fresh-build artifact -> r3 RESOLVED (table full-rebuild + ANY-join, two guards). Final RESOLVED/low.
- **C05**: r1 CONFIRMED (16.53% NULL) -> challenge: decompose by creation_version -> r2 CHANGED (97% Circles, v1.4.1=0) -> challenge: prove dominant cause mechanism -> r3 CHANGED (Circles sampled safes have no threshold event = expected floor). Final CHANGED/medium.
- **C06**: r1 CONFIRMED (untyped + no window:) -> challenge: prove it trips CI -> r2 CHANGED (no window: rule, all 3 allowlisted) -> challenge: prove CI passes by running the check -> r3 CONFIRMED-as-CHANGED (could not run check under read-only; verified via script + allowlist read, CI green). Final CHANGED/low.
- **C07**: r1 CONFIRMED (append + unique_key) -> challenge: check grain without FINAL -> r2 CONFIRMED (grain holds via merge) -> challenge: check incremental filter for lookback overlap -> r3 CONFIRMED (forward-only filter, low dup risk). Final CONFIRMED/medium.
- **C08**: r1 CONFIRMED (tests:[]) -> challenge: would a uniqueness test pass today -> r2 CONFIRMED (grains clean) -> challenge: check address->pseudonym churn vector -> r3 CONFIRMED (deterministic 1:1, pure coverage gap). Final CONFIRMED/medium.
- **C09**: r1 CONFIRMED (no join_use_nulls) -> challenge: demonstrate actual masking -> r2 CONFIRMED (code omission, masking not shown, confidence medium) -> challenge: settle masking for this schema -> r3 CONFIRMED (all consumed unmatched cols Nullable/COALESCE; impact nil). Final CONFIRMED/low.
- **C10**: r1 CONFIRMED (heavy caps vs note) -> challenge: date the settings via git -> r2 CONFIRMED (couldn't date, confidence medium) -> challenge: run git log -L on the block -> r3 CONFIRMED (commit e769ae78, no OOM ref, defensive boilerplate). Final CONFIRMED/low.
- **C11**: r1 CONFIRMED (design defect, magnitude not re-quantified) -> challenge: chunked full-history logs scan -> r2 CONFIRMED (events=0 in completed windows, full scan timed out, insufficient) -> challenge: chunked traces scan year-by-year -> r3 CHANGED (every scannable window=0, magnitude refuted; residual on unscanned quarters). Final CHANGED/low.
- **C12**: r1 CONFIRMED (mixpanel, no gating) -> challenge: anchor to a sibling identity model -> r2 CONFIRMED (gpay bridge sets expose_to_mcp:false/privacy_tier:internal; safe bridge exposed in semantic layer) -> challenge: confirm semantic entry is reachable/ungated -> r3 CONFIRMED (semantic entry ungated, only quality_tier). Final CONFIRMED/medium.
- **C13**: r1 CONFIRMED (authoritative:false) -> challenge: substantiate cross-sector deps -> r2 CONFIRMED (accounts 6 + gnosis_app 1) -> challenge: locate the check that consumes the flag -> r3 CONFIRMED (no CI check reads it; latent flag). Final CONFIRMED/low.
- **C14**: r1 CONFIRMED (all three tier2) -> [no r2 entry] -> challenge: ground tier2 in the taxonomy + peer comparison -> r3 CONFIRMED (tier rule presence-only, no exposure distinction; convention-compliant). Final CONFIRMED/low.
- **C15**: r1 RESOLVED (lag ~0) -> challenge: verify durable not one-off -> r2 RESOLVED (8 contiguous daily partitions) -> challenge: note absence of freshness test as a gap -> r3 RESOLVED (no freshness tripwire; recommend recency-test ticket). Final RESOLVED.
- **C16**: r1 CONFIRMED (tx/log grain unique, 67k gap) -> challenge: prove the gap is benign fan-out -> r2 CONFIRMED (collisions concentrated in cross-block log_index reuse, safe_setup=0) -> challenge: confirm test keys on a grain that stays unique -> r3 CONFIRMED (test on (safe,bts,log), correctly specified). Final CONFIRMED/low.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| critical (KEEP) | Fix `decode_logs` argument-mapping for v1.4.1/v1.4.1L2 `AddedOwner`/`RemovedOwner`: the owner is emitted in `topic1` on these singletons, so the seed's `indexed:false` makes the decoder read the empty data slot. Recovers ~`115,731` owner-delta rows now NULL. Verify against same-ABI v1.3.0 (0.02% NULL) as the correctness oracle. | `seeds/event_signatures.csv`, `models/execution/safe/intermediate/int_execution_safes_owner_events.sql`, `macros/decoding/decode_logs.sql` |
| critical (KEEP) | Eliminate the 8-dup fan-out so served deployment metadata is deterministic: dedup the source (argMax on latest deployment) or apply `FINAL`/argMax in the consumer view. The two served rows for dup safes disagree on `deployment_timestamp`/`deployment_tx_hash`. | `int_execution_safes.sql`, `api_execution_safe_details_latest.sql` |
| high (KEEP) | Drop the dead `v2` artifact (`645` rows frozen at `2022-08-31`, 0 refs, still production-tagged + materialized) and remove its `schema.yml` block. | `int_execution_safes_module_events_v2.sql`, `intermediate/schema.yml` |
| medium (KEEP) | Add `privacy_tier:'internal'` + `expose_to_mcp:False` to match the gpay/gnosis_app identity-bridge posture; gate the semantic entry too. | `fct_execution_safe_owner_pseudonyms.sql`, `marts/schema.yml`, `semantic/authoring/execution/safe/semantic_models.yml` |
| medium (RE-SCOPE) | Label the `current_threshold` NULL floor instead of "fixing" a propagation bug: 97% is the `1.1.1Circles` fork that emits no decodable threshold event (expected floor); v1.4.1 contributes 0. Distinguish "no threshold event" from "threshold unknown". | `int_execution_safes_current_owners.sql`, `fct_execution_account_safes_latest.sql` |
| medium (KEEP) | Add `unique_combination_of_columns` grain tests on the pseudonym bridge `(safe_user_pseudonym, owner_user_pseudonym)` and the latest mart `(owner_address, safe_address)` to give CI a tripwire. | `marts/schema.yml` |
| medium (KEEP) | Make the owner-events dedup query-time-safe: either switch off `append` to an enforced strategy or document that the grain depends on RMT merge + forward-only filter (currently no enforcement under `append`). | `int_execution_safes_owner_events.sql` |
| low (KEEP) | Clear the `columns_untyped` allowlist debt: add `data_type` to the API mart columns and remove the three allowlist entries (CI is green today but the debt is documented). | `marts/schema.yml`, `scripts/checks/check_api_tags.allow` |
| low (KEEP) | Add a `max(block_date)` recency/freshness test on `int_execution_safes` — data is fresh now (C15 resolved) but there is no automated tripwire if lag regresses. | `int_execution_safes.sql`, `intermediate/schema.yml` |
| low (KEEP) | Relax `int_execution_safes_current_owners` query_settings toward the upstream 4GB budget, or update the "cheap to rebuild" note — the 1-thread/2GB caps are defensive boilerplate from `e769ae78` with no OOM justification. | `int_execution_safes_current_owners.sql` |
| low (KEEP) | Resolve `authoritative:false` on the foundational catalog: either flip to `true` or wire a CI check that consumes the flag (currently no check reads it). | `int_execution_safes.sql` / `intermediate/schema.yml` |
| low (KEEP) | Add `join_use_nulls` pre/post hooks per project convention even though current impact is nil (all consumed unmatched columns are Nullable/COALESCE-guarded), to harden future LEFT JOINs. | `fct_execution_account_safes_latest.sql`, `api_execution_safe_details_latest.sql` |
| low (CLOSE — confirm-then-monitor) | C11 singleton-upgrade blind spot: design defect is real but realized blast radius is ~0 in all measured windows. Keep the design note; run an offline full-history ClickHouse `changeMasterCopy` scan to definitively exclude the residual before adding `changeMasterCopy` ABI-rebind handling. | `contracts_safe_registry.sql`, `seeds/event_signatures.csv` |
| low (DROP) | C04 — no action: registry shows 0 dups, full-rebuild + `ANY LEFT JOIN` make fan-out structurally impossible. Close as resolved. | `contracts_safe_registry.sql` |
| low (DROP) | C14 — no action: `tier2` is convention-compliant (tier rule is presence-only). Close. | `api_execution_*` configs |
| low (DROP) | C16 — no action: grain test correctly keyed on `(safe_address, block_timestamp, log_index)`; the 67k gap is benign cross-block `log_index` reuse. Close. | `int_execution_safes_owner_events.sql` |
| resolved (DROP) | C15 — freshness lag closed (`~3d -> ~0d`), full history intact, no incident. Close (paired with the low-priority recency-test ticket above). | `int_execution_safes.sql` |
