# Model review (revisit 2026-06-21): execution/zodiac

Baseline `docs/model_review/execution-zodiac.md` (dated 2026-06-11); `16` cases re-verified over `3` rounds. Headline: `1` resolved (3-day freshness lag closed to T-0), `3` changed (one escalated `low`->`medium` on a real registry coverage gap), `12` still confirmed, `0` new and `0` unverifiable.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONZODIAC-C01 | - | `is_erc1271_exploitable` byte-identical to `submodule_is_safe` | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONZODIAC-C02 | - | registry reads append+RMT proxies with no FINAL | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONZODIAC-C03 | - | no grain-uniqueness test on submodules_latest | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONZODIAC-C04 | - | pre-Nov-2023 modifier-event lookback gap, undocumented | medium | CONFIRMED | low | high | none | 3 |
| EXECUTIONZODIAC-C05 | - | `unique_key` declared with `append` (dead config) | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONZODIAC-C06 | - | 3-day freshness lag, no freshness test | low | RESOLVED | resolved | high | none (normal microbatch) | 3 |
| EXECUTIONZODIAC-C07 | - | unreachable `Unknown` multiIf arm (maintenance trap) | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONZODIAC-C08 | - | `nullIf` Map-default vs join_use_nulls convention | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONZODIAC-C09 | - | submodules_latest has zero downstream consumers | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONZODIAC-C10 | - | `is_gp` undocumented cross-unit dep on gpay/Dune | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONZODIAC-C11 | - | ~140K proxies outside 4-mastercopy registry, no alerting | low | CHANGED | medium | high | none | 3 |
| EXECUTIONZODIAC-C12 | - | proxies duplicate-free on `(proxy_address)` | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONZODIAC-C13 | - | registry composition `153,127` (Delay/Roles/Unknown 0) | low | CHANGED | low | high | none | 3 |
| EXECUTIONZODIAC-C14 | - | events duplicate-free on `(transaction_hash, log_index)` | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONZODIAC-C15 | - | `~0.8%` NULL `avatar_address` (small/expected) | low | CHANGED | low | high | none | 3 |
| EXECUTIONZODIAC-C16 | - | no modifier maps to >1 avatar (argMax dedup) | low | CONFIRMED | low | high | none | 3 |

## Delta vs baseline

### RESOLVED (1)
- **EXECUTIONZODIAC-C06** — 3-day freshness lag closed. Baseline `max(block_timestamp)` was `2026-06-08` vs today `2026-06-11`; both incrementals now reach `2026-06-21 07:56:40` (T-0). Incident attribution corrected to **none / normal microbatch catch-up** — these models are `incremental_strategy='append'` + `ReplacingMergeTree` and were **never** in the June `insert_overwrite` blast radius; the in-window logs-ingestion gap (`2026-05-30`, `2026-06-14`, per `docs/incidents/logs_ingestion_gap_2026.md`) left no hole in the zodiac series (`2026-06-14`: proxies `286` rows, events `59` rows, contiguous). Residual: still no freshness test in `schema.yml` (keeps a low note), but the measured lag is resolved.

### CHANGED (3)
- **EXECUTIONZODIAC-C11** — ESCALATED `low` -> `medium`. Uncovered remainder grew from `~137,287` (`290,414 - 153,127`) to `140,195` (`294,697 - 154,502`). The two largest uncovered mastercopies — `0x732b9e9f259fba6f65a1a012dc89c20872ffbd2f` (`69,928`) and `0x22d903fd45f441f51bcad198d14eba8a75ea1ef0` (`69,928`), `139,856` combined — were confirmed via `contract_explore` to be a **Roles** Modifier and a **Delay** Modifier respectively. These are vulnerable Delay/Roles variants that *belong* in `contracts_zodiac_modules_registry`. Both `first_seen 2026-06-03` with 100% of proxies in the last 90 days, so this is a live, actively-accumulating coverage gap on the exact module types the registry targets — not correctly-excluded non-Modifier proxies.
- **EXECUTIONZODIAC-C13** — descriptive numbers refreshed (organic head-growth, no defect). Registry `153,127` -> `154,502`; `DelayModule 86,066` -> `87,439`; `RolesModule 67,061` -> `67,063`; `Unknown 0` -> `0`. Growth carries recent `max(start_blocktime)` (Delay `2026-06-21`, Roles `2026-06-09`) with unchanged `min` era, i.e. forward growth at the head, not a historical re-decode.
- **EXECUTIONZODIAC-C15** — NULL fraction edged up, cause refined. `1,304/157,845` (`0.83%`) -> `1,476/159,151` (`0.93%`). NULLs are **concentrated** in the oldest/smallest Delay copy `0xd54895b1` (`1,330` = 90% of NULLs) — the baseline's "proportional spread" framing was corrected. The C04-lookback hypothesis for this concentration was tested and **refuted**: of the `1,330` NULL modifiers on `0xd54895b1`, `1,259` have NO Safe-side `enabled_module` row at all and `0` of the `71` with an enable are before `2023-11-01`. Cause is benign/structural (non-Safe avatars), so severity stays `low`, decoupled from C04.

### STILL CONFIRMED (12)
- **EXECUTIONZODIAC-C01** (high) — `is_erc1271_exploitable` and `submodule_is_safe` both still resolve to `toUInt8(s.address IS NOT NULL)` (lines 76-77); `0/159,151` rows differ. `schema.yml` line 243 documents the redundancy (`"Derived flag, equal to submodule_is_safe"`). All `116,807` flag=1 rows are vulnerable Delay/Roles types (0 outside scope), so the flag is structurally redundant but not analytically wrong within table scope. Harm is "misleading-but-unread" given C09's zero consumers — kept high as a named security flag with zero discriminating power.
- **EXECUTIONZODIAC-C02** (medium) — `contracts_zodiac_modules_registry.sql` still SELECTs the append+RMT proxies model with no `FINAL`/`argMax`/`GROUP BY`. Latent, not live: proxies `count(*)=uniqExact(proxy_address)=294,697` (all parts merged). `decode_logs` neutralizes the double-feed one layer down (IN-subquery semi-join at line 110, `ANY LEFT JOIN` at line 261), so this is a registry-layer maintainability/correctness defect, not a live double-decode path.
- **EXECUTIONZODIAC-C03** (medium) — submodules_latest still has only `elementary.schema_changes`, no `unique_combination_of_columns` on `(modifier_address, submodule_address)`, while both upstreams declare it. Grain clean now (`159,151 = uniqExact`); both fan-out vectors (`contracts_safe_registry.address`, `int_execution_gpay_wallets.address`) are dup-free in the warehouse (`count(*)-uniqExact=0`), so it is a missing-test-net, not a live bug.
- **EXECUTIONZODIAC-C04** (medium -> settled **low**) — events `min(block_timestamp)=2023-11-03` (hardcoded `start_blocktime='2023-11-01'`, SQL line 23) vs earliest proxy `2023-02-28`; an `~8-month` window precedes event decode. The verifier's correct no-0x-prefix raw-logs scan found only `5` undecoded `EnabledModule`/`DisabledModule` events from `5` modifiers (`2023-09-12` to `2023-10-26`). Orchestrator settled at **low** (tiny survivorship gap; R3 bump back to medium added no new dropped-event evidence). Gap still undocumented in `schema.yml`. Baseline's stale `2021-01-01` earliest-proxy was corrected to `2023-02-28`.
- **EXECUTIONZODIAC-C05** (low) — both models still pair `incremental_strategy='append'` with a declared `unique_key` (dead under append). On events the `unique_key=(transaction_hash, log_index)` diverges from the RMT `order_by=(modifier_address, block_timestamp, log_index)` that actually governs collapse. C14 proves zero actual dup leakage; harm is reader-confusion only.
- **EXECUTIONZODIAC-C07** (low) — WHERE master_copy set `{0x4a97e651, 0xd54895b1, 0xd62129bf, 0x9646fdad}` exactly equals the multiIf arm union, so the `Unknown` arm fires `0` times. Second smell: `0xd62129bf` has `0` deployed proxies (mapped-but-empty copy), so the live registry is effectively only 3 mastercopies.
- **EXECUTIONZODIAC-C08** (low) — `lower(nullIf(decoded_params['module'], ''))` at line 34 remains; this is the correct Map-default idiom (a missing key returns `''`, not a join null), not a `join_use_nulls` violation. But the events model lacks the `join_use_nulls` hook its own downstream `int_execution_zodiac_modifier_submodules_latest` uses (lines 8-9) — internal inconsistency/maintainability.
- **EXECUTIONZODIAC-C09** (low) — `get_downstream_impact` returns exactly `1` consumer (the elementary `schema_changes` test); `list_saved_queries` none; none of the `7` custom tools reference it; repo grep finds only its own `schema.yml`. `materialized='table'` (full rebuild each run), `159,151` rows computed and read by nothing — airtight across dbt graph and serving layer.
- **EXECUTIONZODIAC-C10** (low) — `is_gp` still derives from `gp_safes AS (SELECT address FROM ref('int_execution_gpay_wallets'))` (line 67), a downstream-of-Dune cross-unit dependency with no freshness SLA and undocumented in `schema.yml` (is_gp doc lines 246-248). Recently-onboarded GP Safes can be misclassified `is_gp=0`.
- **EXECUTIONZODIAC-C12** (low) — proxies still duplicate-free: `294,697 = uniqExact(proxy_address)`, and `count FINAL` collapses nothing (parts merged). CI-guarded by the lookback-windowed `dbt_utils.unique_combination_of_columns` on `proxy_address` (`schema.yml` lines 68-72) — contrasts with C03's untested grain.
- **EXECUTIONZODIAC-C14** (low) — events duplicate-free on `(transaction_hash, log_index)` AND on the divergent RMT order_by grain: `164,549 = uniqExact` on both, pre- and post-FINAL. Structurally guaranteed by `decode_logs.sql` `row_number() OVER (PARTITION BY block_number, transaction_index, log_index ...) WHERE _dedup_rn = 1` (lines 188-246), so the append+RMT order_by divergence cannot create duplicate emission.
- **EXECUTIONZODIAC-C16** (low) — `0` modifiers map to >1 distinct `avatar_address`. The underlying `argMax(safe_address, (block_number, log_index))` has `0` ties (block_number+log_index globally unique), so the single-avatar grain is genuinely enforced, not tautological.

### NEW (0)
None.

### UNVERIFIABLE / UNRESOLVED (0)
None.

## Evidence appendix

**C01** — `SELECT count(*), countIf(submodule_is_safe <> is_erc1271_exploitable), countIf(is_erc1271_exploitable=1), countIf(submodule_is_safe=1) FROM dbt.int_execution_zodiac_modifier_submodules_latest` -> `159,151` total, `0` differ, `116,807` flag=1 (both columns). All `116,807` flag=1 rows have `module_type IN ('DelayModule','RolesModule')` and `master_copy` in the 4 vulnerable copies, `0` outside. SQL lines 76-77 = identical `toUInt8(s.address IS NOT NULL)`; `schema.yml` line 243 doc.

**C02** — `SELECT count(*), (SELECT count(*) FROM dbt.int_execution_zodiac_module_proxies FINAL), uniqExact(proxy_address) FROM dbt.int_execution_zodiac_module_proxies` -> `294,697 = 294,697 = 294,697` (no live dup window). `decode_logs.sql` line 110 addr_filter `... IN (SELECT lower(...) FROM <ref> cw)` semi-join; line 261 `ANY LEFT JOIN`.

**C03** — `SELECT (SELECT count(*)-uniqExact(address) FROM dbt.contracts_safe_registry), (SELECT count(*)-uniqExact(address) FROM dbt.int_execution_gpay_wallets), (SELECT count(*)-uniqExact(address) FROM dbt.contracts_zodiac_modules_registry)` -> `0, 0, 0`. submodules_latest grain `159,151 = uniqExact((modifier_address, submodule_address))`. `schema.yml` block (lines 258-262) has only `elementary.schema_changes`.

**C04** — `SELECT min(block_timestamp) FROM dbt.int_execution_zodiac_module_proxies` -> `2023-02-28T12:32:55`; `SELECT min(block_timestamp) FROM dbt.int_execution_zodiac_modifier_module_events` -> `2023-11-03T09:57:55`. Raw scan of `execution.logs` (no-0x-prefix topic0) for registry-modifier `EnabledModule`/`DisabledModule` in `[2023-02-28, 2023-11-01)` -> `5` events / `5` distinct modifiers, `2023-09-12` to `2023-10-26`.

**C05** — code-only. `int_execution_zodiac_modifier_module_events.sql`: `incremental_strategy='append'` (line 4) + `unique_key='(transaction_hash, log_index)'` (line 8), RMT `order_by=(modifier_address, block_timestamp, log_index)` (line 6). `int_execution_zodiac_module_proxies.sql`: `append` (line 4) + `unique_key='(proxy_address)'` (line 8), `order_by=(proxy_address)` (line 6). Grep confirms only these two carry the pattern.

**C06** — `SELECT max(block_timestamp) FROM dbt.int_execution_zodiac_module_proxies` and `... modifier_module_events` -> both `2026-06-21T07:56:40` (T-0 vs today `2026-06-21`). Daily counts `2026-06-01..21` contiguous; `2026-06-14`: proxies `286`, events `59`. No freshness test in `schema.yml`.

**C07 / C13** — `SELECT contract_type, count(*) FROM dbt.contracts_zodiac_modules_registry GROUP BY contract_type` -> `DelayModule 87,439`, `RolesModule 67,063`, `Unknown 0` (total `154,502`). `SELECT lower(master_copy), count(*) FROM dbt.int_execution_zodiac_module_proxies GROUP BY lower(master_copy) ORDER BY count(*) DESC` -> `0xd62129bf` absent (0 proxies). WHERE set (lines 17-22) == multiIf union (Roles `{0x9646fdad}` + Delay `{0x4a97e651, 0xd54895b1, 0xd62129bf}`).

**C08** — code-only. `int_execution_zodiac_modifier_module_events.sql` line 34: `lower(nullIf(decoded_params['module'], ''))`; pre_hook (line 11) sets only `allow_experimental_json_type=1` + `join_algorithm`, no `SET join_use_nulls=1`. `int_execution_zodiac_modifier_submodules_latest.sql` lines 8-9 DO use `join_use_nulls` hooks. `decode_logs.sql` builds `decoded_params` via `mapFromArrays(...)` (line 551).

**C09** — `get_downstream_impact` -> 1 consumer (`elementary_schema_changes` test). `list_saved_queries` -> none. `list_custom_tools` -> 7 tools, none reference the model. `SELECT count(*) FROM dbt.int_execution_zodiac_modifier_submodules_latest` -> `159,151`. `materialized='table'` (SQL line 3).

**C10** — code-only. `int_execution_zodiac_modifier_submodules_latest.sql` line 67 `gp_safes AS (SELECT address FROM ref('int_execution_gpay_wallets'))`; line 84 LEFT JOIN; line 78 `is_gp = toUInt8(gp.address IS NOT NULL)`. `schema.yml` lines 246-248 omit cross-unit/freshness caveat. `get_upstream_lineage`: 12 ancestors, traces to Dune `stg_gpay__wallets`.

**C11** — `SELECT lower(master_copy), count(*) FROM dbt.int_execution_zodiac_module_proxies GROUP BY lower(master_copy) ORDER BY count(*) DESC LIMIT 12` -> top uncovered `0x732b9e9f...` (`69,928`) and `0x22d903fd...` (`69,928`), both `first_seen 2026-06-03`, 100% last-90d. `contract_explore`: `0x732b9e9f` = `Roles` (assignRoles/scopeFunction/scopeTarget/RolesModSetup/EnabledModule); `0x22d903fd` = `Delay` (executeNextTx/txCooldown/txExpiration/TransactionAdded/EnabledModule). proxies `294,697` - registry `154,502` = `140,195` uncovered (`139,856` from these two copies).

**C12** — `SELECT count(*), (SELECT count(*) FROM dbt.int_execution_zodiac_module_proxies FINAL), (SELECT uniqExact(proxy_address) FROM ... FINAL) FROM dbt.int_execution_zodiac_module_proxies` -> `294,697 = 294,697 = 294,697`. `schema.yml` lines 68-72 declare `dbt_utils.unique_combination_of_columns` on `[proxy_address]` (lookback-windowed).

**C14** — `SELECT count(*), uniqExact((transaction_hash,log_index)), uniqExact((modifier_address,block_timestamp,log_index)) FROM dbt.int_execution_zodiac_modifier_module_events` -> `164,549 = 164,549 = 164,549` (also under FINAL). `decode_logs.sql` lines 191-246: `row_number() OVER (PARTITION BY block_number, transaction_index, log_index ORDER BY insert_version DESC) AS _dedup_rn ... WHERE _dedup_rn = 1`.

**C15** — `SELECT count(*), countIf(avatar_address IS NULL) FROM dbt.int_execution_zodiac_modifier_submodules_latest` -> `159,151` / `1,476` (`0.93%`). NULL by master_copy: `0xd54895b1=1,330`, `0x9646fdad=94`, `0x4a97e651=52`, `0xd62129bf=0`. Of the `1,330` d548 NULL modifiers joined to `int_execution_safes_module_events`: `1,259` with NO enable, `0` enable-before-`2023-11-01`, `71` enable-after.

**C16** — `SELECT countIf(n_avatars>1) FROM (SELECT modifier_address, uniqExact(avatar_address) n_avatars FROM dbt.int_execution_zodiac_modifier_submodules_latest WHERE avatar_address IS NOT NULL GROUP BY modifier_address)` -> `0`. safe_modifier_state argMax tie test -> `0` tied modifiers of `296,820`.

## Review log (>=3 rounds per case)

- **C01**: R1 CONFIRMED high (0/159,151 differ) -> challenge: does true exploitability need a precondition beyond submodule-is-Safe? -> R2 CONFIRMED high (all 116,807 safe rows are vulnerable-type, 0 outside; redundant but not analytically wrong) -> challenge: any consumer treating the flag as distinct? -> R3 CONFIRMED high (only consumer is schema_changes test; misleading-but-unread).
- **C02**: R1 CONFIRMED medium (no FINAL on registry read) -> challenge: prove latent risk live (FINAL delta, system.parts) -> R2 CONFIRMED medium (count=FINAL=uniqExact, no live window; system.parts unverifiable via MCP) -> challenge: does decode_logs DISTINCT the registry addresses? -> R3 CONFIRMED medium (decode_logs semi-join neutralizes double-feed; defect remains registry-layer).
- **C03**: R1 CONFIRMED medium (no grain test) -> challenge: can the model actually fan out? -> R2 CONFIRMED medium (safes/gp_safes join keys uniqueness asserted only in own units) -> challenge: are those keys unique now? -> R3 CONFIRMED medium (both dup-free; missing-test-net not live bug).
- **C04**: R1 CONFIRMED medium (events start 2023-11-01) -> challenge: prove dropped events exist; fix stale 2021-01-01 -> R2 CONFIRMED low (5 dropped events, earliest proxy 2023-02-28) -> R3 verifier medium, orchestrator settled **low** (no new dropped-event evidence; R2 better-calibrated).
- **C05**: R1 CONFIRMED low (append+unique_key) -> challenge: confirm dead, note order_by divergence -> R2 CONFIRMED low (doubly misleading on events) -> challenge: cross-link C14, grep for repeats -> R3 CONFIRMED low (zero dup leakage per C14; only these two models).
- **C06**: R1 RESOLVED (lag 0, attributed insert_overwrite) -> challenge: re-attribute, models are append+RMT -> R2 RESOLVED (attribution corrected to none/normal-microbatch; no hole on incident dates) -> R3 RESOLVED (T-0 confirmed; verifier reverted attribution to insert_overwrite which is WRONG but does not affect RESOLVED status).
- **C07**: R1 CONFIRMED low (Unknown=0) -> challenge: prove set-equality WHERE vs multiIf -> R2 CONFIRMED low (sets identical) -> challenge: does 0xd62129bf have proxies? -> R3 CONFIRMED low (0xd62129bf = 0 proxies, mapped-but-empty; second smell).
- **C08**: R1 CONFIRMED low (nullIf workaround) -> challenge: is it a join-null or Map-default? -> R2 CHANGED low (correct Map-default idiom, not convention violation) -> challenge: re-measure -> R3 CONFIRMED low (still lacks join_use_nulls hook its own downstream uses; internal inconsistency).
- **C09**: R1 CONFIRMED low (0 ref consumers) -> challenge: rule out off-graph consumers -> R2 CONFIRMED low (grep only own schema.yml; table-materialized) -> challenge: use get_downstream_impact/saved_queries/custom_tools -> R3 CONFIRMED low (1 consumer = schema_changes test; airtight).
- **C10**: R1 CONFIRMED low (is_gp joins gpay_wallets) -> challenge: baseline Dune-label lineage stale, trace real upstreams -> R2 CONFIRMED low (provenance corrected; cross-unit dep real, undocumented) -> R3 CONFIRMED low (lineage traces to Dune stg_gpay__wallets; no freshness SLA).
- **C11**: R1 CONFIRMED low (140,195 uncovered) -> challenge: conflates registry-drop vs platform-drop; classify the uncovered mastercopies -> R2 CONFIRMED low (two new mastercopies 69,928 each, first_seen 2026-06-03, actively accumulating) -> challenge: are they Delay/Roles variants? -> R3 **CHANGED low->medium** (contract_explore confirms both ARE Delay/Roles Modifiers; genuine coverage gap).
- **C12**: R1 CONFIRMED low (294,697=uniqExact) -> challenge: re-run WITH FINAL -> R2 CONFIRMED low (FINAL collapses nothing; cross-checks C02) -> challenge: is grain CI-guarded? -> R3 CONFIRMED low (unique_combination_of_columns test exists, lookback-scoped).
- **C13**: R1 CHANGED low (153,127 -> 154,502) -> challenge: organic growth vs recompute? -> R2 CHANGED low (recent max start_blocktime, unchanged min era = head-growth) -> R3 CHANGED low (numbers refreshed, structure unchanged).
- **C14**: R1 CONFIRMED low (0 dups on both grains) -> challenge: re-run WITH FINAL on both grains (order_by diverges) -> R2 CONFIRMED low (dup-free pre/post FINAL) -> challenge: quote the upstream dedup clause -> R3 CONFIRMED low (decode_logs row_number PARTITION BY structurally guarantees it).
- **C15**: R1 CONFIRMED low (1,476/159,151 NULL) -> challenge: confirm cause + check concentration -> R2 CHANGED low (concentrated in 0xd54895b1 = 90% of NULLs, NOT proportional) -> challenge: is the concentration a C04 pre-cutoff signature? -> R3 CHANGED low (refuted; 1,259/1,330 have no Safe enable at all, benign non-Safe avatars).
- **C16**: R1 CONFIRMED low (0 multi-avatar) -> challenge: test the layer below for argMax ties -> R2 CONFIRMED low (0 ties in safe_modifier_state) -> R3 CONFIRMED low (re-measured clean; argMax dedup genuine).

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P1 (ESCALATE) | Make the registry mastercopy filter data-driven or add new-mastercopy alerting: two confirmed Delay/Roles Modifier mastercopies (`0x732b9e9f...`, `0x22d903fd...`, `139,856` proxies, ~47% of all proxies) deployed `2026-06-03` are silently excluded from the vulnerable-Modifier registry. | `models/execution/zodiac/intermediate/contracts_zodiac_modules_registry.sql`, `int_execution_zodiac_module_proxies.sql` |
| P1 (KEEP) | Either drop `is_erc1271_exploitable` or give it real preconditions — it is byte-identical to `submodule_is_safe` (`0/159,151` differ), a named security flag with zero discriminating power. | `int_execution_zodiac_modifier_submodules_latest.sql`, `schema.yml` |
| P2 (KEEP) | Add `FINAL`/`argMax`/`GROUP BY` dedup on the proxies read in the registry, OR document that `decode_logs`' IN-subquery semi-join neutralizes the merge-timing risk. | `contracts_zodiac_modules_registry.sql`, `int_execution_zodiac_module_proxies.sql` |
| P2 (KEEP) | Add a `dbt_utils.unique_combination_of_columns` grain test on `(modifier_address, submodule_address)` for submodules_latest (both upstreams have one; this model does not). | `int_execution_zodiac_modifier_submodules_latest.sql`, `schema.yml` |
| P3 (KEEP) | Document the pre-`2023-11-01` modifier-event lookback gap in `schema.yml` (only `5` events dropped, but undocumented). | `int_execution_zodiac_modifier_module_events.sql`, `schema.yml` |
| P3 (KEEP) | Document the `is_gp` cross-unit dependency on `int_execution_gpay_wallets` (downstream of Dune) and its freshness implications. | `int_execution_zodiac_modifier_submodules_latest.sql`, `schema.yml` |
| P3 (KEEP) | Remove dead `unique_key` from the two `append` incrementals (ignored under append; on events it diverges from the RMT `order_by`, doubly misleading). | `int_execution_zodiac_module_proxies.sql`, `int_execution_zodiac_modifier_module_events.sql` |
| P4 (KEEP) | Maintenance cleanup: remove the unreachable `Unknown` multiIf arm and the mapped-but-empty `0xd62129bf` mastercopy; align the events model's `join_use_nulls` hook with its downstream; decide whether submodules_latest (`159,151` rows, 0 consumers, full rebuild each run) should be materialized at all. | `contracts_zodiac_modules_registry.sql`, `int_execution_zodiac_modifier_module_events.sql`, `int_execution_zodiac_modifier_submodules_latest.sql` |
| (DROP) | Freshness lag (C06) — resolved; lag now T-0. Optional residual: add a freshness test to guard against recurrence. | `int_execution_zodiac_module_proxies.sql`, `int_execution_zodiac_modifier_module_events.sql` |
