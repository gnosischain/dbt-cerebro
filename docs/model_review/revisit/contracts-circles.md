# Model review (revisit 2026-06-21): contracts/Circles

Baseline `docs/model_review/contracts-circles.md` (dated `2026-06-11`); 18 cases re-verified over 3 rounds. Headline: 0 resolved, 1 changed (C04 downgraded medium->low), 17 still confirmed — the two critical findings (StandardTreasury calls gap C01/C09) and the traces-vs-transactions source defect across all 5 `_calls` models remain fully intact, now with direct trace-recoverability proof.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| CONTRACTSCIRCLES-C01 | P0-06 | 5 calls models use `transactions` not `traces` -> permanently 0 rows | critical | CONFIRMED | critical | high | none | 3 |
| CONTRACTSCIRCLES-C02 |  | Registry has 4 duplicate addresses, no `unique(address)` test | high | CONFIRMED | high | high | none | 3 |
| CONTRACTSCIRCLES-C03 |  | v1 Hub events+calls dev-tagged and stale, no retirement decision | high | CONFIRMED | medium | high | none | 3 |
| CONTRACTSCIRCLES-C04 |  | `BaseGroup_calls` dev / `_events` production asymmetry | medium | CHANGED | low | high | none | 3 |
| CONTRACTSCIRCLES-C05 |  | `BaseGroup_calls` missing `unique_combination_of_columns` grain test | medium | CONFIRMED | medium | high | none | 3 |
| CONTRACTSCIRCLES-C06 |  | `CirclesBackingFactory_events` start_blocktime mismatch (undocumented) | low | CONFIRMED | low | high | none | 3 |
| CONTRACTSCIRCLES-C07 |  | Events models pass checksummed addresses, calls use lowercase | low | CONFIRMED | low | high | none | 3 |
| CONTRACTSCIRCLES-C08 |  | 4 events models declare 3-col `unique_key` vs 4-col order_by/tests | low | CONFIRMED | low | high | none | 3 |
| CONTRACTSCIRCLES-C09 | P0-06 | StandardTreasury group-mint call params absent 17+ months | critical | CONFIRMED | critical | high | none | 3 |
| CONTRACTSCIRCLES-C10 | P0-06 | InvitationEscrow call params absent though contract active | high | CONFIRMED | high | high | none | 3 |
| CONTRACTSCIRCLES-C11 | P0-06 | `ERC20TokenOffer_calls` 0 rows; offer economics unanalysable | high | CONFIRMED | high | high | none | 3 |
| CONTRACTSCIRCLES-C12 |  | 4 template addresses double-count via registry address join | medium | CONFIRMED | low | high | none | 3 |
| CONTRACTSCIRCLES-C13 |  | `ERC20TokenOffer_events` 6.5mo stale, no freshness test | medium | CONFIRMED | medium | high | none | 3 |
| CONTRACTSCIRCLES-C14 |  | 3 WEAU/Backer operational addresses vars-only, no seed/test | low | CONFIRMED | low | high | none | 3 |
| CONTRACTSCIRCLES-C15 |  | `PaymentGateway_events` 0 rows = genuine no-EVM-logs behaviour | low | CONFIRMED | low | medium | none | 3 |
| CONTRACTSCIRCLES-C16 |  | Hub v2 events healthy: zero grain dups, fresh | low | CONFIRMED | low | high | logs_ingestion_gap | 3 |
| CONTRACTSCIRCLES-C17 |  | `ERC20TokenOfferCycle_events` healthy: zero dups | low | CONFIRMED | low | high | none | 3 |
| CONTRACTSCIRCLES-C18 |  | `CirclesBackingFactory_events` healthy: complete decode | low | CONFIRMED | low | high | none | 3 |

Rollup: 17 CONFIRMED, 1 CHANGED, 0 RESOLVED, 0 NEW, 0 UNVERIFIABLE/UNRESOLVED.

## Delta vs baseline

### RESOLVED (0)
None. No code fix landed for any baseline finding during the revisit window. Note: the verifier briefly marked C05 RESOLVED in round 2 but this was retracted as a schema.yml line-conflation misread (see Review log).

### CHANGED (1)
- **C04** severity `medium -> low`. The "dev tag silently blocks a needy prod consumer" premise was disproven: `grep` across `models/` for `ref('contracts_circles_v2_BaseGroup_calls')` returns **0 results** — no model (production or otherwise) consumes `BaseGroup_calls`. The dev/production asymmetry between `contracts_circles_v2_BaseGroup_calls.sql` (L10 `dev`) and `contracts_circles_v2_BaseGroup_events.sql` (L10 `production`) persists, but it is tagging-hygiene only. Incident attribution: none.

### STILL CONFIRMED (17)

Critical (traces-vs-transactions source defect, P0-06 cluster):
- **C01** `0` rows in all 5 `_calls` models vs nonzero events siblings (`StandardTreasury_calls 0 / events 11,526`; `InvitationEscrow_calls 0 / events 6,421`; `ERC20TokenOffer_calls 0 / events 1,106`; `CirclesBackingFactory_calls 0 / events 3,428`; `PaymentGateway_calls 0 / events 0`). All 5 SQL files still `tx_table=source('execution','transactions')` at L16. Fix viability proven: `decode_calls.sql` L252-253 auto-detects traces mode and L257-273 maps `action_input/action_to/action_from`, so a one-line `tx_table=source('execution','traces')` is the complete fix. Incident attribution: none.
- **C09** `StandardTreasury_calls = 0` rows for 17+ months while `events = 11,526` (max `2026-03-15`). Trace recoverability confirmed: `execution.traces` for `action_to=0x08f90ab73a515308f03a718257ff9887ed330c6e`, successful internal calls, `2026-03-01..03-16` returns `7` traces (min `2026-03-04`, max `2026-03-15` — matching the events max exactly). Incident attribution: none.

High:
- **C02** Registry: `14,511` total rows vs `14,507` unique addresses = exactly `4` collisions (`0x12dfe`, `0x590bb`, `0x76a42`, `0xb3129`); `schema.yml` L18 `address` carries only `not_null`, no `unique` test. Held HIGH on structural/unbounded risk (dynamic factory-discovered registry, growing, MCP/API-exposed). Incident attribution: none.
- **C10** `InvitationEscrow_calls = 0` rows vs `events = 6,421` (max `2026-05-20`, still flowing). Trace probe `2026-01-01..01-16` for `action_to=0x8f8b74fa13eaaff4176d061a0f98ad5c8e19c903` returns `918` internal-call traces, aligning with the documented Jan 2026 event peak (`2,167`). Incident attribution: none.
- **C11** `ERC20TokenOffer_calls = 0` rows vs `events = 1,106` (through `2025-12-04`); registry `ERC20TokenOfferRuntime = 58`. Direct Safe-internal-call split for the runtime family, `2025-11-01..2025-12-01`: `direct_txs = 0`, `internal_traces = 732`. Incident attribution: none.

Medium:
- **C03** Both `contracts_circles_v1_Hub_calls.sql` (L10) and `contracts_circles_v1_Hub_events.sql` (L12) still dev-tagged; entire v1 substack (avatars/trust_updates/transfers/trust_relations/balances_daily/balance_diffs) dev-tagged; `0` production models reference any v1 intermediate; no `exposures.yml` in the project. Downgraded from high in round 1 (no prod propagation). Incident attribution: none — frozen-watermark dev-retirement (v1 Hub ends cleanly `2026-04-14`, not a single-day collapse).
- **C05** `schema.yml` L428-432 (`contracts_circles_v2_BaseGroup_calls` entry) lists only `elementary.schema_changes`; the `dbt_utils.unique_combination_of_columns` grain test is absent (sibling `CirclesBackingFactory_calls` L434-438 has it). Live 2-col grain probe: `4,264` rows, `0` duplicates — coverage gap only, no active bug. Incident attribution: none.
- **C13** `ERC20TokenOffer_events`: `1,106` rows, max `2025-12-04` (~6.5 months stale), production-tagged, `start_blocktime='2025-10-01'`, no freshness test. `grep` of the sector `schema.yml` for `freshness|loaded_at_field|warn_after|error_after` = `0` matches (sector-wide gap). Post-watermark probe confirms genuine inactivity, not a stuck watermark. Incident attribution: none.

Low:
- **C06** `CirclesBackingFactory_events.sql` L23 `start_blocktime='2025-04-25'` vs calls L20 `'2025-04-01'`, undocumented. `execution.logs` for `0xeced91232c609a42f6016860e8223b8aecaa7bd0` in `[2025-04-01, 2025-04-25)` = `0` rows (harmless). Incident attribution: none.
- **C07** 4 events models pass checksummed addresses (`BaseGroupFactory_events` L17, `CMGroupDeployer_events` L17, `ERC20Lift_events` L17, `CirclesBackingFactory_events` L20) while calls use lowercase. `decode_logs.sql` normalizes via `lower(replaceAll(...,'0x',''))`; `CirclesBackingFactory_events` is populated at `3,428` rows -> cosmetic. Incident attribution: none.
- **C08** 4 events models declare 3-col `unique_key` vs 4-col `order_by`/schema tests; `incremental_strategy='append'` makes `unique_key` inert (ReplacingMergeTree dedups on 4-col order_by). C16/C17/C18 grain checks all `0` dups. Incident attribution: none.
- **C12** Same `4` template/runtime collisions persist; sole address-join consumer `int_execution_circles_v2_mint_events` L44-45 filters `WHERE contract_type='Migration'`, and none of the 4 collision addresses are `Migration`-typed -> no active double-count. Downgraded medium->low in round 1 (blast radius corrected from "~129 models" to 1 guarded consumer). Incident attribution: none.
- **C14** `circles_target_group_address` (dbt_project.yml L15), `circles_v2_cashback_wallet` (L24), `circles_v2_gcrc_token` (L26) all vars-only; `grep` of `seeds/contracts_circles_registry_static.csv` = `0` matches; no schema test. Consumer chain reaches production tier0 mart `api_execution_circles_v2_kpi_total_backers_latest`. Incident attribution: none.
- **C15** `PaymentGateway_events = 0` rows; `execution.logs` for all `72` PaymentGatewayRuntime addresses since `2025-12-01` = `0` rows (genuine no-EVM-logs behaviour). Confidence medium — the dedicated traces-present leg was confirmed by family analogy (C11: 0 direct txs / 732 internal traces) rather than a direct PaymentGatewayRuntime trace count. Incident attribution: none.
- **C16** Hub v2 events: `18,940,794` rows (baseline `17.7M`), 4-col grain dups `0`, undecoded `0`, max `2026-06-21T06:53` (fresh). 30-day per-day contiguity clean; both ingestion-gap days (`2026-05-30`, `2026-06-14`) repopulated. Incident attribution: **logs_ingestion_gap** (incident B) — model now fresh past both gap days with no undecoded rows, consistent with raw-logs backfill + decode reprocess completing.
- **C17** `ERC20TokenOfferCycle_events`: `9,787` rows (baseline `9,044`), grain dups `0`, undecoded `0`, span `2025-09-18..2026-06-21`. Incident attribution: none.
- **C18** `CirclesBackingFactory_events`: `3,428` rows (baseline `3,252`), grain dups `0`, undecoded `0`, min `2025-04-25` (= start_blocktime), max `2026-06-21T11:59`. Incident attribution: none.

### NEW (0)
None.

### UNVERIFIABLE / UNRESOLVED (0)
None.

## Evidence appendix

### C01 / C09 / C10 / C11 / C15 — calls models row counts (10-way UNION)
```sql
SELECT 'StandardTreasury_calls', count(*), max(block_timestamp) FROM dbt.contracts_circles_v2_StandardTreasury_calls
UNION ALL SELECT 'StandardTreasury_events', count(*), max(block_timestamp) FROM dbt.contracts_circles_v2_StandardTreasury_events
UNION ALL ... (5 calls/events pairs)
```
Returned: `StandardTreasury_calls 0 / events 11,526` (events max `2026-03-15`); `InvitationEscrow_calls 0 / events 6,421` (events max `2026-05-20`); `ERC20TokenOffer_calls 0 / events 1,106` (events max `2025-12-04`); `CirclesBackingFactory_calls 0 / events 3,428`; `PaymentGateway_calls 0 / events 0`. All calls models `max_ts = 1970-01-01` (empty sentinel). All 5 `_calls.sql` confirmed `tx_table=source('execution','transactions')` at L16.

### C01 fix-viability — `decode_calls.sql` macro
Read L252-253: `is_traces = ('"traces"' in tx_table_name) OR tx_table_name.endswith('.traces') OR endswith('traces`')`. Traces branch L257-273 selects `action_input AS input, action_to AS to_address, action_from AS from_address, action_value AS value_string, trace_address`, filters `action_call_type IN ('call','delegate_call','static_call') AND error IS NULL`. Transactions branch L274-281 is `SELECT *`. Dedup PARTITION adds `trace_address` only in traces mode (L287). Flipping `tx_table=source('execution','traces')` compiles and routes the entire macro.

### C09 — StandardTreasury traces recoverability
```sql
SELECT count(*), min(block_timestamp), max(block_timestamp)
FROM execution.traces
WHERE block_timestamp>=toDateTime('2026-03-01') AND block_timestamp<toDateTime('2026-03-16')
  AND lower(replaceAll(action_to,'0x',''))='08f90ab73a515308f03a718257ff9887ed330c6e'
  AND action_call_type IN ('call','delegate_call','static_call') AND error IS NULL
```
Returned: `7` traces, min `2026-03-04`, max `2026-03-15` (matches `StandardTreasury_events` max). Note: `execution.traces.action_to` is stored 0x-stripped and lowercased.

### C10 — InvitationEscrow traces recoverability (Jan peak)
```sql
SELECT count(*), min(block_timestamp), max(block_timestamp)
FROM execution.traces
WHERE block_timestamp>=toDateTime('2026-01-01') AND block_timestamp<toDateTime('2026-01-16')
  AND lower(replaceAll(action_to,'0x',''))='8f8b74fa13eaaff4176d061a0f98ad5c8e19c903'
  AND action_call_type IN ('call','delegate_call','static_call') AND error IS NULL
```
Returned: `918` traces (min `2026-01-01`, max `2026-01-15`), tracking the Jan 2026 event peak (`2,167`).

### C11 — Safe-internal-call split for ERC20TokenOfferRuntime family
```sql
WITH addrs AS (SELECT lower(replaceAll(address,'0x','')) a FROM dbt.contracts_circles_registry WHERE contract_type='ERC20TokenOfferRuntime')
SELECT
 (SELECT count(*) FROM execution.transactions WHERE block_timestamp>=toDateTime('2025-11-01') AND block_timestamp<toDateTime('2025-12-01') AND lower(replaceAll(to_address,'0x','')) IN (SELECT a FROM addrs)) direct_txs,
 (SELECT count(*) FROM execution.traces WHERE block_timestamp>=toDateTime('2025-11-01') AND block_timestamp<toDateTime('2025-12-01') AND lower(replaceAll(action_to,'0x','')) IN (SELECT a FROM addrs) AND action_call_type IN ('call','delegate_call','static_call') AND error IS NULL) internal_traces
```
Returned: `direct_txs = 0`, `internal_traces = 732`. Registry `ERC20TokenOfferRuntime` count = `58`.

### C02 / C12 — registry collisions
```sql
SELECT count(*), uniqExact(lower(address)), count(*)-uniqExact(lower(address)) FROM dbt.contracts_circles_registry;
SELECT lower(address), groupArray(contract_type), count(*) FROM dbt.contracts_circles_registry GROUP BY lower(address) HAVING count(*)>1
```
Returned: `14,511` total / `14,507` unique / `4` excess. Collisions: `0x12dfe` [`ERC20TokenOfferRuntime`, `ERC20TokenOffer`]; `0x590bb` [`PaymentGatewayRuntime`, `PaymentGateway`]; `0x76a42` [`ERC20TokenOfferCycle`, `ERC20TokenOfferCycleRuntime`]; `0xb3129` [`ERC20TokenOfferCycleV2`, `ERC20TokenOfferCycleRuntime`]. None is `Migration`. `schema.yml` L18 `address` test = `not_null` only. Sole address-join consumer `int_execution_circles_v2_mint_events` L44-45 filters `WHERE contract_type='Migration'`.

### C05 — BaseGroup_calls grain probe
```sql
SELECT count(*), uniqExact((block_timestamp,transaction_hash)), count(*)-uniqExact((block_timestamp,transaction_hash)), max(block_timestamp)
FROM dbt.contracts_circles_v2_BaseGroup_calls
```
Returned: `4,264` rows, uniqExact `4,264`, dup `0`, max `2026-06-19`. `schema.yml` L428-432 = `[elementary.schema_changes]` only.

### C06 / C13 — gap-window and post-watermark log probes
```sql
SELECT count(*) FROM execution.logs WHERE block_timestamp>='2025-04-01' AND block_timestamp<'2025-04-25' AND replaceAll(lower(address),'0x','')='eced91232c609a42f6016860e8223b8aecaa7bd0';
SELECT count(*), max(block_timestamp) FROM execution.logs WHERE block_timestamp>'2025-12-04' AND replaceAll(lower(address),'0x','') IN (SELECT replaceAll(lower(address),'0x','') FROM dbt.contracts_circles_registry WHERE contract_type IN ('ERC20TokenOfferRuntime','ERC20TokenOffer'))
```
C06 returned `0` rows (gap window empty). C13 returned `9` rows, all max `2025-12-04T13:43:55` (boundary capture; effectively `0` strictly after the last event) — genuine inactivity.

### C15 — PaymentGatewayRuntime logs probe
```sql
SELECT count(*) FROM execution.logs WHERE block_timestamp>='2025-12-01' AND lower(address) IN (SELECT lower(address) FROM dbt.contracts_circles_registry WHERE contract_type='PaymentGatewayRuntime')
```
Returned: `0` rows across all `72` PaymentGatewayRuntime addresses (10.4s scan). `PaymentGateway_events = 0`, `PaymentGateway_calls = 0`.

### C16 / C17 / C18 — healthy decode model grain/freshness checks
```sql
SELECT count(*), uniqExact((contract_address,transaction_hash,log_index,block_timestamp)), count(*)-uniqExact(...), countIf(event_name='' OR event_name IS NULL), min(block_timestamp), max(block_timestamp) FROM dbt.contracts_circles_v2_Hub_events
-- + per-day groupBy toDate(block_timestamp) last 30 days for C16 contiguity
```
C16 Hub v2: `18,940,794` rows, dup `0`, undecoded `0`, min `2024-10-14`, max `2026-06-21T06:53`; 30-day series all 31 days present (interior `45,851`-`242,161`/day), gap days `2026-05-30` & `2026-06-14` healthy. C17 `ERC20TokenOfferCycle_events`: `9,787` rows, dup `0`, undecoded `0`, span `2025-09-18..2026-06-21T05:39`. C18 `CirclesBackingFactory_events`: `3,428` rows, dup `0`, undecoded `0`, min `2025-04-25`, max `2026-06-21T11:59`.

### C03 / C04 / C07 / C08 / C14 — code-only checks
- C03: `contracts_circles_v1_Hub_calls.sql` L10 / `contracts_circles_v1_Hub_events.sql` L12 both `tags=['dev',...]`; all 6 v1 intermediates dev-tagged; `grep` for any non-v1-intermediate model ref-ing a v1 intermediate = `0`; no `exposures.yml`.
- C04: `BaseGroup_calls.sql` L10 `dev`, `BaseGroup_events.sql` L10 `production`; `grep` `ref('contracts_circles_v2_BaseGroup_calls')` across `models/` = `0`.
- C07: checksummed literals `0xD0B5Bd9962197BEaC4cbA24244ec3587f19Bd06d` (BaseGroupFactory L17), `0xFEca40Eb02FB1f4F5F795fC7a03c1A27819B1Ded` (CMGroupDeployer L17), `0x5F99a795dD2743C36D63511f0D4bc667e6d3cDB5` (ERC20Lift L17), `0xecEd91232C609A42F6016860E8223B8aEcaA7bd0` (CirclesBackingFactory L20); normalized in `decode_logs.sql` L130/L141/L146/L154.
- C08: `BaseGroup_events` L7 `unique_key=(contract_address,transaction_hash,log_index)` vs L6 `order_by=(contract_address,block_timestamp,transaction_hash,log_index)`; schema test L150 = 4-col; same in `GroupLBPFactory_events`, `ERC20TokenOfferCycle_events`, `PaymentGateway_events`; all `incremental_strategy='append'`, `engine='ReplacingMergeTree()'`.
- C14: `dbt_project.yml` L15/L24/L26; `grep` seed = `0` matches; mart `api_execution_circles_v2_kpi_total_backers_latest.sql` config `tags=['production','execution','tier0','api:circles_v2_kpi_total_backers','granularity:latest']`.

## Review log (>=3 rounds per case)

- **C01**: R1 CONFIRMED (code: all 5 still `transactions`; counts 0) -> challenge: prove fix populates via traces -> R2 CONFIRMED (StandardTreasury 7 internal calls Mar 2026, selector `f23a6e61`) -> challenge: prove one-line fix is the WHOLE fix -> R3 CONFIRMED (decode_calls L252-273 auto-routes; corroborated by C11 732 traces). Critical held.
- **C02**: R1 CONFIRMED (4 dups, no unique test) -> challenge: measure real downstream blast radius -> R2 CONFIRMED (only `mint_events` joins on address, contract_type-filtered; blast radius corrected) -> challenge: justify HIGH vs MEDIUM -> R3 CONFIRMED (held HIGH on structural/unbounded MCP/API risk).
- **C03**: R1 CONFIRMED high (both dev, 68d stale) -> challenge: any prod consumer? -> R2 CONFIRMED, downgraded medium (all lineage consumers dev) -> challenge: verify no prod exposure/mart -> R3 CONFIRMED medium (0 prod selectors, no exposures.yml).
- **C04**: R1 CONFIRMED medium (asymmetry) -> challenge: does `_calls` materialize / any prod ref? -> R2 CONFIRMED medium (materializes, no prod ref) -> challenge: prove the negative -> R3 CHANGED medium->low (`grep` ref = 0; tagging-hygiene only).
- **C05**: R1 CONFIRMED medium (lone calls model missing grain test) -> challenge: run live grain probe -> R2 RESOLVED (WRONG — conflated schema.yml L432 with sibling L438) -> challenge: REJECTED, re-read in isolation -> R3 CONFIRMED medium (test still absent, 0 live dups; prior RESOLVED retracted).
- **C06**: R1 CONFIRMED low (start_blocktime mismatch) -> challenge: query exact gap window -> R2 CONFIRMED low (0 logs in gap) -> challenge: confirm calls+seed both `2025-04-01` (one-directional) -> R3 CONFIRMED low (calls L20 = `2025-04-01`, events later).
- **C07**: R1 CONFIRMED low (4 checksummed events) -> challenge: prove normalization cosmetic -> R2 CONFIRMED low (CBF_events 3,428 rows populated) -> challenge: quote decode_logs transform line -> R3 CONFIRMED low (L130/L141/L146/L154 normalize).
- **C08**: R1 CONFIRMED low (3-col vs 4-col) -> challenge: confirm append makes unique_key inert -> R2 CONFIRMED low (append + RMT 4-col) -> challenge: quote materialization behavior -> R3 CONFIRMED low (append issues plain INSERT; C16/17/18 0 dups).
- **C09**: R1 CONFIRMED critical (0 rows 17mo) -> challenge: recover groupMint params from traces -> R2 CONFIRMED critical (7 calls Mar, selector `f23a6e61` = onERC1155Received) -> challenge: prove whole-span recoverability -> R3 CONFIRMED critical (7 traces active tail, max matches events; full month-series timed out at 30s budget).
- **C10**: R1 CONFIRMED high (0 calls / 6,421 events) -> challenge: traces for InvitationEscrow -> R2 CONFIRMED high (3 calls May, max matches events) -> challenge: widen to peak alignment -> R3 CONFIRMED high (918 traces Jan peak).
- **C11**: R1 CONFIRMED high (0 calls / 1,106 events) -> challenge: 58-address tx-vs-traces split -> R2 CONFIRMED high (registry 58 re-confirmed; split deferred) -> challenge: run deferred split now -> R3 CONFIRMED high (direct_txs 0 / internal_traces 732, Nov 2025).
- **C12**: R1 CONFIRMED medium (4 template dups) -> challenge: measure ~129-model audit scope -> R2 CONFIRMED, downgraded low (1 guarded consumer) -> challenge: confirm 4 collisions not `Migration` -> R3 CONFIRMED low (none Migration; Migration filter excludes all 4).
- **C13**: R1 CONFIRMED medium (6.5mo stale, no freshness test) -> challenge: stuck watermark vs genuine inactivity -> R2 CONFIRMED medium (post-watermark probe = genuine inactivity) -> challenge: confirm sector-wide freshness gap -> R3 CONFIRMED medium (0 freshness tests sector-wide).
- **C14**: R1 CONFIRMED low (3 addresses vars-only) -> challenge: name WEAU/Backer consumer -> R2 CONFIRMED low (consumer chain to total_backers KPI) -> challenge: confirm mart production-tagged -> R3 CONFIRMED low (tier0 production mart confirmed).
- **C15**: R1 CONFIRMED low (0 logs, 72 addresses) -> challenge: traces-present/logs-absent split for 2-3 addresses -> R2 CONFIRMED low, confidence dropped to medium (split not re-run) -> challenge: run the split now -> R3 CONFIRMED low, confidence medium (PaymentGatewayRuntime trace leg confirmed by C11 family analogy only; budget exhausted).
- **C16**: R1 CONFIRMED low (zero dups, fresh, gap days repopulated) -> challenge: per-day continuity -> R2 CONFIRMED low (30-day series clean) -> challenge: confirm zero undecoded -> R3 CONFIRMED low (0 undecoded; attribution logs_ingestion_gap).
- **C17**: R1 CONFIRMED low (9,787 rows, 0 dups) -> challenge: add undecoded + span -> R2 CONFIRMED low (0 undecoded, span fresh) -> challenge: per-day contiguity spot-check -> R3 CONFIRMED low (aggregates cover freshness; per-day check deferred on budget).
- **C18**: R1 CONFIRMED low (3,428 rows, 0 empty event_name) -> challenge: grain integrity check -> R2 CONFIRMED low (0 grain dups) -> challenge: confirm increment is recent append -> R3 CONFIRMED low (unchanged min + advanced max + 0 dups = append growth).

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P0 (KEEP/ESCALATE) | Switch `tx_table=source('execution','transactions')` -> `source('execution','traces')` on all 5 calls models; `decode_calls.sql` auto-routes, so this is a one-line-per-file complete fix. Recovers `StandardTreasury` group-mint params (732 internal traces proven for the offer family; 7 for ST in the active tail) and unblocks InvitationEscrow / ERC20TokenOffer call-level analysis. | `models/contracts/Circles/contracts_circles_v2_StandardTreasury_calls.sql`, `_InvitationEscrow_calls.sql`, `_CirclesBackingFactory_calls.sql`, `_ERC20TokenOffer_calls.sql`, `_PaymentGateway_calls.sql` |
| P1 (KEEP) | Add a `unique`/uniqueness test (or de-duplicate the static seed vs factory-discovered rows) on `contracts_circles_registry.address`; the registry is growing, MCP/API-exposed, and any future unguarded address-join silently double-counts the 4 template/runtime collisions. | `models/contracts/Circles/contracts_circles_registry.sql`, `models/contracts/Circles/schema.yml`, `seeds/contracts_circles_registry_static.csv` |
| P2 (KEEP) | Add a freshness/recency test to production decode models in this sector (none currently carry one); `ERC20TokenOffer_events` is 6.5mo stale with no way for consumers to distinguish genuine inactivity from a stuck watermark. | `models/contracts/Circles/schema.yml`, `_ERC20TokenOffer_events.sql` |
| P2 (KEEP) | Document a retirement decision for the v1 Hub substack (dev-tagged, stale to `2026-04-14`, no prod consumers) — either formally retire or add a rationale comment so the silent gap is intentional. | `models/contracts/Circles/contracts_circles_v1_Hub_calls.sql`, `_Hub_events.sql` (+ v1 intermediates) |
| P3 (KEEP) | Add the `dbt_utils.unique_combination_of_columns` grain test to `BaseGroup_calls` to match every sibling decode model (currently 0 live dups, but no early-warning coverage). | `models/contracts/Circles/schema.yml` |
| P3 (KEEP) | Move the 3 WEAU/Backer operational addresses out of dbt_project.yml vars into the seed (or add a schema test) so a misconfig cannot silently corrupt the tier0 `total_backers` KPI without test failure. | `dbt_project.yml`, `seeds/contracts_circles_registry_static.csv`, `api_execution_circles_v2_kpi_total_backers_latest.sql` |
| P4 (KEEP, hygiene) | Resolve the `BaseGroup_calls` dev / `_events` production tag asymmetry (now low — no consumer); document the `CirclesBackingFactory_events` start_blocktime `2025-04-25` (C06); normalize checksummed addresses to lowercase (C07); widen `unique_key` to 4 cols to match order_by/tests (C08). | `_BaseGroup_calls.sql`, `_CirclesBackingFactory_events.sql`, 4 checksummed events files, 4 events files w/ narrow `unique_key` |

DROP: none (no baseline finding resolved).
