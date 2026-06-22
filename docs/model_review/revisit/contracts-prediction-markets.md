# Model review (revisit 2026-06-21): contracts/prediction-markets

Baseline: `docs/model_review/contracts-prediction-markets.md` (2026-06-11). Re-verified all `15` cases over `3` rounds. Headline: `0` resolved, `2` changed (C01 latent -> actively-collapsing; C11 critical halt -> low chain-retirement), `13` still confirmed — the EIP-1167 clone trading blind spot (C10) remains critical and the runner silent-drop -> FPMM calls 81-day staleness (C02/C12) is still live and unrepaired.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| CONTRACTSPREDICTIONMARKETS-C01 | | Wrapped1155Factory RMT keyed on `transaction_hash` not `log_index` (multi-event tx collapse) | high | CHANGED | high | high | none | 3 |
| CONTRACTSPREDICTIONMARKETS-C02 | P0-08 | `dbt_incremental_runner.py` 30-slice cap silently drops models (empty slice, exit 0, no alert) | high | CONFIRMED | high | high | none | 3 |
| CONTRACTSPREDICTIONMARKETS-C03 | | AgentResultMapping calls `start_blocktime` 20d after events; Jun 23-29 2025 calls absent | high | CONFIRMED | high | high | none | 3 |
| CONTRACTSPREDICTIONMARKETS-C04 | | `decode_logs` single-address path bare equality, lacks `lower()/replaceAll()` normalization | medium | CONFIRMED | medium | high | none | 3 |
| CONTRACTSPREDICTIONMARKETS-C05 | | `toStartOfMonth` partitioning nearing CH Cloud 100-partition rebuild ceiling | medium | CHANGED | low | high | none | 3 |
| CONTRACTSPREDICTIONMARKETS-C06 | | `decode_calls` tx-level dedup misses internal/router/proxy calls | medium | CONFIRMED | medium | high | none | 3 |
| CONTRACTSPREDICTIONMARKETS-C07 | | schema.yml column defs are fabricated stubs / macro-params-as-columns | medium | CONFIRMED | medium | high | none | 3 |
| CONTRACTSPREDICTIONMARKETS-C08 | | FPMM calls 7 rows (0.34%) null `function_name` (unresolved 4-byte selector) | low | CONFIRMED | low | high | none | 3 |
| CONTRACTSPREDICTIONMARKETS-C09 | | No `api:/granularity:/window:/tier:` tags on any of 10 PM models; guard bypassable | low | CONFIRMED | low | high | none | 3 |
| CONTRACTSPREDICTIONMARKETS-C10 | | FPMM_events captures zero Omen trading (EIP-1167 clone events outside factory filter) | critical | CONFIRMED | critical | high | none | 3 |
| CONTRACTSPREDICTIONMARKETS-C11 | P0-08 | OmenAgentResultMapping stale 309/319 days | critical | CHANGED | low | high | none | 3 |
| CONTRACTSPREDICTIONMARKETS-C12 | | FPMM_calls stale (max 2026-04-01) while events current; runner cap freeze | high | CONFIRMED | high | high | none | 3 |
| CONTRACTSPREDICTIONMARKETS-C13 | | Zero warehouse analytics layer (data terminus, no downstream consumers) | high | CONFIRMED | high | high | none | 3 |
| CONTRACTSPREDICTIONMARKETS-C14 | | OmenARM v1 (scalar) vs ARM v2 (arrays) incompatible schemas, no unioning model | medium | CONFIRMED | medium | high | none | 3 |
| CONTRACTSPREDICTIONMARKETS-C15 | | Seer Wrapped1155 `start_blocktime` 8 months pre-Seer-deployment noise | low | CONFIRMED | low | high | none | 3 |

Final severity ladder: `1` critical (C10), `5` high (C01, C02, C03, C12, C13), `4` medium (C04, C06, C07, C14), `5` low (C05, C08, C09, C11, C15).

## Delta vs baseline

### RESOLVED (0)
None. No defect was fixed between the baseline (2026-06-11) and this revisit (2026-06-21).

### CHANGED (2)
- **C01 — `models/contracts/SeerPM/contracts_Seer_Wrapped1155Factory_events.sql`**: status upgraded from *latent* to *actively-collapsing*. Severity held at `high`. A bounded on-chain scan (`rpc_scan_logs` Wrapped1155Creation, blocks `44,800,000`->`46,800,000`) returned `3,230` creation logs across `892` distinct tx with **every** tx emitting `>1` event (max `29` logs/tx). The warehouse table holds `2,347` rows = `uniqExact(transaction_hash)` = `2,347` with `0` dup-tx — that is the ReplacingMergeTree key `(block_timestamp, transaction_hash)` already collapsing the collisions (~`72%` of in-window events lost), not evidence of safety. Incident attribution: `none`.
- **C11 — `models/contracts/OmenAgentResultMapping/contracts_OmenAgentResultMapping_{events,calls}.sql`**: status changed CONFIRMED->CHANGED, severity downgraded `critical`->`low`. Three on-chain scans of `0x260e1077dea98e738324a6cefb0ee9a272ed471a` returned **zero** logs across every window after `2025-08-06` (blocks `41,473,512`->`42,500,000`; May 2026 `45.94M`-`46.2M`; Jun 2026 `46.8M`->latest). The warehouse max (`2025-08-06T20:27:40`, `65,919` event / `46,124` call rows) coincides with the contract's last on-chain emission: the data is current with chain (chain-retirement), not a pipeline halt. Residual: the retirement is **undocumented** in code (no deprecation note / `end_date` in the model dir or `schema.yml` meta). Incident attribution: `none` (append-strategy, not the June insert_overwrite incident).

### STILL CONFIRMED (13)
- **C10 (critical) — `models/contracts/FPMMDeterministicFactory/contracts_FPMMDeterministicFactory_events.sql`**: `GROUP BY event_name` returns exactly `1` distinct event (`FixedProductMarketMakerCreation`, `21,645` rows, up from baseline `21,167`); zero `FPMMBuy/Sell/FundingAdded/FundingRemoved` rows. Round 2 proved the EIP-1167 mechanism end-to-end: clone `0x7ec4f4c96521bb8481dab4c88b6fbba6a2ebfdd2` (extracted from a creation row) emitted `42` FPMMBuy logs on-chain (blocks `46,808,116`-`46,816,319`) entirely outside the factory-address filter. Trading/LP/price data for all ~`21.6k` markets is absent. Incident: `none`.
- **C02 (high, P0-08) — `scripts/refresh/dbt_incremental_runner.py`**: cap branch lines `1114-1126` prints `[error]` to stderr then `plan.append((stage, []))` + `continue`; the main loop appends to `failures` only on `rc!=0`, never for empty slices; `return 0` at line `1379`. `cron_preview.sh` execs `run_dbt_observability.sh` with no exit-code freshness gate. A dropped model is invisible end-to-end. Incident: `none`.
- **C12 (high) — `models/contracts/FPMMDeterministicFactory/contracts_FPMMDeterministicFactory_calls.sql`**: calls frozen at `max(block_timestamp)=2026-04-01T12:06:35` (`2,034` rows) while events are current at `2026-06-21T03:56:30` (`21,645` rows). Staleness widened `71`->`81` days. `.gap_refresh_state.json` `completed` lists `_events` but not `_calls`. Same-contract events-current/calls-stale asymmetry = the live instance of the C02 silent cap (`81` slices >> `30`). Model is `incremental_strategy='append'` (line 4) -> fix is a bounded append backfill, NOT incident-A-class. Incident: `none`.
- **C03 (high) — `contracts_AgentResultMapping_{calls,events}.sql`**: calls `start_blocktime='2025-06-30'` vs events `start_blocktime='2025-06-10'`; calls `min=2025-06-30T00:01:15` with `0` rows before it; events `min=2025-06-23T10:52:45` with `2,865` rows in Jun 23-29 2025. Same contract `0x99c43743...` emitted those events, so the gap is a configured cutoff, not inactivity. Append-strategy -> bounded backfill fix. Incident: `none`.
- **C13 (high) — all 10 PM decode tables**: `get_downstream_impact(contracts_ConditionalTokens_events)` returns `1` consumer = its own `dbt_utils_unique_combination_of_columns` schema test; grep finds `0` external `ref()/source()` to any of the 6 PM prefixes. Data terminus confirmed against the compiled manifest. Incident: `none`.
- **C04 (medium) — `macros/decoding/decode_logs.sql`**: single-address branch line `146` is bare equality `address = '<addr>'`; multi-address line `141` and ref path line `110` normalize via `lower(replaceAll(...))`. Fresh source check over the `2026-06-20` partition (`3,674,270` logs): `countIf(address!=lower(address))=0`, `countIf(startsWith(address,'0x'))=0` — `100%` 0x-stripped lowercase today, the sole reason the bare path is currently safe. Latent robustness gap. Incident: `none`.
- **C06 (medium) — `macros/decoding/decode_calls.sql`**: tx-level dedup `ROW_NUMBER PARTITION BY block_number, transaction_index` over `execution.transactions`; traces branch (`is_traces` ~line 253) unused by all 6 PM calls models. Materiality now quantified via warehouse set-difference (blocks `46.6M`-`46.7M`): `30,231` distinct tx emitted ConditionalTokens events but only `535` are captured as top-level CT calls — `29,696` (`98.2%`) of CT-interacting tx reached CT via internal calls invisible to the tx-level path. Events layer still captures the activity; only the calls layer undercounts. Incident: `none`.
- **C07 (medium) — all 6 PM `schema.yml`**: column lists still fabricated vs actual `decode_calls`/`decode_logs` output. Consumer reach proven: `get_relevant_columns(contracts_FPMMDeterministicFactory_calls)` returns the **stub** set (`call_data, caller_address, output_json_type, output_data, gas_used, status, created_at, updated_at`), not the live-catalog set (`function_name, decoded_input, value, nonce, gas_price`). `authoritative:false` does not strip the stubs from the manifest path, so the drift reaches the LLM/API column-discovery path. Incident: `none`.
- **C14 (medium) — OmenARM v1 vs ARM v2 events**: distinct contracts (`0x260e1077` vs `0x99c43743`); decoded_params keys differ — v1 has scalar `estimatedProbabilityBps`, v2 has `estimatedProbabilitiesBps[]` + parallel `outcomes[]`. No unioning intermediate model (C13 = 0 consumers). Note: schema.yml column NAMES are NOT identical (baseline wording imprecise) — only `result_value` overlaps; both are generic-and-misaligned. Incident: `none`.
- **C08 (low) — FPMM calls**: `7` of `2,034` rows (`0.34%`) null `function_name` (same frozen rows). Mechanism identified: all `7` tx share unresolved 4-byte selector `0x23f66e47`, absent from the `function_signatures` seed. Incident: `none`.
- **C09 (low) — all 10 PM models**: grep returns `0` `api:/granularity:/window:/tier:` tags and `0` `internal/expose_to_mcp` annotations. `check_api_tags.py` skips non-production and api-untagged models (`if not api: continue`), so promotion without retagging would bypass every rule. The "internal-only not annotated" sub-claim is informational (convention has no required internal annotation). Incident: `none`.
- **C15 (low) — `contracts_Seer_Wrapped1155Factory_events.sql`**: `start_blocktime='2024-02-07'`; `212` of `2,347` rows (`9.0%`) predate Seer MarketFactory deploy (`2024-10-08`, per SeerPM/schema.yml meta). No Seer market existed before that date, so by timestamp ordering these `212` wraps cannot reference any Seer market — genuinely unrelated ERC-1155 noise. Incident: `none`.

### NEW (0)
None.

### UNVERIFIABLE / UNRESOLVED (0)
None. All 15 cases settled with 3 rounds of self-consistent evidence.

## Evidence appendix

### C01 — Wrapped1155Factory RMT key
- Code: `models/contracts/SeerPM/contracts_Seer_Wrapped1155Factory_events.sql` lines 6-7 `order_by`/`unique_key` = `(block_timestamp, transaction_hash)`.
- Warehouse: `SELECT count() - uniqExact(transaction_hash) FROM dbt.contracts_Seer_Wrapped1155Factory_events` -> `0`; `count()=2,347`, `uniqExact(transaction_hash)=2,347`.
- On-chain (`rpc_scan_logs` Wrapped1155Creation @ `0xd194319d1804c1051dd21ba1dc931ca72410b79f`, blocks `44,800,000`->`46,800,000`): `3,230` logs, `892` distinct tx, `multi_event_txs=892` (100%), `max_logs_per_tx=29`.

### C02 — runner silent drop
- `scripts/refresh/dbt_incremental_runner.py`: cap lines `1114-1126` (`print [error]` + `plan.append((stage,[]))` + `continue`); failures appended only on `rc!=0` (~line 1354); `return 0` line `1379`; `max_slices_per_stage` default `30` (line ~1022).
- `cron_preview.sh`: `exec /app/scripts/run_dbt_observability.sh`, `MANDATORY_STEPS=dbt-run,edr-report`, Monitor gated only on `$SLACK_WEBHOOK`; no post-runner freshness/exit-code gate.

### C03 — AgentResultMapping calls/events start gap
- Code: calls `start_blocktime='2025-06-30'`, events `start_blocktime='2025-06-10'` (both line 23); contract `0x99c43743a2dbd406160cc43cf08113b17178789c`; calls `incremental_strategy='append'` (line 4).
- SQL: calls `min(block_timestamp)=2025-06-30T00:01:15`, `0` rows `< 2025-06-30`; events `min=2025-06-23T10:52:45`; Jun 23-29 2025 -> calls `0`, events `2,865`.

### C04 — decode_logs single-address normalization
- `macros/decoding/decode_logs.sql`: line `146` `addr_filter = address_column = '<addr>'` (bare); line `141` and ref path line `110` use `lower(replaceAll(...,'0x',''))`.
- `SELECT countIf(address!=lower(address)) mixedcase, countIf(startsWith(address,'0x')) prefixed, count() FROM execution.logs WHERE block_timestamp>=toDateTime('2026-06-20')` -> `mixedcase=0`, `prefixed=0`, `total=3,674,270`.

### C05 — partition cap
- 6 models: `partition_by='toStartOfMonth(block_timestamp)'`, `incremental_strategy='append'`. Distinct months: ConditionalTokens_events=`68`, Realitio_v2_1_events=`63`, FPMMDeterministicFactory_events=`62` (all `<100`); count-based trigger ~Jan 2029.
- `scripts/full_refresh/refresh.py` `run_model_batched` (lines ~486-517): one `dbt run -s <model> --vars {start_month,end_month}` per `batch_months=6` batch, `--full-refresh` appended ONLY to the first batch (lines 506-508) -> ~6-partition INSERTs; the 100/252 cap is never approached on the supported path.

### C06 — decode_calls tx-level dedup
- `macros/decoding/decode_calls.sql`: reads `execution.transactions`, dedup `ROW_NUMBER PARTITION BY block_number, transaction_index` (~line 287); `is_traces` branch unused by all 6 PM models.
- Set-difference (blocks `46,600,000`-`46,700,000`): `30,231` distinct tx emitting CT events vs `535` distinct tx as top-level CT calls -> `29,696` (`98.2%`) absent from the calls model.

### C07 — schema.yml drift
- `get_relevant_columns(contracts_FPMMDeterministicFactory_calls)` -> stub set: `call_data, caller_address, output_json_type, output_data, gas_used, status, created_at, updated_at`.
- `describe_table` / live catalog -> `block_number, block_timestamp, transaction_hash, contract_address, nonce, gas_price, value, function_name, decoded_input`.
- SeerPM `schema.yml` documents macro params `incremental_column`, `start_blocktime` as output columns; ConditionalTokens lists `condition_id, condition_type, outcome_index`. All `authoritative: false`.

### C08 — null function_name
- `SELECT countIf(function_name IS NULL OR function_name='') FROM contracts_FPMMDeterministicFactory_calls` -> `7` of `2,034` (`0.34%`).
- Inline-literal join (53.5s) on the 7 tx (`5cbd82b7..., 4c27dddd..., 948a82fa..., f1387f32..., cce9ffef..., d2a17ec9..., 6a7dcdc2...`): `SELECT substring(input,1,10) selector, count() FROM execution.transactions WHERE transaction_hash IN (...)` -> all `7` share selector `0x23f66e47`, unresolved in `function_signatures` seed.

### C09 — missing API tags
- `grep -rE 'api:|granularity:|window:|tier:|expose_to_mcp|internal'` across the 6 PM dirs -> `0` matches.
- `scripts/checks/check_api_tags.py` lines ~51-57: `if 'production' not in tags: continue`; `api=[t for t in tags if t.startswith('api:')]`; `if not api: continue`. No required internal/expose_to_mcp annotation.

### C10 — FPMM zero trading capture
- `SELECT event_name, count() FROM contracts_FPMMDeterministicFactory_events GROUP BY event_name` -> exactly `1`: `FixedProductMarketMakerCreation` = `21,645` rows; `0` of any FPMMBuy/Sell/Funding event_name.
- On-chain: clone `0x7ec4f4c96521bb8481dab4c88b6fbba6a2ebfdd2` (from creation tx `f4085560...`, `2026-06-21`) -> `rpc_scan_logs` FPMMBuy = `42` logs (blocks `46,808,116`-`46,816,319`), all outside the factory filter `0x9083a2b6...`.

### C11 — OmenAgentResultMapping chain-retirement
- `SELECT max(block_timestamp), count() FROM contracts_OmenAgentResultMapping_events / _calls` -> both `max=2025-08-06T20:27:40`; events `65,919` rows, calls `46,124` rows.
- On-chain (`rpc_scan_logs` @ `0x260e1077dea98e738324a6cefb0ee9a272ed471a`): `5,705` logs up to block `41,473,484` (freeze ~`2025-08-06 20:30`); `0` logs blocks `41,473,512`->`42,500,000`; `0` logs May 2026 (`45.94M`-`46.2M`); `0` logs Jun 2026 (`46.8M`->latest).
- Grep `models/contracts/OmenAgentResultMapping/` + `schema.yml` meta -> no deprecation/`end_date` note.

### C12 — FPMM calls staleness
- `SELECT max(block_timestamp), count() FROM contracts_FPMMDeterministicFactory_calls vs _events` -> calls `max=2026-04-01T12:06:35` (`2,034` rows), events `max=2026-06-21T03:56:30` (`21,645` rows). Staleness `81` days. Calls `incremental_strategy='append'` (line 4). `.gap_refresh_state.json` `completed` lists `_events`, not `_calls`.

### C13 — data terminus
- `get_downstream_impact(contracts_ConditionalTokens_events)` -> `1` consumer = own `dbt_utils_unique_combination_of_columns` test.
- Grep `ref()/source()` to all 6 PM prefixes across `models/` (excluding own dirs) -> `0`.

### C14 — v1/v2 schema incompatibility
- `SELECT arrayStringConcat(mapKeys(decoded_params),',')` per table: v1 (`0x260e1077`) keys `marketAddress, estimatedProbabilityBps (scalar), publisherAddress, txHashes, ipfsHash`; v2 (`0x99c43743`) keys `marketAddress, publisherAddress, outcomes (array), estimatedProbabilitiesBps (array), txHashes, ipfsHash`.
- schema.yml: Omen events `result_id, result_value, result_timestamp, additional_data`; ARM events `agent_result_mapping_id, agent_address, result_status, result_value, event_data` (only `result_value` shared).

### C15 — Seer pre-deployment window
- `SELECT countIf(block_timestamp < toDateTime('2024-10-08')) pre_seer, count() FROM contracts_Seer_Wrapped1155Factory_events` -> `212` of `2,347` (`9.0%`).
- Code: `start_blocktime='2024-02-07'` (line 23); SeerPM MarketFactory `start_date='2024-10-08'`.

## Review log (>=3 rounds per case)

- **C01**: R1 CONFIRMED high (code key + 0 dup-tx, latent) -> challenge: run on-chain GROUP BY for any tx with >1 Wrapped1155Creation -> R2 CONFIRMED high (data-side re-confirmed, on-chain deferred) -> challenge: ONE bounded `rpc_scan_logs` to decide latent vs active -> R3 CHANGED high (`3,230` logs / `892` tx, all multi-event, max `29`/tx => actively collapsing ~`72%`).
- **C02**: R1 CONFIRMED high (silent-drop path) -> challenge: prove no compensating monitor -> R2 CONFIRMED high (no post-runner freshness/Slack gate) -> challenge: quote cron exit-code handling -> R3 CONFIRMED high (`cron_preview.sh` execs orchestrator, empty-slice path returns 0, nothing catches it).
- **C03**: R1 CONFIRMED high (literals + 0 vs 2,865 rows) -> challenge: on-chain calls in Jun 23-29 -> R2 CONFIRMED high (events prove contract activity, gap is configured cutoff) -> challenge: confirm bounded window + fix actionability -> R3 CONFIRMED high (calls min `2025-06-30`, 0 before; append => bounded backfill, not incident-A).
- **C04**: R1 CONFIRMED medium (line 146 bare equality) -> challenge: prove source is 0x-stripped lowercase today -> R2 CONFIRMED medium (code re-confirmed, source query deferred) -> challenge: run the source-format query -> R3 CONFIRMED medium (`3.67M` logs, `mixedcase=0`, `prefixed=0`; latent).
- **C05**: R1 CONFIRMED medium (toStartOfMonth, 68 months) -> challenge: confirm append + dated trigger -> R2 CONFIRMED medium (append, ~Jan 2029 trigger) -> challenge: confirm full-refresh failure mode (single vs batched INSERT) -> R3 CHANGED low (refresh.py batches by 6 months, cap never fires on supported path).
- **C06**: R1 CONFIRMED medium (tx-level dedup, traces unused) -> challenge: quantify blast radius -> R2 CONFIRMED medium but INSUFFICIENT (materiality unmeasured) -> challenge: measure internal-vs-tx-level magnitude -> R3 CONFIRMED medium (set-difference: `29,696`/`30,231` = `98.2%` absent).
- **C07**: R1 CONFIRMED medium (stubs vs describe_table) -> challenge: prove consumer reach -> R2 CONFIRMED medium (yml/manifest consumers vs live catalog) -> challenge: run `get_relevant_columns` -> R3 CONFIRMED medium (returns stub set, reaches LLM/API path).
- **C08**: R1 CONFIRMED low (7/2034) -> challenge: extract selectors -> R2 CONFIRMED low (join timed out, mechanism inferred) -> challenge: tight inline-literal join -> R3 CONFIRMED low (single selector `0x23f66e47` missing from seed).
- **C09**: R1 CONFIRMED low (0 tags) -> challenge: verify guard selection -> R2 CONFIRMED low (guard skips api-untagged) -> challenge: convention require internal annotation? -> R3 CONFIRMED low (no required internal annotation; sub-claim informational).
- **C10**: R1 CONFIRMED critical (1 event_name) -> challenge: prove EIP-1167 clone mechanism -> R2 CONFIRMED critical (clone `0x7ec4f4c9...` 42 FPMMBuy on-chain) -> challenge: quantify blast radius -> R3 CONFIRMED critical (`21,645` markets, 0 trading rows across all PM models).
- **C11**: R1 CONFIRMED critical (319-day stale) -> challenge: halt vs chain-silence -> R2 CHANGED low (0 on-chain logs after 2025-08-06 = chain-retirement) -> challenge: grep for deprecation note -> R3 CHANGED low (no deprecation note; current-with-chain but undocumented).
- **C12**: R1 CONFIRMED high (calls frozen 2026-04-01) -> challenge: link to C02 + watermark -> R2 CONFIRMED high (81 slices >> 30; gap_refresh_state lists events not calls) -> challenge: confirm fix actionable/safe -> R3 CONFIRMED high (append => bounded backfill, not incident-A).
- **C13**: R1 CONFIRMED high (grep 0 refs) -> challenge: check compiled child_map -> R2 CONFIRMED high (grep 0, child_map inference) -> challenge: call `get_downstream_impact` -> R3 CONFIRMED high (only consumer is own schema test).
- **C14**: R1 CONFIRMED medium (distinct contracts/schemas) -> challenge: prove decoded_params incompatibility -> R2 CONFIRMED medium (v1 scalar vs v2 array keys) -> challenge: reconcile "identical names" wording -> R3 CONFIRMED medium (names DIFFERENT per file, only `result_value` shared; payloads incompatible).
- **C15**: R1 CONFIRMED low (212 pre-Seer rows) -> challenge: sample rows for Seer references -> R2 CONFIRMED low (deploy date corroborated, sampling deferred) -> challenge: confirm rows are noise -> R3 CONFIRMED low (deploy-date ordering dispositive; 9.0% noise).

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P0 (ESCALATE) | Build the warehouse trading/LP/price layer off per-market EIP-1167 clone addresses (from `decoded_params['fixedProductMarketMaker']`) so Omen trading analytics is not zero; the factory-address filter can never see FPMMBuy/Sell/Funding. | `models/contracts/FPMMDeterministicFactory/contracts_FPMMDeterministicFactory_events.sql` (C10) |
| P0 (KEEP) | Fix the runner silent-drop: make `dbt_incremental_runner.py` raise / exit non-zero (or emit a Slack/monitor alert) when a stage exceeds `max_slices_per_stage`, instead of appending an empty slice and returning 0. | `scripts/refresh/dbt_incremental_runner.py` (C02, root cause of C12) |
| P1 (KEEP) | Backfill the FPMM calls 81-day gap: `python scripts/full_refresh/refresh.py --select contracts_FPMMDeterministicFactory_calls --incremental-only` (append-only, safe, not incident-A). | `contracts_FPMMDeterministicFactory_calls.sql` (C12) |
| P1 (KEEP) | Align `contracts_AgentResultMapping_calls` `start_blocktime` to `2025-06-10` and backfill the Jun 10-29 2025 window (append, bounded). | `contracts_AgentResultMapping_{calls,events}.sql` (C03) |
| P1 (KEEP) | Change the Wrapped1155Factory RMT `order_by`/`unique_key` to include `log_index`; multi-event tx are actively collapsing (~72% loss in the recent window), then re-materialize. | `contracts_Seer_Wrapped1155Factory_events.sql` (C01) |
| P2 (KEEP) | Decide ConditionalTokens calls completeness: either re-point the calls model at `execution.traces` or document that the calls layer only captures top-level tx (`98.2%` of CT-interacting tx are internal). | `macros/decoding/decode_calls.sql`, `contracts_ConditionalTokens_calls.sql` (C06) |
| P2 (KEEP) | Correct all 6 PM `schema.yml` column lists to the real `decode_calls`/`decode_logs` output (the stubs reach `get_relevant_columns` / LLM consumers despite `authoritative:false`). | all 6 PM `schema.yml` (C07) |
| P2 (KEEP) | Add `lower(replaceAll(...))` normalization to the `decode_logs` single-address branch (line 146) for parity with the multi/ref paths (latent; safe only by current source convention). | `macros/decoding/decode_logs.sql` (C04) |
| P3 (KEEP) | Add a unioning intermediate model giving a consistent prediction view across OmenARM v1 (scalar prob) and ARM v2 (array prob + outcomes); resolve the v1/v2 payload incompatibility. | OmenARM + ARM events (C14) |
| P3 (KEEP) | Add `api:/granularity:/window:/tier:` tags (or an explicit internal annotation) before any of the 10 PM models is promoted to an API endpoint, since the CI guard skips untagged models. | all 10 PM models (C09) |
| P3 (DOWNGRADED, monitor) | Partition-cap is no longer urgent: standard `refresh.py` batches by 6 months so the 100/252 cap is never hit on the supported path. Only relevant if someone runs a raw un-batched `dbt run --full-refresh`. | 6 ConditionalTokens/FPMM/Realitio models (C05) |
| P3 (KEEP, doc-only) | Document the OmenAgentResultMapping v1 chain-retirement (last on-chain emission `2025-08-06`) with a deprecation note / `end_date` in the model dir or `schema.yml` meta so the `319`-day "staleness" reads as intentional. | OmenAgentResultMapping events/calls (C11) |
| P4 (KEEP, optional) | Add ABI selector `0x23f66e47` to the `function_signatures` seed to clear the 7 null-`function_name` rows. | `contracts_FPMMDeterministicFactory_calls.sql` (C08) |
| P4 (KEEP, optional) | Consider raising the Seer Wrapped1155 `start_blocktime` to `2024-10-08` (or filtering pre-Seer rows in downstream joins) to drop the `9.0%` unrelated ERC-1155 noise. | `contracts_Seer_Wrapped1155Factory_events.sql` (C15) |
