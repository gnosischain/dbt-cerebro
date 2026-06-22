# Model review (revisit 2026-06-21): execution/GBCDeposit

Re-verification of the baseline at `docs/model_review/execution-gbcdeposit.md` (dated 2026-06-11) over 3 rounds: of `16` cases, `1` resolved (3-day staleness), `0` net-changed, and `15` still confirmed — the highest-severity defects (raw-Gwei `~1e9x`-inflated approved metric, unguarded BLS address derivation polluting `45.8%` of graph nodes) remain fully open.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| `EXECUTIONGBCDEPOSIT-C01` | — | contracts schema.yml documents phantom columns the decode macros never emit | high | CONFIRMED | high | high | none | 3 |
| `EXECUTIONGBCDEPOSIT-C02` | — | `apply_monthly_incremental_filter` on a `materialized='view'` (latent) | high | CONFIRMED | medium | high | none | 3 |
| `EXECUTIONGBCDEPOSIT-C03` | — | no uniqueness test on `(date, withdrawal_credentials)` grain | medium | CONFIRMED | medium | high | none | 3 |
| `EXECUTIONGBCDEPOSIT-C04` | — | `contracts_GBCDeposit_calls` orphaned, built every cron | medium | CONFIRMED | medium | high | none | 3 |
| `EXECUTIONGBCDEPOSIT-C05` | — | data 3 days stale; freshness test severity:warn | low | RESOLVED | resolved | high | none | 3 |
| `EXECUTIONGBCDEPOSIT-C06` | — | `deposists` typo propagated across 6 artifacts incl. served metric | low | CONFIRMED | low | high | none | 3 |
| `EXECUTIONGBCDEPOSIT-C07` | P0-11 | address entity derived w/o `0x01` type guard (BLS pollution) | high | CONFIRMED | high | high | none | 3 |
| `EXECUTIONGBCDEPOSIT-C08` | P0-11 | raw uint64 Gwei summed, no `/1e9`; `~1e9x` inflated approved metric; `wei` mislabel | high | CONFIRMED | high | high | none | 3 |
| `EXECUTIONGBCDEPOSIT-C09` | — | no mart/reconciliation; two independent deposit pipelines, no authority | medium | CONFIRMED | medium | high | none | 3 |
| `EXECUTIONGBCDEPOSIT-C10` | — | `quality_tier: approved` on a self-described auto-generated candidate | low | CONFIRMED | low | high | none | 3 |
| `EXECUTIONGBCDEPOSIT-C11` | — | events row count / grain / freshness descriptive | low | CONFIRMED | low | high | none | 3 |
| `EXECUTIONGBCDEPOSIT-C12` | — | intermediate row count / nulls / freshness descriptive | low | CONFIRMED | low | high | none | 3 |
| `EXECUTIONGBCDEPOSIT-C13` | — | amount hex decodes little-endian uint64 Gwei | low | CONFIRMED | low | high | none | 3 |
| `EXECUTIONGBCDEPOSIT-C14` | P0-11 | grain unique; `45.8%` rows are 0x00-type BLS credentials | high | CONFIRMED | high | high | none | 3 |
| `EXECUTIONGBCDEPOSIT-C15` | — | RMT dedup clean (FINAL == non-FINAL) | low | CONFIRMED | low | high | none | 3 |
| `EXECUTIONGBCDEPOSIT-C16` | — | 5 non-DepositEvent rows present, correctly filtered | low | CONFIRMED | low | high | none | 3 |

No NEW cases.

## Delta vs baseline

**RESOLVED (1)**

- `EXECUTIONGBCDEPOSIT-C05` — the 3-day staleness is gone. `max(date)` advanced from `2026-06-08` (3 missed runs at baseline) to `2026-06-21` (today), with the last 90 days fully contiguous: `distinct_days=91 == span_days=91`, zero gaps. The append decode caught up and is producing daily rows reliably. Resolved on **symptom only** — the underlying control is unchanged: `elementary.freshness_anomalies` in `models/execution/GBCDeposit/intermediate/schema.yml` (L61-68) is still `severity: warn`, so a future cron stall would not fail CI. No incident attribution (the table was never month-collapsed; this was ordinary cron lag).

**CHANGED (0)**

- `C11`, `C12`, `C16` were flagged CHANGED in Round 1 (forward-ingest growth + a shift in the non-DepositEvent event mix), but stabilized to CONFIRMED in Rounds 2-3 once the before/after deltas were attributed as pure forward appends. No net-changed cases at sector close.

**STILL CONFIRMED (15)**

High-severity, business-impacting:

- `EXECUTIONGBCDEPOSIT-C08` (high) — `models/execution/GBCDeposit/intermediate/int_GBCDeposit_deposists_daily.sql` L12 is `SUM(reinterpretAsUInt64(unhex(substring(decoded_params['amount'],3))))` with **no `/1e9`**. June 2026 raw sum = `552,009,832,341,274` Gwei (`~5.52e14`) = `552,009.83` GNO after `/1e9`, i.e. `~1e9x` the consensus pipeline's GNO figure (`551,948.26` GNO, which divides by 1e9). The metric `GBCDeposit_deposists_daily__amount_value` is `agg: sum` over `expr: amount`, so the served value mechanically equals the raw-Gwei sum. `models/contracts/GBCDeposit/schema.yml` doubly mislabels the unit as `wei` (calls L29, events L81).
- `EXECUTIONGBCDEPOSIT-C07` (high) — `semantic/authoring/execution/GBCDeposit/semantic_models.yml` derives the `address` entity (L9), `withdrawal_address` dimension (L21) and graph `source_column` (L39) as `concat('0x', substring(withdrawal_credentials, 27, 40))` with **no `0x01` type guard**. Canonical `int_consensus_validators_withdrawal_addresses.sql` (L17-20) uses `CASE WHEN startsWith(withdrawal_credentials,'0x01') THEN concat(...) ELSE NULL`. `6,667` of `14,563` rows (`45.8%`) are 0x00-type BLS credentials, plus `969` unguarded 0x02 (EIP-7251) — `7,636` meaningless address nodes on the approved `deposit_to_validator` graph profile.
- `EXECUTIONGBCDEPOSIT-C14` (high) — grain `(date, withdrawal_credentials)` confirmed unique (`uniqExact = count = 14,563`); credential split `0x00=6,667 (45.8%)`, `0x01=6,927`, `0x02=969`. The 0x00 pollution is structural (spans `2021-12-01`..`2026-04-26`) and economically material: 0x00-type rows carry `189,088` GNO in 2021-2023 alone.

Medium:

- `EXECUTIONGBCDEPOSIT-C02` (medium, lowered from high) — `int_GBCDeposit_deposists_daily.sql` is `materialized='view'` (L3) with `apply_monthly_incremental_filter(...)` inline in the WHERE (L16). Gating lives in the macro (`macros/db/get_incremental_filter.sql` wraps its body in `{% if is_incremental() %}` L24-84), so it emits nothing on a view. Latent only — concrete failure if flipped to `incremental` without `engine`/`partition_by`/`unique_key` (config has none).
- `EXECUTIONGBCDEPOSIT-C03` (medium) — intermediate `schema.yml` tests only `not_null` on `date`; no `dbt_utils.unique_combination_of_columns` on `(date, withdrawal_credentials)`. Grain is clean today (`dup_grain=0`) only because the model's `GROUP BY 1,2` enforces it; nothing guards a future double-aggregation regression that would inflate the served sum measure.
- `EXECUTIONGBCDEPOSIT-C04` (medium) — `contracts_GBCDeposit_calls` has zero `ref()` outside its own `schema.yml`; it holds `15,082` rows spanning `2021-12-02`..`2026-06-21` (4.5 yr) and is rebuilt/appended every cron run (ReplacingMergeTree, append strategy) with no downstream value.
- `EXECUTIONGBCDEPOSIT-C09` (medium) — no mart/`api_*` model and no reconciliation against `int_consensus_deposits_withdrawals_daily`. The two pipelines match closely on June GNO (`552,009.83` vs `551,948.26`, `0.011%` delta) but diverge structurally in grain/count; `99.9%` of June volume (`551,465.83`/`552,009.83` GNO) is in the un-reconciled EIP-7251 0x02 path.

Low (documentation / descriptive / governance):

- `EXECUTIONGBCDEPOSIT-C01` (low impact, high sev kept) — `models/contracts/GBCDeposit/schema.yml` documents phantom columns (events: `depositor_address`/`deposit_amount`/`deposit_token`/`deposit_timestamp`/`event_type`/`transaction_fee`/`status`; calls: `sender`/`receiver`/`amount`/`status`). `describe_table` returns the decode JSON shape (`event_name`/`function_name` + `decoded_params`/`decoded_input` Map). Blast radius bounded to documentation-only: grep for the phantom column names against `contracts_GBCDeposit_*` refs = zero consumer hits.
- `EXECUTIONGBCDEPOSIT-C06` (low) — `deposists` typo persists in all six artifacts and on the queryable MCP surface (`discover_metrics` returns metric name `GBCDeposit_deposists_daily__amount_value`, label `Gbcdeposit Deposists Daily - Amount`).
- `EXECUTIONGBCDEPOSIT-C10` (low) — `semantic_models.yml` has `quality_tier: approved` (L57) alongside description "Auto-generated candidate metric; review and promote before relying on it." (L49); `discover_metrics` confirms `quality_tier=approved` on the served registry; no CI guard enforces tier-vs-description consistency.
- `EXECUTIONGBCDEPOSIT-C11` (low) — `contracts_GBCDeposit_events`: `562,552` rows (`562,290` at baseline, `+262` all forward appends), grain-unique on `(block_timestamp, log_index)`, `max = 2026-06-21`. Grain test in contracts `schema.yml` (L114-119) actively scans the recent 7-day window in CI.
- `EXECUTIONGBCDEPOSIT-C12` (low) — `int_GBCDeposit_deposists_daily`: `14,563` rows (`14,513` baseline, `+50`), `0` null dates, `max(date)=2026-06-21`, June 1-21 contiguous. `+47` rows after `2026-06-08` (forward) + `~3` late-arriving historical rows; no destructive rewrite.
- `EXECUTIONGBCDEPOSIT-C13` (low) — canonical `0x0040597307000000` little-endian uint64 = `32,000,000,000` Gwei = 32 GNO; independently confirmed via `contract_decode_receipt_logs` on a fresh tx (`0x6975045f02000000` = `10,184,062,313` Gwei, an EIP-7251 0x02 variable-amount deposit).
- `EXECUTIONGBCDEPOSIT-C15` (low) — `count() = count() FINAL = uniqExact(block_timestamp,log_index) = 562,552`, `dup_keys=0`; RMT dedup clean on the actual ORDER BY key `(block_timestamp, log_index)`.
- `EXECUTIONGBCDEPOSIT-C16` (low) — `562,547` DepositEvent + `5` non-DepositEvent rows (proxy admin events); intermediate L14-15 `WHERE event_name='DepositEvent'` excludes them — zero data impact.

**NEW (0)** / **UNVERIFIABLE or UNRESOLVED (0)**

## Evidence appendix

**C01 — phantom schema columns** (`code_only` / `describe_table`)
- `describe_table(contracts_GBCDeposit_events)` → `block_number, block_timestamp, transaction_hash, transaction_index, log_index, contract_address, event_name, decoded_params Map(String,Nullable(String))`; `decoded_params` keys = `[pubkey, withdrawal_credentials, amount, signature, index]`.
- `describe_table(contracts_GBCDeposit_calls)` → `block_number, block_timestamp, transaction_hash, contract_address, nonce, gas_price, value, function_name, decoded_input Map`; `decoded_input` keys = `[pubkeys, withdrawal_credentials, signatures, deposit_data_roots, amounts]`.
- `models/contracts/GBCDeposit/schema.yml` documents events L76-102 and calls L20-34 phantom columns; `decode_logs.sql` final SELECT L562-571 emits exactly the 8-column shape. grep of `models/` + `semantic/` for phantom column names against GBCDeposit refs = 0 consumer hits.

**C02 — incremental filter on a view** (`code_only`)
- `int_GBCDeposit_deposists_daily.sql` L2-6 config = `materialized='view'`, tags only (no `partition_by`/`engine`/`unique_key`/`incremental_strategy`). L14-16: `WHERE event_name='DepositEvent' {{ apply_monthly_incremental_filter(source_field='block_timestamp', destination_field='date', add_and=true) }}`.
- `macros/db/get_incremental_filter.sql` L24 wraps the entire body in `{% if is_incremental() %}` ... L84 `{% endif %}`; else-path L63-82 emits self-referencing `{{this}}` watermark subqueries. A flip to incremental without engine/partition_by would build with the dbt-clickhouse default.

**C03 — missing grain test** (`code_only` / `sql`)
- `SELECT count() FROM (SELECT date, withdrawal_credentials, count() c FROM int_GBCDeposit_deposists_daily GROUP BY 1,2 HAVING c>1)` → `0`. `count = uniqExact(date,withdrawal_credentials) = 14,563`.
- intermediate `schema.yml` L11-14 = only `not_null` on `date`; no `unique_combination_of_columns`. Served measure `semantic_models.yml` L22-24 = `agg: sum` / `expr: amount` over grain.

**C04 — orphaned calls model** (`sql` / grep)
- `SELECT count(), min(toDate(block_timestamp)), max(toDate(block_timestamp)) FROM contracts_GBCDeposit_calls` → `15,082`, `2021-12-02`, `2026-06-21`.
- `grep -rn contracts_GBCDeposit_calls models/ semantic/` (excl `.claude/worktrees`) → only `models/contracts/GBCDeposit/schema.yml` L4-5. `execution_graph.yml` L68 left_model and `semantic_models.yml` ref only the deposists intermediate.

**C05 — freshness** (`sql`)
- `SELECT max(toDate(date)) FROM int_GBCDeposit_deposists_daily` → `2026-06-21`.
- Last 90d (`>= 2026-03-23`): `distinct_days = 91`, `span_days = 91` (zero gaps). `schema.yml` L61-68 `freshness_anomalies severity: warn` UNCHANGED.

**C06 — typo propagation** (`code_only` / `discover_metrics`)
- `grep -rln deposists models/ semantic/` → filename `int_GBCDeposit_deposists_daily.sql`; intermediate `schema.yml` model name; `semantic_models.yml` semantic model name + metric name (L23/L47) + question_synonyms (L33/L71); `execution_graph.yml` L68 left_model.
- `discover_metrics` → served name `GBCDeposit_deposists_daily__amount_value`, label `Gbcdeposit Deposists Daily - Amount`, `quality_tier=approved`.

**C07 / C14 — address derivation + credential breakdown** (`sql` / `code_only`)
- `SELECT substring(withdrawal_credentials,1,4) AS prefix, count() ... GROUP BY prefix` → `0x01=6,927`, `0x00=6,667 (45.8%)`, `0x02=969`. `uniqExact(date,withdrawal_credentials) = count = 14,563`.
- 0x00 date span `2021-12-01`..`2026-04-26`; `sumIf(amount,startsWith('0x00'))/1e9` for 2021-2023 = `189,088` GNO.
- `semantic_models.yml` L9/L21/L39 = `concat('0x', substring(withdrawal_credentials, 27, 40))`, no guard; canonical `int_consensus_validators_withdrawal_addresses.sql` L17-20 = `CASE WHEN startsWith(...,'0x01') ... ELSE NULL`.

**C08 / C13 — Gwei denomination** (`sql` / `decode`)
- `SELECT round(sum(amount)/1e9,2), sum(amount) FROM int_GBCDeposit_deposists_daily WHERE toDate(date)>=toDate('2026-06-01')` → `552,009.83`, `552,009,832,341,274`.
- consensus June `Deposits` = `551,948.26` GNO; ratio `~1e9x`. `int ... .sql` L12 = `SUM(reinterpretAsUInt64(...))` no `/1e9`; `schema.yml` calls L29 / events L81 unit = `wei`.
- `reinterpretAsUInt64(unhex(substring('0x0040597307000000',3)))` = `32,000,000,000`. `contract_decode_receipt_logs` on tx `0x1dce8a28...44319d`: amount bytes `0x69 75 04 5f 02 00 00 00` little-endian = `10,184,062,313` Gwei; matches ClickHouse.

**C09 — pipeline divergence** (`sql`)
- June GBC = `552,009.83` GNO across `80` `(date,credential)` rows; consensus `Deposits` = `551,948.26` GNO across `512` deposits; GNO delta `~61.6` (`0.011%`); count grain differs.
- 0x02 (EIP-7251) June = `551,465.83`/`552,009.83` GNO = `99.9%`. No mart/`api_*`/reconciliation model exists.

**C10 — tier overstatement** (`code_only` / `discover_metrics`)
- `semantic_models.yml` L57 `quality_tier: approved`; L49 description "Auto-generated candidate metric; review and promote before relying on it."; `discover_metrics` confirms `quality_tier=approved`. No CI guard found in `scripts/`/`.github` enforcing tier-vs-description.

**C11 / C15 — events count + RMT dedup** (`sql`)
- `count = 562,552`, `count FINAL = 562,552`, `uniqExact(block_timestamp,log_index) = 562,552`, `dup_keys = 0`, `max(block_timestamp) = 2026-06-21 00:38:55`. Rows post-`2026-06-08` = `256`. Model config L5-7: `engine='ReplacingMergeTree()'`, `order_by/unique_key='(block_timestamp, log_index)'`. Grain test in `schema.yml` L114-119 covers `toDate(block_timestamp) >= today() - 7`.

**C12 — intermediate count** (`sql`)
- `count = 14,563`, `countIf(date IS NULL) = 0`, `max(date) = 2026-06-21`, `countIf(date > 2026-06-08) = 47`, `countIf(date <= 2026-06-08) = 14,516` (vs baseline total `14,513`). June 1-21 = 21/21 days.

**C16 — non-DepositEvent rows** (`sql`)
- `countIf(event_name='DepositEvent') = 562,547`, `countIf(event_name != 'DepositEvent') = 5`. Intermediate L14-15 `WHERE event_name='DepositEvent'`. Baseline mix (AdminChanged, Upgraded, Paused, Unpaused) → now Upgraded `3` + AdminChanged `2` (one-time proxy/admin lifecycle events, never re-emitted daily).

## Review log (>=3 rounds per case)

- `C01` — R1 CONFIRMED (schema.yml vs `describe_table` events) → challenge: corroborate from `_calls` angle + confirm Map type → R2 CONFIRMED (calls describe_table; `decoded_input` keys; Map(String,Nullable(String))) → challenge: prove documentation-only blast radius → R3 CONFIRMED (grep phantom names = 0 consumer hits). Final high.
- `C02` — R1 CONFIRMED (view + macro call) → challenge: gating is in the macro not the model; lower to medium → R2 CONFIRMED (macro body wrapped in is_incremental) → challenge: show concrete failure-if-flipped → R3 CONFIRMED (else-path watermark output quoted). Final medium.
- `C03` — R1 CONFIRMED (no test in schema.yml) → challenge: prove regression real (dup probe) → R2 CONFIRMED (`dup_grain=0`, grain unique) → challenge: tie to served measure → R3 CONFIRMED (measure agg:sum over grain). Final medium.
- `C04` — R1 CONFIRMED (grep no consumer) → challenge: confirm materialized/rebuilt + re-grep semantic → R2 CONFIRMED (`15,078` rows, max 2026-06-20) → challenge: partition-spanning count since SYSTEM blocked → R3 CONFIRMED (`15,082`, 2021-12-02..2026-06-21). Final medium.
- `C05` — R1 RESOLVED (max=2026-06-21, June contiguous) → challenge: prove durable, note severity:warn unchanged → R2 RESOLVED (no source-view lag; 21/21 days) → challenge: 90-day gap check → R3 RESOLVED (91/91 days; control unchanged). Final resolved.
- `C06` — R1 CONFIRMED (grep 6 artifacts) → challenge: confirm typo on served registry → R2 CONFIRMED (`get_metric_details` returns typo'd name) → challenge: retry via discover_metrics → R3 CONFIRMED (discover_metrics literal name/label). Final low.
- `C07` — R1 CONFIRMED (no guard; 0x00=45.8%) → challenge: show pollution vs canonical guard on a real row → R2 CONFIRMED (canonical CASE/startsWith quoted; 7,636 polluted) → challenge: concrete 0x00 row side-by-side → R3 CONFIRMED (substring slices BLS hash; canonical NULLs it). Final high.
- `C08` — R1 CONFIRMED (no /1e9; min=1e9; wei label) → challenge: cross-pipeline `~1e9x` quantification → R2 CONFIRMED (June 552,009.83 vs 551,948.26) → challenge: confirm served metric returns raw Gwei → R3 CONFIRMED (measure mechanically = table sum; manifest_hash_mismatch on query_metrics is environmental). Final high.
- `C09` — R1 CONFIRMED (no mart/reconciliation) → challenge: prove pipelines diverge for a window → R2 CONFIRMED (0.011% GNO match, count grain differs) → challenge: break out EIP-7251 share → R3 CONFIRMED (99.9% of June in 0x02 path). Final medium.
- `C10` — R1 CONFIRMED (approved + candidate caveat in yml) → challenge: confirm via served registry → R2 CONFIRMED (`get_metric_details` both surface) → challenge: check for CI guard → R3 CONFIRMED (no tier-promotion guard exists; systemic). Final low.
- `C11` — R1 CHANGED (`562,290`→`562,552`, max advanced) → challenge: confirm grain under FINAL + all forward → R2 CONFIRMED (FINAL==count; +262 forward) → challenge: confirm CI grain test scope → R3 CONFIRMED (test covers recent 7d). Final low.
- `C12` — R1 CHANGED (`14,513`→`14,563`, +50) → challenge: attribute +50 to forward aggregation → R2 CONFIRMED (`+47` after 06-08) → challenge: confirm no historical rewrite → R3 CONFIRMED (only +3 at/before 06-08, view recompute). Final low.
- `C13` — R1 CONFIRMED (canonical hex = 32e9) → challenge: decode on fresh live row → R2 CONFIRMED (fresh rows decode to sane GNO Gwei) → challenge: independent on-chain decode → R3 CONFIRMED (`contract_decode_receipt_logs` matches). Final low.
- `C14` — R1 CONFIRMED (grain unique; 0x00=6,667 45.8%) → challenge: show 0x00 spans full history → R2 CONFIRMED (2021-12-01..2026-04-26) → challenge: quantify GNO value of 0x00 → R3 CONFIRMED (189,088 GNO 2021-2023). Final high.
- `C15` — R1 CONFIRMED (FINAL==non-FINAL) → challenge: probe dup keys pre-merge → R2 CONFIRMED (`dup_keys=0`) → challenge: ground dedup key in actual ORDER BY → R3 CONFIRMED (config `(block_timestamp, log_index)`). Final low.
- `C16` — R1 CHANGED (5 rows, mix shifted Paused/Unpaused absent) → challenge: confirm filtered + explain mix shift → R2 CONFIRMED (5 rows filtered; one-time admin events) → R3 CONFIRMED (562,547 + 5; filter present). Final low.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P1 | ESCALATE: divide amount by `1e9` (Gwei→GNO) in the model and/or measure; fix `schema.yml` unit label from `wei` to `GNO`. Until fixed, the `~1e9x`-inflated approved metric must not be served. | `models/execution/GBCDeposit/intermediate/int_GBCDeposit_deposists_daily.sql`, `semantic/authoring/execution/GBCDeposit/semantic_models.yml`, `models/contracts/GBCDeposit/schema.yml` |
| P1 | ESCALATE: add a `0x01` type guard (`CASE WHEN startsWith(withdrawal_credentials,'0x01') ... ELSE NULL`) to the address entity, withdrawal_address dimension and graph source_column, mirroring `int_consensus_validators_withdrawal_addresses`; decide handling for 0x02 (EIP-7251). Removes 7,636 meaningless nodes from the approved graph. | `semantic/authoring/execution/GBCDeposit/semantic_models.yml` |
| P2 | KEEP: downgrade `quality_tier` from `approved` to `candidate`/`development` until C07/C08 are fixed; add a CI guard rejecting `approved` tier on descriptions containing "auto-generated candidate". | `semantic/authoring/execution/GBCDeposit/semantic_models.yml` |
| P2 | KEEP: add `dbt_utils.unique_combination_of_columns` on `(date, withdrawal_credentials)` to the intermediate schema to protect the served sum measure from double-aggregation regressions. | `models/execution/GBCDeposit/intermediate/schema.yml` |
| P2 | KEEP: build a reconciliation model (or mart) against `int_consensus_deposits_withdrawals_daily` and document which deposit pipeline is authoritative; cover the EIP-7251 0x02 path (99.9% of current volume). | `models/execution/GBCDeposit/` (new mart), `int_consensus_deposits_withdrawals_daily` |
| P3 | KEEP: drop or document `contracts_GBCDeposit_calls` — orphaned, 15,082 rows / 4.5 yr rebuilt every cron with zero downstream ref. | `models/contracts/GBCDeposit/contracts_GBCDeposit_calls.sql` |
| P3 | KEEP: remove the latent `apply_monthly_incremental_filter` call from the view, or add `engine`/`partition_by`/`unique_key` before any future flip to incremental. | `models/execution/GBCDeposit/intermediate/int_GBCDeposit_deposists_daily.sql` |
| P3 | KEEP: fix the contracts `schema.yml` to document the real decode output shape (`event_name`/`function_name` + `decoded_params`/`decoded_input` Map), removing phantom columns (documentation-only, no broken consumers). | `models/contracts/GBCDeposit/schema.yml` |
| P3 | KEEP: rename `deposists` → `deposits` across the 6 artifacts (filename, schema model name, semantic model/metric/synonyms, graph left_model) to fix MCP discoverability. | `int_GBCDeposit_deposists_daily.sql`, intermediate `schema.yml`, `semantic_models.yml`, `semantic/relationships/execution_graph.yml` |
| P4 | KEEP: raise `freshness_anomalies` from `severity: warn` to `error` on the intermediate so a future cron stall fails CI (symptom resolved, control unchanged). | `models/execution/GBCDeposit/intermediate/schema.yml` |
| — | DROP: no standalone remediation needed for the staleness symptom (C05) — data is current and 91/91 contiguous; only the warn-level control item (P4) remains. | — |
