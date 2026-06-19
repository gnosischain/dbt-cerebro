# Model review: execution/GBCDeposit

**Convergence:** converged in 1 round — inspector and context reports were mutually consistent on all material findings; the arbiter directly verified the load-bearing claims (fabricated schema.yml columns, dead incremental macro, BLS address extraction vs. canonical consensus guard, orphaned calls model, Gwei denomination) and closed every open question without disagreement.

---

## Scope and inventory

The unit tracks validator deposits to the Gnosis Beacon Chain (GBC) deposit contract. It contains three SQL models across two dbt packages:

| Layer | Model | Path |
|---|---|---|
| Contract decode (incremental) | `contracts_GBCDeposit_events` | `models/contracts/GBCDeposit/` |
| Contract decode (incremental) | `contracts_GBCDeposit_calls` | `models/contracts/GBCDeposit/` |
| Intermediate aggregation (view) | `int_GBCDeposit_deposists_daily` | `models/execution/GBCDeposit/intermediate/` |

There is no mart or `api_*` layer. The unit's only downstream surface is the semantic/graph layer. The `check_api_tags.py` CI guard never fires on this unit.

---

## Business context

**What it should measure.** GNO staking inflow to the GBC deposit contract, broken down by day and by `withdrawal_credentials` (the 32-byte field encoding the beneficiary validator's withdrawal intent). This feeds:

1. The semantic `deposit_to_validator` graph profile (quality tier: approved), enabling Graph Explorer to answer "which validators are funded by which deposit address?"
2. The `deposit_to_validator_identity` cross-sector relationship, joining to `int_consensus_validators_labels` on `withdrawal_credentials`.

Dashboard deposit counts and volumes (`api_consensus_deposits_withdrawls_cnt_daily`, `api_consensus_deposits_withdrawls_volume_daily`) are sourced from the consensus beacon-chain indexer (`stg_consensus__deposits`), NOT from this unit. The GBCDeposit execution-layer pipeline is used exclusively by the semantic/graph layer.

**Canonical definitions.**

- **DepositEvent** — on-chain event emitted by `SBCDepositContract` for each validator deposit. Fields: `pubkey` (48-byte BLS key), `withdrawal_credentials` (32 bytes), `amount` (little-endian uint64 Gwei), `signature`, `index`. Source contract: proxy `SBCDepositContractProxy` at `0x0B98057eA310F4d31F2a452B414647007d1645d9`, implementation `SBCDepositContract` at `0x49dE1aced385334F1a66d86Db363264eB5b6A708`.
- **withdrawal_credentials** — two formats: (a) `0x00`-prefix = BLS withdrawal key, no EVM address extractable; (b) `0x01`-prefix = last 20 bytes contain an EVM withdrawal address. The canonical model `int_consensus_validators_withdrawal_addresses` explicitly returns NULL for `0x00`-type credentials. The GBCDeposit semantic model does not apply this guard (see Business-logic assessment).
- **amount** — raw little-endian uint64 decoded as Gwei. One single-validator deposit = 32,000,000,000 (32 GNO at 1e9 Gwei-per-GNO). No `/1e9` conversion is applied anywhere in this unit. The consensus counterpart divides by 1e9 to report GNO. The intermediate schema.yml describes it as "unsigned 64-bit integer units"; the contracts schema.yml says "amount in wei" — both misleading.
- **History start** — `2021-12-01` (hardcoded `start_blocktime` in both contract decode models), consistent with GBC genesis.

---

## Implementation assessment

### HIGH — Both contracts_GBCDeposit schema.yml entries describe columns that do not exist

`models/contracts/GBCDeposit/schema.yml` documents `depositor_address`, `deposit_amount`, `deposit_token`, `deposit_timestamp`, `event_type`, `transaction_fee`, `status` for the events model and `sender`, `receiver`, `amount`, `status` for the calls model. The `decode_logs` and `decode_calls` macros with `output_json_type=true` emit `block_number`, `block_timestamp`, `transaction_hash`, `transaction_index`, `log_index`, `contract_address`, `event_name`/`function_name`, and a `decoded_params`/`decoded_input` JSON Map. None of the documented columns are produced. These descriptions appear auto-generated and were never reconciled against actual macro output. Any MCP or downstream consumer trusting the schema documentation is misled on column availability and semantics.

### HIGH — Dead `apply_monthly_incremental_filter` call in a view-materialized model

`models/execution/GBCDeposit/intermediate/int_GBCDeposit_deposists_daily.sql` (line 16) calls `apply_monthly_incremental_filter` inside a `{% if is_incremental() %}` block. The model is `materialized='view'`, so `is_incremental()` is always false and the macro emits nothing today. This is latent danger: if a developer changes the materialization to incremental without adding the required `partition_by`/`engine` settings, the macro will silently emit a whole-month self-referential filter against a non-existent self-reference and crash at runtime. The call should either be removed or accompanied with a comment noting it is inert pending a planned incremental conversion.

### MEDIUM — No uniqueness test on the intermediate `(date, withdrawal_credentials)` grain

`models/execution/GBCDeposit/intermediate/schema.yml` only tests `not_null` on `date`. The `(date, withdrawal_credentials)` grain feeds the semantic measure (sum of `amount`) and the graph relationship. A `dbt_utils.unique_combination_of_columns` test would catch any future double-aggregation regression before it reaches the approved-tier metric. Both contract decode models carry uniqueness tests with lookback windows; the intermediate does not.

### MEDIUM — `contracts_GBCDeposit_calls` is orphaned

A repo-wide search finds no `ref('contracts_GBCDeposit_calls')` outside the model's own definition. The model is an incremental ReplacingMergeTree built on every cron run, consuming compute and storage while producing no downstream value. Either a planned by-sender intermediate was never built or the model should be removed.

### LOW — 3-day staleness; `freshness_anomalies` severity is `warn`

At review time `max(date)` in `int_GBCDeposit_deposists_daily` = 2026-06-08 vs. today 2026-06-11 (3 missed daily cron runs). `elementary.freshness_anomalies` is configured `severity: warn`, so a stalled cron does not fail CI. Silent gaps are possible without an external alert.

### LOW — `deposists` misspelling propagated across six artifacts

The typo appears in the model filename (`int_GBCDeposit_deposists_daily.sql`), the intermediate `schema.yml` model name, the semantic model name, the metric name, the `execution_graph.yml` left-model reference, and `question_synonyms`. A coordinated rename across all six files is required; the misspelling degrades MCP metric discoverability.

---

## Business-logic assessment

### HIGH — Semantic entity emits garbage addresses for ~46% of rows (no 0x01 type guard)

`semantic/authoring/execution/GBCDeposit/semantic_models.yml` derives the primary `address` entity and `withdrawal_address` dimension as `concat('0x', substring(withdrawal_credentials, 27, 40))` with no type prefix guard. The substring offset (27, 40) is correct for `0x01`-type credentials. However, the platform's own canonical model `int_consensus_validators_withdrawal_addresses.sql` wraps the identical expression in `CASE WHEN startsWith(withdrawal_credentials, '0x01') ... ELSE NULL` because `0x00`-type BLS credentials have no EVM address. GBCDeposit omits this guard.

Warehouse confirmation: 6,667 of 14,513 daily rows (45.9%) carry `0x00`-type credentials. These produce plausible-looking but meaningless 20-byte hex strings as the `address` entity. Because the graph profile `deposit_to_validator` is quality tier: approved, these spurious address nodes flow into validator-ownership investigations served to MCP consumers. The data looks valid and is silently wrong.

**Fix:** mirror the canonical guard — `CASE WHEN startsWith(withdrawal_credentials, '0x01') THEN concat('0x', substring(withdrawal_credentials, 27, 40)) ELSE NULL END`.

### HIGH — Amount denomination is Gwei, undocumented and inconsistent with consensus counterpart

`int_GBCDeposit_deposists_daily.amount` is raw uint64 Gwei (32,000,000,000 per single-validator deposit). No `/1e9` conversion is applied. `int_consensus_deposits_withdrawals_daily` divides by 1e9 to report GNO. The approved-tier metric `GBCDeposit_deposists_daily__amount_value` sums the Gwei column directly, so a consumer charting it sees values approximately 1 billion times larger than every GNO-denominated figure elsewhere on the platform — with no documented reconciliation. The contracts `schema.yml` additionally mislabels the unit as "wei" (it is Gwei).

**Fix:** either divide by 1e9 in the intermediate and rename to `amount_gno`, or rename the column/measure to `amount_gwei` and document the conversion factor explicitly.

### MEDIUM — No mart layer; authoritative deposit source is ambiguous

The unit stops at the intermediate layer. Two independent deposit pipelines exist: this execution-layer `DepositEvent` source and the beacon-chain consensus source (`stg_consensus__deposits`, `stg_consensus__execution_requests`) that backs all `api_consensus_deposits_*` endpoints. There is no reconciliation model and no documented statement of which is authoritative. EIP-7251 consolidation deposits may appear in the consensus source but not in `DepositEvent`, creating a real divergence risk for anyone treating `GBCDeposit` as a deposit-volume source of truth.

### LOW — Approved-tier metric carries its own unreviewed caveat

The `GBCDeposit_deposists_daily__amount_value` metric has `quality_tier: approved` but its own description reads "Auto-generated candidate metric; review and promote before relying on it." An approved tier on a Gwei-denominated, BLS-address-polluted, auto-generated metric overstates trustworthiness to the MCP/semantic layer.

---

## Data findings

Five queries were run against the warehouse during the review:

| Query | Result |
|---|---|
| `contracts_GBCDeposit_events` row count, freshness, grain | 562,290 rows; unique on `(block_timestamp, log_index)`; `max` = 2026-06-08 |
| `int_GBCDeposit_deposists_daily` aggregate stats | 14,513 rows; 0 nulls on `date`; `max(date)` = 2026-06-08 |
| Raw amount hex vs. decoded uint64 | `0x0040597307000000` = 32,000,000,000 (little-endian); confirms Gwei denomination |
| Intermediate grain uniqueness + BLS/ETH1 breakdown | Grain confirmed unique; 6,667/14,513 rows (45.9%) are `0x00`-type BLS credentials |
| `contracts_GBCDeposit_events` FINAL count | Same as non-FINAL; ReplacingMergeTree deduplication is clean, no pending merges |

Additional: 5 non-`DepositEvent` rows present (`AdminChanged`, `Upgraded`, `Paused`, `Unpaused`). These are correctly filtered by `WHERE event_name = 'DepositEvent'` in the intermediate and have no data impact.

---

## Pros / Cons

**Pros**

- No critical correctness bug in live production rows: grain is unique, little-endian `reinterpretAsUInt64` decoding correctly matches SSZ encoding, and FINAL parity confirms clean RMT deduplication.
- Contract decode models are well-engineered: ReplacingMergeTree with correct order keys, monthly partitioning, incremental append strategy, and uniqueness tests with lookback windows.
- The `(date, withdrawal_credentials)` grain is the right shape for the operator-identity question the semantic graph asks.
- Sourcing is correct and well-anchored: proxy and implementation addresses verified against seeds; `DepositEvent` ABI confirmed; history starts at GBC genesis 2021-12-01.
- Elementary volume/freshness/schema/column anomaly tests are configured on the intermediate.
- The `substring(27, 40)` offset is correct and consistent with the canonical consensus convention — only the missing type guard is wrong.

**Cons**

- Approved-tier graph entity emits meaningless addresses for ~46% of rows (BLS credentials), unlike the canonical consensus model which NULLs them.
- Amount metric is in Gwei with no conversion, ~1e9x larger than every other GNO figure on the platform — a silent trap for consumers.
- Both contracts `schema.yml` entries are entirely fabricated; they describe columns the decode macros never produce.
- Pervasive `deposists` misspelling embedded across six artifacts — hurts MCP discoverability and requires a coordinated rename.
- `contracts_GBCDeposit_calls` is orphaned: built every cron run, referenced by nothing.
- No mart/API layer; only reachable via the semantic graph, so `check_api_tags` CI never fires and there is no REST fallback.
- No uniqueness test on the intermediate grain; no execution-vs-consensus reconciliation; view materialization will full-scan growing event history.
- Data was 3 days stale at review time; `freshness_anomalies` severity is `warn`, so a stalled cron does not fail CI.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| 1 | Add `0x01` type guard to semantic entity/dimension: `CASE WHEN startsWith(withdrawal_credentials,'0x01') THEN concat('0x', substring(withdrawal_credentials,27,40)) ELSE NULL END`, mirroring `int_consensus_validators_withdrawal_addresses.sql` | `semantic/authoring/execution/GBCDeposit/semantic_models.yml` |
| 2 | Resolve amount unit: divide by 1e9 in intermediate and rename to `amount_gno` (matching consensus counterpart), or rename to `amount_gwei` and document conversion factor; fix contracts `schema.yml` "wei" label to "Gwei" | `models/execution/GBCDeposit/intermediate/int_GBCDeposit_deposists_daily.sql`, `models/execution/GBCDeposit/intermediate/schema.yml`, `models/contracts/GBCDeposit/schema.yml` |
| 3 | Rewrite both contracts `schema.yml` entries to reflect actual decode macro output (`block_number`, `block_timestamp`, `transaction_hash`, `transaction_index`, `log_index`, `contract_address`, `event_name`/`function_name`, `decoded_params`/`decoded_input`) | `models/contracts/GBCDeposit/schema.yml` |
| 4 | Demote `GBCDeposit_deposists_daily__amount_value` from `quality_tier: approved` until items 1 and 2 land and the metric's own "review before relying on it" caveat is cleared | `semantic/authoring/execution/GBCDeposit/semantic_models.yml` |
| 5 | Add `dbt_utils.unique_combination_of_columns` on `(date, withdrawal_credentials)` to the intermediate `schema.yml` | `models/execution/GBCDeposit/intermediate/schema.yml` |
| 6 | Decide `contracts_GBCDeposit_calls` fate: build the intended downstream intermediate or drop it to stop wasteful cron builds | `models/contracts/GBCDeposit/contracts_GBCDeposit_calls.sql` |
| 7 | Remove or comment the dead `apply_monthly_incremental_filter` call from the view | `models/execution/GBCDeposit/intermediate/int_GBCDeposit_deposists_daily.sql` |
| 8 | Fix the `deposists` misspelling across all six artifacts (model filename, intermediate schema, semantic model, metric, `execution_graph.yml` relationship, `question_synonyms`) in one coordinated rename | `models/execution/GBCDeposit/intermediate/`, `models/execution/GBCDeposit/intermediate/schema.yml`, `semantic/authoring/execution/GBCDeposit/semantic_models.yml`, `semantic/relationships/execution_graph.yml` |
| 9 | Document execution-vs-consensus deposit sourcing: state which is authoritative and add a lightweight reconciliation check; note EIP-7251 consolidation-deposit divergence risk | `models/execution/GBCDeposit/intermediate/` |
| 10 | Investigate 3-day cron gap; consider raising `freshness_anomalies` to `error` (or adding an external alert) so stalled runs are not silently warned-through | `models/execution/GBCDeposit/intermediate/schema.yml` |

---

## Open disagreements

None. The review converged in 1 round.

---

## Review log

| Round | Agent | Challenge / Resolution |
|---|---|---|
| 1 | Inspector | Identified 4 high, 3 medium, 3 low findings across implementation and data layers; 5 open questions issued |
| 1 | Context | Provided canonical definitions, contract addresses, semantic coverage map, and caveats; mutually consistent with inspector; no contradictions |
| 1 | Arbiter | Verified load-bearing claims directly; confirmed fabricated schema.yml columns, dead macro, orphaned calls model, Gwei denomination, and BLS address extraction vs. canonical guard; all open questions resolved; converged |
