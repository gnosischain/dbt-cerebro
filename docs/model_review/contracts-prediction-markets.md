# Model review: contracts/prediction-markets

**Convergence:** converged in 2 rounds — all three round-2 challenges resolved cleanly (FPMM events confirmed factory-creation-only, Wrapped1155Factory decoded_params confirmed fully populated, FPMM calls staleness root-caused to a silent pipeline cap).

---

## Scope and inventory

Six contract families, 10 dbt models (calls + events per family; SeerPM contributes 4 models for 2 contracts):

| Family | Models | Contract address | Protocol |
|---|---|---|---|
| `ConditionalTokens` | calls + events | `0xceafdd6b` | Omen + Seer (shared) |
| `FPMMDeterministicFactory` | calls + events | `0x9083a2b6` | Omen |
| `OmenAgentResultMapping` | calls + events | `0x260e1077` | Omen AI agents (v1) |
| `AgentResultMapping` | calls + events | `0x99c43743` | Seer AI agents (v2) |
| `Realitio_v2_1` | calls + events | `0x79e32ae0` | Omen + Seer oracle |
| `SeerPM` (MarketFactory + Wrapped1155Factory) | events x2 | `0x83183da8`, `0xd194319d` | Seer |

All 10 models are thin wrappers around the shared `decode_calls` / `decode_logs` macros. They are raw source-layer decode tables; there are zero downstream intermediate models, zero `api_*` mart models, and zero semantic layer entries for this entire unit.

---

## Business context

This unit answers: what on-chain activity happened across Gnosis Chain prediction-market protocols (Omen and Seer) at raw event/call resolution?

**Omen** (live since September 2020): ConditionalTokens provides the ERC-1155 outcome-token ledger; FPMMDeterministicFactory deploys per-market AMM pool clones; Realitio v2.1 (reality.eth) is the resolution oracle; OmenAgentResultMapping (v1) records AI-agent binary probability predictions as a single `uint16 estimatedProbabilityBps`.

**Seer** (factory deployed October 2024): reuses the same ConditionalTokens ledger and Realitio oracle; Seer MarketFactory creates categorical/scalar/multi-outcome markets; Wrapped1155Factory wraps ERC-1155 outcome tokens into ERC-20 for AMM liquidity; AgentResultMapping (v2) extends agent predictions to multi-outcome markets via `uint16[] estimatedProbabilitiesBps[]` and `string[] outcomes[]`.

**All seven contract addresses are fully verified** in `event_signatures.csv`, `function_signatures.csv`, and `contracts_abi.csv` (a round-1 error claiming `0xd194319d` was missing from seeds was corrected in round 2).

All live Omen and Seer product analytics (26 Omen queries, 30+ Seer queries) currently run on Dune (`omen_gnosis.*`, `seer_pm_gnosis.*`). The warehouse decode tables are production-tagged and incrementally maintained but feed no warehouse consumer.

---

## Implementation assessment

### HIGH — Wrapped1155Factory_events uses `(block_timestamp, transaction_hash)` as unique key instead of `(block_timestamp, log_index)`

`models/contracts/SeerPM/contracts_Seer_Wrapped1155Factory_events.sql`

Every other events model uses `(block_timestamp, log_index)` as the ReplacingMergeTree order-by and unique key, which is correct because multiple events can share a transaction hash. The Wrapped1155Factory_events model deviates and uses `transaction_hash`. If the contract emits two `Wrapped1155Creation` events in a single transaction, the ReplacingMergeTree collapses them to one row. Current data (2,341 rows, all unique transaction hashes) hides the defect; the schema is structurally wrong and dangerous as activity grows. Requires a full refresh after the key is corrected.

### HIGH — `dbt_incremental_runner.py` silently drops models that exceed the max-slices-per-stage cap

`scripts/refresh/dbt_incremental_runner.py`, `models/contracts/FPMMDeterministicFactory/contracts_FPMMDeterministicFactory_calls.sql`

The runner enforces `max_slices_per_stage=30` (lines 965-977, 1044-1053). When a model's staleness gap exceeds 30 days, the runner appends an empty slice list, prints to stderr, and returns exit code 0. The daily pipeline completes "successfully" while the model is silently excluded. This is the confirmed root cause of the 71-day staleness on `contracts_FPMMDeterministicFactory_calls`: once the gap exceeded 30 slices, every subsequent daily run skipped it without alerting. There is no Slack notification, non-zero exit code, or monitoring hook to surface this condition.

### HIGH — AgentResultMapping calls `start_blocktime` is 20 days after events `start_blocktime`

`models/contracts/AgentResultMapping/contracts_AgentResultMapping_calls.sql`, `models/contracts/AgentResultMapping/contracts_AgentResultMapping_events.sql`, `models/contracts/AgentResultMapping/schema.yml`

The events model uses `start_blocktime='2025-06-10'`; the calls model uses `'2025-06-30'`. In the warehouse, events begin 2025-06-23 and calls begin 2025-06-30, leaving seven days of call activity (June 23-29) absent. The events `start_date` of 2025-06-10 also predates the first actual event by 13 days, suggesting a stale documentation value rather than a backfill gap. The `schema.yml` `full_refresh.start_date` perpetuates the mismatch.

### MEDIUM — `decode_logs` single-address `addr_filter` lacks `lower()`/`replaceAll()` normalization

`macros/decoding/decode_logs.sql` (line 146), all seven events models in scope

The multi-address code path (line 141) correctly applies `lower(replaceAll(...))` to normalize the address before comparison. The single-address path (line 146) applies a bare equality filter. Currently safe because `execution.logs` stores addresses in 0x-stripped lowercase, but any future change to log storage format would silently drop records on all single-address event models with no error.

### MEDIUM — ConditionalTokens, FPMMDeterministicFactory, Realitio_v2_1 partition count approaching CH Cloud 100-partition insert limit

`models/contracts/ConditionalTokens/contracts_ConditionalTokens_calls.sql`, `contracts_ConditionalTokens_events.sql`, `models/contracts/FPMMDeterministicFactory/contracts_FPMMDeterministicFactory_calls.sql`, `contracts_FPMMDeterministicFactory_events.sql`, `models/contracts/Realitio_v2_1/contracts_Realitio_v2_1_calls.sql`, `contracts_Realitio_v2_1_events.sql`

All three families use `toStartOfMonth` partitioning with 65-69 months of history (start dates 2020-09 to 2021-01). A full rebuild inserts ~69 monthly partitions in a single statement; CH Cloud's hard limit is 100. Project memory (`feedback_ch_cloud_partition_cap.md`) explicitly advises migrating wide-history tables to `toStartOfYear`. At current growth, a full rebuild in mid-2027 would exceed the limit.

### MEDIUM — `decode_calls` deduplicates at the transaction level, missing internal calls from routers and proxies

`models/contracts/ConditionalTokens/contracts_ConditionalTokens_calls.sql`, `macros/decoding/decode_calls.sql`

The macro uses `execution.transactions` (one row per tx, deduplicated by `block_number + transaction_index` via `ROW_NUMBER`). Internal calls triggered via routers, aggregators, or proxy contracts are not captured. For ConditionalTokens (102K calls vs 10.5M events), multi-step DeFi flows may produce internal calls that are systematically undercounted. The macro has a traces path available but none of the prediction-market models invoke it.

### MEDIUM — All schema.yml column definitions are AI-generated stubs that do not match actual macro output

`models/contracts/ConditionalTokens/schema.yml`, `models/contracts/FPMMDeterministicFactory/schema.yml`, `models/contracts/OmenAgentResultMapping/schema.yml`, `models/contracts/AgentResultMapping/schema.yml`, `models/contracts/Realitio_v2_1/schema.yml`, `models/contracts/SeerPM/schema.yml`

The `decode_calls` macro emits: `block_number`, `block_timestamp`, `transaction_hash`, `contract_address`, `nonce`, `gas_price`, `value`, `function_name`, `decoded_input`. The `decode_logs` macro emits: `block_number`, `block_timestamp`, `transaction_hash`, `transaction_index`, `log_index`, `contract_address`, `event_name`, `decoded_params`. The schema.yml files list invented columns (`condition_id`, `condition_type`, `outcome_index`, `result_id`, `call_data`, `caller_address`, `gas_used`, `log_index` on calls models, etc.). `SeerPM/schema.yml` additionally documents macro parameters (`incremental_column`, `start_blocktime`) as output columns. All six files carry `authoritative: false`, but the fabricated column names impede future consumer development.

### LOW — FPMMDeterministicFactory_calls has 7 rows (0.34%) with null `function_name`

`models/contracts/FPMMDeterministicFactory/contracts_FPMMDeterministicFactory_calls.sql`

Seven calls matched the contract address filter but the 4-byte selector did not resolve to any function in the `function_signatures` seed. Likely proxy fallback calls or an ABI version gap. Low impact at 2,034 total rows but indicates an ABI coverage gap.

### LOW — No `api:`/`granularity:`/`window:`/`tier:` tags on any prediction-market models

All 10 models in scope

The project's canonical tag convention (`project_api_tag_convention.md`) requires these tags for models intended as API/MCP endpoints. None of the 10 decode models carry them. If any are ever promoted to API endpoints, they would bypass the CI tag guard. If they are strictly internal staging tables, they should be explicitly annotated as such.

---

## Business-logic assessment

### CRITICAL — FPMMDeterministicFactory events model captures zero Omen trading activity

`models/contracts/FPMMDeterministicFactory/contracts_FPMMDeterministicFactory_events.sql`

FPMMBuy, FPMMSell, FPMMFundingAdded, and FPMMFundingRemoved are emitted by individual FPMM pool clone contracts (EIP-1167 minimal proxies deployed by the factory), each with their own unique address. The `decode_logs` filter on the factory address `0x9083a2b6` captures only factory-level events: `FixedProductMarketMakerCreation` (21,167 rows, confirmed by live query) and `CloneCreated`. Zero trading or liquidity events exist in this model. Any warehouse analytics built on this table for Omen trading volume, LP activity, or market prices would return zero results. Dune covers the gap via the `omen_gnosis.trades` spell. No warehouse model captures FPMM clone trading events.

### CRITICAL — OmenAgentResultMapping is stale by 309 days

`models/contracts/OmenAgentResultMapping/contracts_OmenAgentResultMapping_events.sql`, `models/contracts/OmenAgentResultMapping/contracts_OmenAgentResultMapping_calls.sql`

Both tables have `max(block_timestamp) = 2025-08-06`, which is 309 days behind today (2026-06-11). No incremental run has processed this model in over 10 months. The total data captured is 65,919 event rows and 46,124 call rows — all stale. Whether this contract is deprecated (superseded by AgentResultMapping v2) or whether the halt is unintentional has not been documented. Any downstream AI-agent analytics on Omen binary markets is severely incomplete.

### HIGH — FPMMDeterministicFactory_calls is stale by 71 days due to confirmed silent pipeline exclusion

`models/contracts/FPMMDeterministicFactory/contracts_FPMMDeterministicFactory_calls.sql`

Max timestamp is 2026-04-01 while the events model is current at 2026-06-08. Root cause confirmed (round 2): `dbt_incremental_runner.py` silently dropped the model once the gap exceeded the 30-slice cap. The events model was at some earlier point backfilled via `refresh.py`; the calls model was not. The immediate fix is: `python scripts/full_refresh/refresh.py --select contracts_FPMMDeterministicFactory_calls --incremental-only`.

### HIGH — Zero warehouse analytics layer exists for this unit

`models/contracts/ConditionalTokens/`, `models/contracts/FPMMDeterministicFactory/`, `models/contracts/Realitio_v2_1/`, `models/contracts/AgentResultMapping/`, `models/contracts/SeerPM/`

All 10 decode tables are a data terminus. There are zero intermediate models, zero `api_*` mart models, and zero semantic layer entries consuming them. The entire Omen + Seer analytics pipeline lives in Dune. The tables are production-tagged and incrementally maintained but serve no current warehouse consumer.

### MEDIUM — OmenAgentResultMapping (v1) and AgentResultMapping (v2) have incompatible schemas with no unification layer

`models/contracts/OmenAgentResultMapping/contracts_OmenAgentResultMapping_events.sql`, `models/contracts/AgentResultMapping/contracts_AgentResultMapping_events.sql`

v1 stores a single `uint16 estimatedProbabilityBps` for binary markets. v2 stores `uint16[] estimatedProbabilitiesBps[]` and `string[] outcomes[]` for multi-outcome markets. The schema.yml for both uses identical generic column names (`result_id`, `result_value`, `additional_data`), obscuring the structural difference. No intermediate model unions the two contracts for a consistent AI-agent performance view.

### LOW — Seer Wrapped1155Factory `start_blocktime` (2024-02-07) predates Seer MarketFactory deployment (2024-10-08) by 8 months

`models/contracts/SeerPM/contracts_Seer_Wrapped1155Factory_events.sql`

The 8-month pre-Seer period may capture unrelated ERC-1155 wrapping activity not associated with Seer prediction markets. Not investigated; could create noise in analytics joining Wrapped1155Factory events to Seer market creation events.

---

## Data findings

Queries run across both rounds (10 tables sampled):

| Table | Rows | Max block_timestamp | Staleness |
|---|---|---|---|
| `contracts_ConditionalTokens_events` | 10,500,000+ | 2026-06-08 | 3 days |
| `contracts_ConditionalTokens_calls` | 102,000 | 2026-06-08 | 3 days |
| `contracts_FPMMDeterministicFactory_events` | 21,167 | 2026-06-08 | 3 days |
| `contracts_FPMMDeterministicFactory_calls` | 2,034 | 2026-04-01 | **71 days** |
| `contracts_OmenAgentResultMapping_events` | 65,919 | 2025-08-06 | **309 days** |
| `contracts_OmenAgentResultMapping_calls` | 46,124 | 2025-08-06 | **309 days** |
| `contracts_AgentResultMapping_events` | 71,000 | 2026-06-08 | 3 days |
| `contracts_Realitio_v2_1_events` | 74,000 | 2026-06-08 | 3 days |
| `contracts_Seer_MarketFactory_events` | 1,400 | 2026-06-07 | 4 days |
| `contracts_Seer_Wrapped1155Factory_events` | 2,341 | 2026-06-07 | 4 days |

Additional query results: `contracts_FPMMDeterministicFactory_events` confirmed to contain exactly 1 distinct `event_name` (`FixedProductMarketMakerCreation`, 21,167 rows — zero buy/sell/funding rows). `contracts_Seer_Wrapped1155Factory_events` decoded_params fully populated for all 5 sampled rows (3 key-value pairs per row: `multiToken`, `tokenId`, `wrappedToken`). No grain duplicates found in any sampled table. `FPMMDeterministicFactory_calls` null `function_name` rate: 7/2034 = 0.34%.

---

## Pros / Cons

**Pros**

- All seven contract addresses fully verified in all three local seeds (`event_signatures.csv`, `function_signatures.csv`, `contracts_abi.csv`).
- Append-only incremental pattern with monthly partitioning and `block_number` watermark is consistent and correct across all 10 models.
- Wrapped1155Factory unique-key concern was refuted: `decoded_params` correctly resolves all three indexed-only parameters from topics.
- FPMMDeterministicFactory staleness root cause is identified and actionable: a single `refresh.py` backfill command closes the 71-day gap.
- ConditionalTokens, Realitio, and AgentResultMapping are all current to within 4 days.
- ReplacingMergeTree with `allow_nullable_key` and experimental JSON type follows the established warehouse pattern; no grain duplicates found in sampled tables.
- AgentResultMapping v2 correctly extends the AI-agent prediction model to multi-outcome markets with complete seed coverage.

**Cons**

- FPMM trading data is entirely absent from the warehouse: FPMMBuy, FPMMSell, FPMMFundingAdded, FPMMFundingRemoved are clone-emitted, not factory-emitted; zero trading rows exist in this unit.
- OmenAgentResultMapping is stale by 309 days with no pipeline alert and no documented deprecation decision.
- `dbt_incremental_runner.py` swallows max-slices-per-stage overflow silently (exits 0, logs to stderr only), allowing multi-week staleness to compound undetected.
- Wrapped1155Factory_events unique key is structurally wrong (`transaction_hash` instead of `log_index`).
- All schema.yml column definitions are AI-generated stubs; consumers querying schema.yml will find fabricated field names.
- Zero downstream models, marts, or semantic layer entries — the entire unit is a data terminus.
- `decode_calls` operates at the transaction level, missing internal calls from routers and proxies.
- Three oldest models (ConditionalTokens, FPMMDeterministicFactory, Realitio) have 65-69 months of monthly partition history, approaching the CH Cloud 100-partition insert limit.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| IMMEDIATE | Backfill `FPMMDeterministicFactory_calls`: `python scripts/full_refresh/refresh.py --select contracts_FPMMDeterministicFactory_calls --incremental-only` | `models/contracts/FPMMDeterministicFactory/contracts_FPMMDeterministicFactory_calls.sql` |
| IMMEDIATE | Determine whether OmenAgentResultMapping is deprecated (contract sunset, migration to v2) or the 309-day halt is unintentional. If deprecated, add a deprecation tag and stop maintaining it. If active, diagnose and remediate. | `models/contracts/OmenAgentResultMapping/` |
| HIGH | Fix `dbt_incremental_runner.py` silent overflow: emit a non-zero exit code and/or a Slack/PagerDuty alert when a model is dropped from the daily plan due to the 30-slice cap. | `scripts/refresh/dbt_incremental_runner.py` |
| HIGH | Fix Wrapped1155Factory_events unique key from `(block_timestamp, transaction_hash)` to `(block_timestamp, log_index)` to match correct event log grain. Requires a full refresh to rekey existing rows. | `models/contracts/SeerPM/contracts_Seer_Wrapped1155Factory_events.sql` |
| HIGH | Decide explicitly whether to track FPMMBuy/FPMMSell/FPMMFundingAdded/FPMMFundingRemoved from FPMM pool clone addresses. If yes, build a model that discovers clone addresses from `FixedProductMarketMakerCreation` events and passes them as a multi-address input to `decode_logs`. If no, document the factory-creation-only scope in schema.yml to prevent misuse. | `models/contracts/FPMMDeterministicFactory/contracts_FPMMDeterministicFactory_events.sql` |
| MEDIUM | Migrate ConditionalTokens, FPMMDeterministicFactory, and Realitio_v2_1 from `toStartOfMonth` to `toStartOfYear` partitioning before history exceeds 100 months. | `models/contracts/ConditionalTokens/`, `models/contracts/FPMMDeterministicFactory/`, `models/contracts/Realitio_v2_1/` |
| MEDIUM | Rewrite schema.yml column definitions for all six contract families to reflect actual `decode_logs`/`decode_calls` macro output columns. Remove all fabricated field names and macro parameters documented as output columns. | All six schema.yml files |
| MEDIUM | Align AgentResultMapping calls `start_blocktime` with events model (or confirmed deployment date); backfill calls for June 23-29, 2025 if transactions exist. | `models/contracts/AgentResultMapping/contracts_AgentResultMapping_calls.sql`, `schema.yml` |
| LOW | Add `lower(replaceAll(...))` normalization to the single-address `addr_filter` branch in `decode_logs.sql` (line 146) to match the multi-address path's defensive normalization. | `macros/decoding/decode_logs.sql` |
| LOW | Annotate all 10 prediction-market models with `expose_to_mcp: false` or the equivalent privacy/tier tag if not intended as API/MCP endpoints, to prevent accidental semantic exposure. | All 10 models |

---

## Open disagreements

None. Review converged fully in round 2.

---

## Review log

| Round | Agent | Challenge | Outcome |
|---|---|---|---|
| 2 | Verdict vs Inspector | Challenge 1: Verify whether `FPMMDeterministicFactory_events` captures only factory-creation events or also trading events from clones. | CONFIRMED — live query returned exactly 1 distinct `event_name` (`FixedProductMarketMakerCreation`, 21,167 rows; zero buy/sell/funding rows). |
| 2 | Verdict vs Inspector | Challenge 2: Verify whether `Wrapped1155Factory_events` `decoded_params` is actually populated for the indexed-only event. | REFUTED — 5-row sample showed all rows fully populated with 3 key-value pairs (`multiToken`, `tokenId`, `wrappedToken`). Unique-key defect remains but the decoded_params population concern was incorrect. |
| 2 | Verdict vs Inspector | Challenge 3: Root-cause the 71-day staleness asymmetry between `FPMMDeterministicFactory_events` (current) and `_calls` (stale). | ROOT-CAUSED — `dbt_incremental_runner.py` `max_slices_per_stage=30` cap silently drops models exceeding 30 days of gap; events model was manually backfilled at some earlier point via `refresh.py`; calls model was not. |
| 2 | Verdict vs Context | Correction: Wrapped1155Factory address (`0xd194319d`) seed-presence claim from round 1 was wrong. | CORRECTED — round-2 context confirmed address is present in all three seeds (event_signatures.csv line 854, function_signatures.csv lines 2406-2414, contracts_abi.csv line 74). Round-1 "NOT FOUND" was a search error. |
| 2 | Verdict vs Context | Challenge: FPMM factory-vs-clone scope caveat from round 1 — confirm whether clone buy/sell events are architecturally excluded. | CONFIRMED — `decode_logs` applies `address = '9083a2b6...'` filter on the emitting contract address column; EIP-1167 clones emit from their own addresses; no clone trading events are captured in any warehouse model. |
