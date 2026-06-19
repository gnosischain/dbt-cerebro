# Model review: contracts/lending-tokens-oracles

**Convergence:** converged in 2 rounds — all factual challenges resolved; remaining issues are business decisions with no open disagreements between agents.

---

## Scope and inventory

Twenty SQL models across seven contract families, all thin wrappers around the shared `decode_logs` / `decode_calls` macros. No custom SQL logic beyond macro invocation and parameter selection.

| Family | Models | Source table | Purpose |
|---|---|---|---|
| `contracts/aave` | 3 | `execution.logs` | Aave V3 Pool, PoolConfigurator, AToken events |
| `contracts/spark` | 3 | `execution.logs` | SparkLend Pool, PoolConfigurator, AToken events |
| `contracts/agave` | 1 | `execution.logs` | Agave V2 LendingPool events (orphan) |
| `contracts/backedfi` | 9 | `execution.logs` | BackedFi Chainlink-compatible RWA oracle events |
| `contracts/chainlink` | 1 | `execution.logs` | Multi-feed Chainlink AnswerUpdated events |
| `contracts/tokens` | 3 | `execution.logs` / `execution.transactions` | sDAI events, WxDAI events, WxDAI calls |
| `contracts/GBCDeposit` | 2 | `execution.logs` / `execution.transactions` | GBC deposit events and calls |

---

## Business context

This unit is the foundational decoded-event staging layer for Gnosis Chain DeFi analytics. It feeds six downstream pipelines:

1. **Lending analytics** (Aave V3 + SparkLend): supply/borrow APY, TVL, utilization, user-balance cohorts. Aave V3 and SparkLend pool events feed `int_execution_lending_aave_daily`; aToken events additionally feed `int_execution_lending_aave_diffs_daily` for scaled-balance delta tracking (wallet-to-wallet BalanceTransfer and treasury mintToTreasury are invisible to pool events and require aToken events).
2. **Native token prices** (`int_execution_prices_oracle_daily`): Chainlink AnswerUpdated events decoded via `contracts_chainlink_feeds_events` replace the Dune price-feed dependency.
3. **RWA prices** (`int_execution_rwa_backedfi_prices`): nine BackedFi oracle contracts using the same Chainlink ABI.
4. **Savings xDAI APY** (`int_yields_savings_xdai_rate_daily`): `contracts_sdai_events` provides ERC-4626 Deposit/Withdraw events from which the 7-day geometric rolling APY is derived.
5. **ERC-20 transfer analytics**: `contracts_wxdai_events` feeds `int_execution_transfers_whitelisted_daily`.
6. **Validator deposit tracking**: `contracts_GBCDeposit_calls` / `contracts_GBCDeposit_events` feed `int_GBCDeposit_deposists_daily` and the approved semantic model `GBCDeposit_deposists_daily`.

**Canonical definitions (as implemented):**

- Supply APY = `(1 + liquidityRate / 1e27 / 31536000)^31536000 - 1` applied to the last `ReserveDataUpdated.liquidityRate` of each calendar day. Variable borrow APY uses `variableBorrowRate` with the same formula.
- Chainlink USD price = `decoded_params['current'] / 1e8` (8-decimal feeds); wstETH uses `decoded_params['current'] / 1e18 * ETH_USD` (18-decimal exchange rate).
- BackedFi RWA price = `decoded_params['current'] / 1e8` via `argMax(block_timestamp)` for end-of-day value.
- sDAI APY = `(1 + daily_rate)^365 - 1` where `daily_rate` is the 7-day geometric slope `(share_price_today / share_price_7d_ago)^(1/7) - 1`.
- GBCDeposit validator address = `concat('0x', substring(withdrawal_credentials, 27, 40))`.
- Scaled balance delta (aToken): three sources in `int_execution_lending_aave_diffs_daily` — pool_deltas (rayDiv of amount against liquidityIndex via ASOF JOIN), transfer_deltas (raw `value` from BalanceTransfer), treasury_mint_deltas (rayDiv from Mint where caller = Pool).

**Contract addresses verified** against `seeds/lending_market_mapping.csv`, `seeds/atoken_reserve_mapping.csv`, `seeds/tokens_whitelist.csv`, and `docs/native_token_prices_build_plan.md`. All Aave V3 and SparkLend pool, configurator, and aToken addresses match exactly (case-insensitive). No unresolvable mismatches found.

---

## Implementation assessment

### Critical

**Agave LendingPool ABI missing — 63.4 M rows 100% undecoded**
`models/contracts/agave/contracts_agave_LendingPool_events.sql` (address `0x5E15d5E33d318dCEd84Bfe3F4EACe07909bE6d9c`) has zero rows in `event_signatures` for its address. The `ANY LEFT JOIN` in `decode_logs` produces `event_name = ''` for every row. Confirmed by query: 63,381,046 rows, 100% blank `event_name`, date range 2022-04-19 to 2026-06-07. Every stored row is an undecoded raw payload with no usable signal.

### High

**Schema.yml phantom columns — fabricated column names in majority of models**
Most schema.yml files describe columns that `decode_logs` / `decode_calls` never emit (e.g. `event_type`, `pool_address`, `reserve_asset`, `amount`, `sender_address`, `event_data`, `oracle_id`, `round_id`, `created_at`, `call_data`, `amount_wei`, `token_address`, `receiver_address`) while omitting real output columns (`decoded_params`, `event_name`, `transaction_index`). Confirmed affected: `contracts_aaveV3_PoolInstance_events`, `contracts_aaveV3_PoolConfigurator_events`, `contracts_GBCDeposit_calls`, `contracts_wxdai_calls`, `contracts_wxdai_events`, `contracts_sdai_events`, all 9 `contracts_backedfi_*_Oracle_events`. The chainlink, agave, spark, and aaveV3_AToken schemas are correct. Affected schema files: `models/contracts/aave/schema.yml`, `models/contracts/backedfi/schema.yml`, `models/contracts/tokens/schema.yml`, `models/contracts/GBCDeposit/schema.yml`.

**contracts_backedfi_bC3M_Oracle_events is 49 days stale — root cause unknown**
`models/contracts/backedfi/contracts_backedfi_bC3M_Oracle_events.sql` shows `max(block_timestamp) = 2026-04-23`, 49 days behind today (2026-06-11). All other 8 BackedFi oracle models are 3 days stale (normal daily lag). bC3M emitted ~14 events/week through mid-April 2026, then stopped entirely. Root cause is not established: either the bC3M oracle contract (`0x83Ec02059F686E747392A22ddfED7833bA0d7cE3`) was deprecated or migrated to a new address, or there is a watermark staleness bug specific to this model. This must be resolved before the next RWA price reporting cycle.

### Medium

**Agave decode model is an orphan — zero downstream consumers, burning daily compute**
No intermediate or mart model references `contracts_agave_LendingPool_events`. Combined with the missing ABI (all `event_name = ''`), the model burns `execution.logs` scan time and stores 63 M rows of undecoded data for zero analytical value. Agave protocol wound down circa 2023.

**calls models unique_key = (block_timestamp, transaction_hash) — unsafe if source switches to traces**
`models/contracts/GBCDeposit/contracts_GBCDeposit_calls.sql` and `models/contracts/tokens/contracts_wxdai_calls.sql` define `unique_key = (block_timestamp, transaction_hash)`. If a single transaction contains multiple internal calls (via traces), `decode_calls` would emit multiple rows per `tx_hash`, silently violating uniqueness. Both models currently read from `execution.transactions` (not traces), so the grain holds today, but the key would break silently on a source migration.

**Spark Pool events start_blocktime (2023-09-05) predates SparkLend Gnosis market launch (2023-10-06)**
`models/contracts/spark/contracts_spark_Pool_events.sql` scans a full extra month of `execution.logs` for a contract address that had no events before 2023-10-06. Harmless for correctness but wasteful on every full-refresh batch.

### Low

**RMT order_by uses (block_timestamp, log_index) instead of (block_number, log_index)**
All events models set `order_by = (block_timestamp, log_index)`. The `decode_logs` dedup keys on `(block_number, transaction_index, log_index)`, which is strictly unique. `block_timestamp` is one-to-one with `block_number` on Gnosis Chain in practice, so no current data integrity issue is observed, but `(block_number, log_index)` is the provably unique ordering key. Affected: `models/contracts/aave/contracts_aaveV3_PoolInstance_events.sql`, `models/contracts/spark/contracts_spark_Pool_events.sql`, `models/contracts/chainlink/contracts_chainlink_feeds_events.sql`.

**Chainlink feeds start_blocktime (2021-01-01) predates most feeds on Gnosis by ~1 year**
`models/contracts/chainlink/contracts_chainlink_feeds_events.sql` starts at 2021-01-01; most Gnosis Chainlink feeds launched in 2022. The `batch_months = 1` full-refresh setting bounds per-batch cost, so there is no correctness issue.

**aGnoWXDAI hex-case mismatch between seeds**
`seeds/lending_market_mapping.csv` has `0xd0Dd6cEF72143E22cCed...` (lowercase `ed`); `seeds/atoken_reserve_mapping.csv` has `0xd0Dd6cEF72143E22cCED...` (uppercase `ED`). Same EVM address; `decode_logs` normalises to lowercase before filtering, so no functional impact. Seeds should be aligned to the same canonical checksum case.

---

## Business-logic assessment

### High

**bTSLA has no BackedFi oracle model — USD price is absent for a live whitelisted asset**
bTSLA (token `0x14a5f...`, whitelisted since 2024-09-12) has no `contracts_backedfi_bTSLA_Oracle_events` model. Only TSLAx (`0x8aD3c...`, whitelisted since 2025-01-01) has an oracle model. `int_execution_rwa_backedfi_prices` therefore produces no USD price for bTSLA. Any downstream TVL or user-balance-in-USD metric for bTSLA positions will be NULL or incorrectly zero. The path to resolution (new oracle model vs. alternative price source) is not documented. Affected: `models/contracts/backedfi/`.

**osETH and stETH carry no on-chain USD anchor despite being documented as planned feeds**
The osETH-ETH feed (`0xD132Cf1dd2e1FB75c7d97d591d87D5E07A681353`) and a STETH/USD feed are documented in `docs/native_token_prices_build_plan.md` as feeds to add but are absent from `models/contracts/chainlink/contracts_chainlink_feeds_events.sql`. Any downstream model attempting to price osETH or stETH positions will produce NULL prices.

### Medium

**atoken_reserve_mapping.csv covers only Aave V3 (6 aTokens) and omits all 9 SparkLend spTokens**
The full 15-row aToken-to-reserve mapping lives in `seeds/lending_market_mapping.csv`, but `seeds/atoken_reserve_mapping.csv` contains only 6 Aave V3 entries. Downstream models joining against `atoken_reserve_mapping.csv` will silently miss all SparkLend spToken positions. This seed is either stale and redundant (should be deprecated in favour of `lending_market_mapping.csv`) or serves a scope-limited purpose that needs explicit documentation.

**GBCDeposit 'deposists' typo propagates into an approved semantic model**
`models/execution/GBCDeposit/intermediate/int_GBCDeposit_deposists_daily.sql` and `semantic/authoring/execution/GBCDeposit/semantic_models.yml` both carry the misspelling `deposists` instead of `deposits`. This is the model name exposed to MCP natural-language query routing. Synonym matching for 'deposits', 'deposited', 'deposit activity' will degrade because the canonical name does not contain the standard spelling.

**Chainlink stablecoin peg assumptions mask potential depeg events**
CHF/USD oracle is used as proxy for both ZCHF and svZCHF; DAI/USD for xDAI and WxDAI; USDC/USD for both USDC and USDC.e. Appropriate for routine operations but means the price feed will not capture depeg events. Any metric using oracle prices as collateral value ground truth will be incorrect during a depeg. The assumption is not documented in `models/contracts/chainlink/contracts_chainlink_feeds_events.sql`.

### Low

**contracts_spark_AToken_events 96K row count is confirmed correct — lower SparkLend activity, not ABI gaps**
All 9 spToken addresses have complete ABI coverage (7 rows each in `event_signatures`; 6 AToken events + 1 Upgraded proxy event) and all 6 event types decode correctly. The 96K vs 3M disparity versus Aave V3 is entirely attributable to lower on-chain activity: aGnoEURe alone drives ~2.2 M Aave V3 rows due to high-frequency Approval/Transfer usage. No pipeline correctness concern for `int_execution_lending_aave_diffs_daily`. Affected: `models/contracts/spark/contracts_spark_AToken_events.sql`.

**Pool configurator events decoded but unused**
`models/contracts/aave/contracts_aaveV3_PoolConfigurator_events.sql` and `models/contracts/spark/contracts_spark_PoolConfigurator_events.sql` are decoded and current but have no downstream intermediate or mart consumers. They exist for ad-hoc governance/risk analysis only.

---

## Data findings

Eight queries were run against production warehouse tables during the review:

| Table | Key metric |
|---|---|
| `contracts_agave_LendingPool_events` | 63,381,046 rows; 100% `event_name = ''`; range 2022-04-19 to 2026-06-07 |
| `event_signatures` for agave address | 0 rows |
| `contracts_aaveV3_PoolInstance_events` | 3.66 M rows; `max_date` = 2026-06-08 (3 days stale) |
| `contracts_chainlink_feeds_events` | 1,167,766 rows; `max_date` = 2026-06-09 (2 days stale); 0 duplicates on `(block_timestamp, log_index)` |
| `contracts_aaveV3_AToken_events` | 3,011,808 rows; 0 duplicates on `(block_timestamp, log_index)` |
| `contracts_spark_AToken_events` | 96,225 rows; all 9 spToken addresses x 6 event types present |
| `contracts_backedfi_*` freshness | bC3M `max_date` = 2026-04-23 (49 days stale); all 8 others = 2026-06-08 (3 days stale) |
| `event_signatures` for 9 spToken addresses | 7 rows each (complete coverage) |

Grain integrity is clean: zero duplicates across Chainlink (1.17 M rows) and Aave V3 AToken (3.01 M rows). All active models are 2-4 days stale, within expected daily batch lag with no SLA breach.

---

## Pros / Cons

**Pros**

- All 20 models are thin, consistent wrappers around battle-tested `decode_logs` / `decode_calls` macros — no custom SQL logic to audit or maintain separately.
- Contract address registry is fully verified against seeds with zero unresolvable mismatches.
- Grain integrity confirmed clean: zero duplicates across Chainlink feeds and Aave V3 AToken events.
- All 9 spToken ABI entries are complete and correct — SparkLend balance-cohort pipeline has no silent decode gaps.
- Freshness for all active models is within 2-4 days; no SLA breach.
- BackedFi oracle family covers 9 RWA tokens with Chainlink-compatible AnswerUpdated ABI, providing a fully on-chain RWA price feed.
- sDAI APY derivation uses a defensible 7-day geometric rolling slope that correctly handles discrete yield relay batches.
- GBCDeposit correctly extracts the 20-byte validator address from `withdrawal_credentials` and feeds an approved semantic model.

**Cons**

- 63.4 M rows in `contracts_agave_LendingPool_events` are 100% undecoded because the Agave ABI was never loaded into `event_signatures` — the model burns daily compute and storage for zero analytical value.
- Schema.yml phantom columns affect the majority of models, undermining schema documentation and any tooling that relies on it.
- `contracts_backedfi_bC3M_Oracle_events` is 49 days stale with no root cause identified.
- bTSLA (whitelisted since 2024-09-12) has no BackedFi oracle model — its USD price is absent from `int_execution_rwa_backedfi_prices`.
- osETH and stETH are not priced via oracle despite both feeds being documented in the build plan.
- calls models use `unique_key = (block_timestamp, transaction_hash)`, which would silently break on a traces-based source migration.
- GBCDeposit typo 'deposists' propagates through `int_GBCDeposit_deposists_daily` into the approved semantic model.
- `atoken_reserve_mapping.csv` covers only 6 Aave V3 aTokens and omits all 9 SparkLend spTokens, creating asymmetry between seed registries.

---

## Recommendations

| Priority | Recommendation | Affected models / files |
|---|---|---|
| P0 | Investigate `contracts_backedfi_bC3M_Oracle_events` staleness (49 days): check whether oracle `0x83Ec02059F686E747392A22ddfED7833bA0d7cE3` has been deprecated or migrated; if migrated, update the contract address; if still active, diagnose the watermark staleness bug. | `models/contracts/backedfi/contracts_backedfi_bC3M_Oracle_events.sql` |
| P0 | Disable or fix `contracts_agave_LendingPool_events`: either load the Agave V2 LendingPool ABI into `event_signatures` for `0x5E15d5E33d318dCEd84Bfe3F4EACe07909bE6d9c` and create a downstream intermediate model, or tag the model `disabled` in `dbt_project.yml` to stop burning daily `execution.logs` scans. | `models/contracts/agave/contracts_agave_LendingPool_events.sql` |
| P1 | Add a BackedFi oracle model for bTSLA using the same template as other backedfi oracle models, or document explicitly that bTSLA has no on-chain price feed and how it is priced. bTSLA is a live whitelisted asset with no USD anchor in `int_execution_rwa_backedfi_prices`. | `models/contracts/backedfi/` |
| P1 | Add the osETH-ETH feed (`0xD132Cf1dd2e1FB75c7d97d591d87D5E07A681353`) to `contracts_chainlink_feeds_events.sql` as documented in `docs/native_token_prices_build_plan.md`, using the same wstETH-style 18-decimal ETH rate conversion. | `models/contracts/chainlink/contracts_chainlink_feeds_events.sql` |
| P1 | Run `generate-schema` for all models with phantom schema.yml columns: `contracts_aaveV3_PoolInstance_events`, `contracts_aaveV3_PoolConfigurator_events`, `contracts_GBCDeposit_calls`, `contracts_wxdai_calls`, `contracts_wxdai_events`, `contracts_sdai_events`, and all 9 `contracts_backedfi_*_Oracle_events`. Replace fabricated column lists with the correct `decode_logs` / `decode_calls` 8-column schema. | `models/contracts/aave/schema.yml`, `models/contracts/backedfi/schema.yml`, `models/contracts/tokens/schema.yml`, `models/contracts/GBCDeposit/schema.yml` |
| P2 | Fix the 'deposists' typo across `int_GBCDeposit_deposists_daily` and the semantic model `GBCDeposit_deposists_daily` in a single atomic rename, updating all SQL refs and `semantic_models.yml` simultaneously. | `models/execution/GBCDeposit/intermediate/int_GBCDeposit_deposists_daily.sql`, `semantic/authoring/execution/GBCDeposit/semantic_models.yml` |
| P2 | Deprecate or scope-document `seeds/atoken_reserve_mapping.csv`: if SparkLend spTokens are intentionally excluded, add a comment explaining the scope; if stale, migrate consumers to `seeds/lending_market_mapping.csv` and drop the seed. | `seeds/atoken_reserve_mapping.csv` |
| P2 | Update `contracts_spark_Pool_events` start_blocktime from `2023-09-05` to `2023-10-06` to match the actual SparkLend Gnosis market launch and eliminate one month of wasted `execution.logs` scanning. | `models/contracts/spark/contracts_spark_Pool_events.sql` |
| P3 | Update `unique_key` for `contracts_GBCDeposit_calls` and `contracts_wxdai_calls` to include a call-level discriminator (e.g. `call_index`) so the key remains valid if these models are ever migrated to a traces-based source. | `models/contracts/GBCDeposit/contracts_GBCDeposit_calls.sql`, `models/contracts/tokens/contracts_wxdai_calls.sql` |
| P3 | Add documentation in `contracts_chainlink_feeds_events.sql` and canonical definitions noting that CHF/USD serves as proxy for ZCHF and svZCHF, DAI/USD for xDAI and WxDAI, and USDC/USD for USDC and USDC.e — and that these assumptions mask depeg events. | `models/contracts/chainlink/contracts_chainlink_feeds_events.sql` |
| P3 | Align `aGnoWXDAI` address checksum case between `seeds/lending_market_mapping.csv` (`0xd0Dd6cEF72143E22cCed...`) and `seeds/atoken_reserve_mapping.csv` (`0xd0Dd6cEF72143E22cCED...`). No functional impact; seeds should use a single canonical form. | `seeds/lending_market_mapping.csv`, `seeds/atoken_reserve_mapping.csv` |
| P3 | Tighten `order_by` in events models from `(block_timestamp, log_index)` to `(block_number, log_index)` to match the `decode_logs` dedup key exactly. | `models/contracts/aave/contracts_aaveV3_PoolInstance_events.sql`, `models/contracts/spark/contracts_spark_Pool_events.sql`, `models/contracts/chainlink/contracts_chainlink_feeds_events.sql` |

---

## Open disagreements

None — review converged fully.

---

## Review log

| Round | Challenge | Outcome |
|---|---|---|
| 1 | Inspector reported `contracts_spark_AToken_events` (96K rows) as a medium-severity possible ABI gap. | Context agent confirmed the model feeds `int_execution_lending_aave_diffs_daily` via the `atoken_events_raw` CTE — making row count correctness a pipeline-critical concern, not merely a coverage nuance. |
| 2 | Inspector queried all 9 spToken addresses in `event_signatures` and `contracts_spark_AToken_events`. | Confirmed complete ABI coverage (7 rows each) and correct decoding of all 6 event types. Row count disparity is entirely explained by lower SparkLend on-chain activity (aGnoEURe alone drives ~2.2 M Aave V3 rows). Finding downgraded from medium to low. All open questions resolved or deferred to business decisions. |
