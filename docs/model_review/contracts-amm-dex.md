# Model review: contracts/AMM-DEX

**Convergence:** converged in 2 rounds — three inspector challenges were all resolved with warehouse evidence in round 2; no remaining disagreements between inspector and context agents.

---

## Scope and inventory

| Protocol | Models | Key tables |
|---|---|---|
| BalancerV2 | 2 | `contracts_BalancerV2_Vault_events`, `contracts_BalancerV2_Vault_events_live` |
| BalancerV3 | 2 | `contracts_BalancerV3_Vault_events`, `contracts_BalancerV3_Vault_events_live` |
| Curve | 2 | `contracts_Curve3PoolLP_events`, `contracts_CurveGauge_events` |
| Swapr V3 | 5 | `contracts_Swapr_v3_AlgebraPool_events/calls`, `contracts_Swapr_v3_AlgebraFactory_events/calls`, `contracts_Swapr_v3_AlgebraPool_events_live` |
| UniswapV3 | 4 | `contracts_UniswapV3_Pool_events/live`, `contracts_UniswapV3_Factory_events`, `contracts_UniswapV3_NonfungiblePositionManager_events` |
| CowProtocol | 4 | `contracts_CowProtocol_GPv2Settlement_events`, `contracts_CowProtocol_CoWSwapEthFlow_events`, `contracts_CowProtocol_GPv2VaultRelayer_events`, `contracts_CowProtocol_GPv2AllowListAuthentication_events` |
| **Total** | **19** | — |

All 19 models (17 SQL files plus 2 calls models) are thin wrappers around the shared `decode_logs` (557 lines) and `decode_calls` (609 lines) macros. No hand-written join, filter, window, or aggregation logic exists within the model files. All decoded fields land in a `Map(String, Nullable(String))` `decoded_params` column. There is no API/mart layer in this unit — it is a pure raw-event decode store consumed exclusively by downstream pipelines.

Pool addresses are driven by `seeds/contracts_whitelist.csv` for UniswapV3 (29 entries) and Swapr V3 (12 entries). All other contract addresses are hardcoded in model config.

---

## Business context

This unit is the canonical on-chain event decode layer for Gnosis Chain AMM/DEX protocols. It feeds seven distinct downstream pipelines:

1. **LP yield and pool health analytics** (`execution/pools`) — TVL, fee APR, LVR, unique LP counts for Uniswap V3, Swapr V3, Balancer V2, Balancer V3.
2. **CoW Protocol trade analytics** (`execution/cow`) — Trade and Settlement events from GPv2Settlement, solver competition, batch routing, protocol revenue. GPv2AllowListAuthentication events feed `stg_cow__solvers` for solver registry.
3. **UBO token-claim attribution** (`execution/ubo`) — Curve 3pool LP and gauge Transfer/Deposit/Withdraw events; Uniswap V3 and Swapr V3 NonfungiblePositionManager events for LP NFT ownership tracking.
4. **Near-real-time DEX trades feed** (`execution/live`) — four `_live` plain views over `execution_live.logs`, TTL-managed to the last 2 hours, feeding the live trade dashboard on the `feat/live-trades` branch.
5. **Gnosis App WAU** (`execution/gnosis_app`) — GPv2Settlement events feed `int_execution_gnosis_app_swaps`, contributing to the GA WAU composite metric.
6. **Swapr pool registry discovery** — `contracts_Swapr_v3_AlgebraFactory_events` feeds `stg_pools__v3_pool_registry` for dynamic pool discovery.
7. **MCP ad-hoc query access** — GPv2VaultRelayer and CoWSwapEthFlow events are decoded for direct query access; no structured downstream pipeline references them.

**Key canonical definitions** (verified against `docs/protocols/dexes/` and downstream model SQL):

- **TVL (pool):** `sum(reserve_amount * price_usd)` per token per pool per day; reserve excludes unclaimed fees; price via ASOF join on `int_execution_token_prices_daily` (Dune oracle source).
- **fee_apr_7d:** `(fees_usd_7d / tvl_usd_7d_avg) * (365/7) * 100`; NULL when fewer than 3 days in window, avg TVL <= $500, or protocol is Balancer V2 (fee computation not implemented for V2).
- **Gross fees:** UniV3: `swap_in * fee_ppm / 1e6`; Swapr V3: dynamic fee via ASOF join on Fee events; Balancer V3: explicit `swapFeeAmount` field in Swap events; Balancer V2: not implemented.
- **lvr_apr_7d:** `(swap_flow_usd_7d - fees_usd_7d) / tvl_usd_7d_avg * (365/7) * 100`; always <= 0 by sign convention; Uniswap V3 and Swapr V3 only.
- **is_cow (Pure CoW trade):** `num_trades > 1 AND num_interactions = 0` — full peer-to-peer matching, no AMM calls.
- **volume_usd (CoW):** `amount_bought * buy_token_price_usd`, fallback to sold side; price join on token symbol (not address — see caveats).
- **fee_usd (CoW, Sep 2024+):** surplus-based protocol revenue from `crawlers_data.cow_api_trade_fees`. Pre-Sep 2024 `feeAmount` is the user's signed-maximum fee ceiling under CIP-12 — not protocol revenue; summing it massively overstates historical CoW revenue.
- **UBO claim (Curve):** `(holder effective_lp / total effective_lp) * pool_reserve_per_token`; effective LP combines direct x3CRV balances and gauge-deposited positions.

**Contract addresses verified** against schema.yml, cerebro-docs protocol pages, and warehouse evidence (all cross-verified; no unresolvable addresses):

| Contract | Address | Actual first on-chain event (Gnosis Chain) |
|---|---|---|
| BalancerV2 Vault | `0xBA12222222228d8Ba445958a75a0704d566BF2C8` | 2022-11-01 (deployed Nov 2022, not 2021) |
| BalancerV3 Vault | `0xba1333333333a1ba1108e8412f11850a5c319ba9` | 2024-12-05 |
| Curve x3CRV LP | `0x1337BedC9D22ecbe766dF105c9623922A27963EC` | 2021-09-01 |
| Curve Gauge | `0xb721cc32160ab0da2614cc6ab16ed822aeebc101` | 2021-09-01 |
| UniswapV3 Factory | `0xe32f7dd7e3f098d518ff19a22d5f028e076489b1` | 2022-04-22 |
| UniswapV3 NPM | `0xae8fbe656a77519a7490054274910129c9244fa3` | 2022-04-22 |
| Swapr AlgebraFactory | `0xa0864cca6e114013ab0e27cbd5b6f4c8947da766` | 2023-09-22 (redeployment; original Swapr V1/V2 addresses retired) |
| Swapr NPM | `0x91fd594c46d8b01e62dbdebed2401dde01817834` | 2022-03-01 era |
| CowProtocol GPv2Settlement | `0x9008D19f58AAbD9eD0D60971565AA8510560ab41` | 2021-08-04 (contract not active on Gnosis Chain before Aug 2021) |
| CowProtocol CoWSwapEthFlow | `0xbA3cB449bD2B4ADddBc894D8697F5170800EAdeC` | 2023-04-01 era |
| CowProtocol GPv2VaultRelayer | `0xC92E8bdf79f0507f65a392b0ab4667716BFE0110` | 2021-08-04 era |
| CowProtocol GPv2AllowListAuthentication | `0x2c4c28DDBdAc9C5E7055b4C863b72eA0149D8aFE` | 2021-08-04 era |

---

## Implementation assessment

### CRITICAL

**1. Seven newly-whitelisted UniswapV3Pool addresses will never receive historical backfill via incremental runs.**
`seeds/contracts_whitelist.csv` received two post-initial commits: four addresses added 2026-05-14, three added 2026-05-21. The `decode_logs` macro incremental path gates on `block_number > max(block_number)` (`macros/decoding/decode_logs.sql` line 218). Current watermark: block 46596999 (2026-06-08). Historical events for all seven pools — potentially months of swap, mint, and burn activity — are permanently absent unless a targeted full-refresh is run with a start date prior to each pool's deployment block. No automated mechanism exists to detect or flag newly seeded addresses that require backfill.

Affected: `models/contracts/UniswapV3/contracts_UniswapV3_Pool_events.sql`, `seeds/contracts_whitelist.csv`

### HIGH

**2. schema.yml documents aspirational flat columns for both Swapr AlgebraPool and AlgebraFactory events models, but physical tables use `Map(String,Nullable(String))` decoded_params only.**
`DESCRIBE TABLE` confirms both `contracts_Swapr_v3_AlgebraPool_events` and `contracts_Swapr_v3_AlgebraFactory_events` have exactly 8 columns: `block_number`, `block_timestamp`, `transaction_hash`, `transaction_index`, `log_index`, `contract_address`, `event_name`, `decoded_params`. The schema.yml for AlgebraPool documents 14 flat columns (`pool_address`, `event_type`, `sender`, `recipient`, `amount0`, `amount1`, `sqrt_price_x96`, `liquidity`, `tick`, etc.). The schema.yml for AlgebraFactory documents a different non-standard layout (`sender`, `recipient`, `amount_in`, `amount_out`, `event_data JSON`). Neither schema reflects the materialized table. Known downstream consumers (`stg_pools__v3_pool_registry`) correctly use `decoded_params['key']` map access and are not broken, but any engineer reading schema.yml will receive incorrect information.

Affected: `models/contracts/Swapr/contracts_Swapr_v3_AlgebraPool_events.sql`, `models/contracts/Swapr/contracts_Swapr_v3_AlgebraFactory_events.sql`, `models/contracts/Swapr/schema.yml`

**3. All four `_live` tables silently return 0 rows during any dbt scheduler gap exceeding 2 hours — no alerting exists.**
TTL is set to `block_timestamp + INTERVAL 2 HOUR` on all live models. When the dbt microbatch runner pauses beyond the TTL, all rows expire and downstream real-time consumers receive empty datasets with no error signal. The `execution_live.logs` source is confirmed healthy (151M rows, max_ts 2026-06-11T07:40 UTC) — this is purely a dbt run-frequency vs TTL duration gap. The self-healing fallback (`now() - INTERVAL 30 MINUTE` as source window when table is empty) means tables recover on the next dbt run, but no monitoring alert fires during the gap.

Affected: `models/contracts/UniswapV3/contracts_UniswapV3_Pool_events_live.sql`, `models/contracts/BalancerV2/contracts_BalancerV2_Vault_events_live.sql`, `models/contracts/BalancerV3/contracts_BalancerV3_Vault_events_live.sql`, `models/contracts/Swapr/contracts_Swapr_v3_AlgebraPool_events_live.sql`

**4. unique_key omits block_timestamp while RMT order_by includes it — dedup key mismatch on four models.**
`contracts_UniswapV3_Factory_events`, `contracts_UniswapV3_NonfungiblePositionManager_events`, `contracts_Swapr_v3_AlgebraFactory_events`, and `contracts_Swapr_v3_NonfungiblePositionManager_events` all declare `order_by=(block_timestamp, transaction_hash, log_index)` but `unique_key=(transaction_hash, log_index)`. The dbt schema uniqueness test enforces a weaker key than the ReplacingMergeTree engine. A tx_hash+log_index pair at two different block_timestamps (possible during reorg backfill) would be collapsed by the RMT but would pass the dbt test undetected. Practical risk is low on-chain but the test coverage guarantee is weaker than the engine guarantee.

Affected: `models/contracts/UniswapV3/contracts_UniswapV3_Factory_events.sql`, `models/contracts/UniswapV3/contracts_UniswapV3_NonfungiblePositionManager_events.sql`, `models/contracts/Swapr/contracts_Swapr_v3_AlgebraFactory_events.sql`, `models/contracts/Swapr/contracts_Swapr_v3_NonfungiblePositionManager_events.sql`

### MEDIUM

**5. Stale start_blocktime values across five models cause wasteful full-refresh scans and documentation drift.**
Confirmed actual deployment dates (from warehouse evidence) vs configured start dates:

| Model | SQL start_blocktime | Actual first data | Wasted months on full-refresh |
|---|---|---|---|
| `contracts_BalancerV2_Vault_events` | 2021-01-01 | 2022-11-01 | ~22 months |
| `contracts_BalancerV3_Vault_events` | 2024-01-01 | 2024-12-05 | ~11 months |
| All Swapr V3 models | 2022-03-01 | 2023-09-22 / 2023-10-06 | ~19 months |
| `contracts_Curve3PoolLP_events` | 2021-01-01 (SQL) | 2021-09-01 (schema.yml correct) | ~8 months (SQL/schema mismatch) |
| `contracts_CowProtocol_CoWSwapEthFlow_events` | 2023-01-01 (SQL) | 2023-04-01 (schema.yml correct) | ~3 months (SQL/schema mismatch) |

For BalancerV2 and Swapr V3: the "gaps" are not missing backfills — the contracts did not exist on Gnosis Chain before those dates. The stale start_blocktime values are artifacts from incorrect assumptions or prior contract versions.

**6. AlgebraPool_calls and AlgebraFactory_calls unique_key misses multi-call transactions.**
`contracts_Swapr_v3_AlgebraPool_calls.sql` uses `unique_key=(block_timestamp, transaction_hash)`. A transaction calling the same whitelisted AlgebraPool twice shares identical block_timestamp+transaction_hash, causing the second call to be silently collapsed by the ReplacingMergeTree. The `decode_calls` macro supports trace_address disambiguation via `is_traces=true` but this model uses transactions mode.

Affected: `models/contracts/Swapr/contracts_Swapr_v3_AlgebraPool_calls.sql`, `models/contracts/Swapr/contracts_Swapr_v3_AlgebraFactory_calls.sql`

**7. max_block_size memory tuning not applied to BalancerV2 and BalancerV3 Vault event models.**
`contracts_UniswapV3_Pool_events` and `contracts_CowProtocol_GPv2Settlement_events` both apply `SET max_block_size = 5000` via pre_hook to constrain per-block memory during high-volume incremental runs. `contracts_BalancerV2_Vault_events` (25.9M rows, the highest-row-count model in the unit) and `contracts_BalancerV3_Vault_events` do not apply this tuning. BalancerV2 is the most likely trigger for OOM spikes during large incremental backfill windows.

Affected: `models/contracts/BalancerV2/contracts_BalancerV2_Vault_events.sql`, `models/contracts/BalancerV3/contracts_BalancerV3_Vault_events.sql`

### LOW

**8. ANY LEFT JOIN in decode_logs produces silent NULL event_name rows on ABI gaps — untested in schema.yml.**
The `decode_logs` macro joins logs to the ABI CTE via `ANY LEFT JOIN`. Logs whose topic0 has no match in `event_signatures` produce NULL `event_name` and NULL `decoded_params` but still appear in output. No `not_null` test on `event_name` exists in any of the six schema.yml files. Current null rate for BalancerV2 and CowProtocol is confirmed 0, but any future ABI coverage gap would silently produce junk rows that pass all uniqueness tests.

Affected: `macros/decoding/decode_logs.sql`

---

## Business-logic assessment

### CRITICAL

**9. BalancerV2 excluded from all fee_apr and TVL analytics despite being the highest-volume AMM — exclusion undisclosed to API consumers.**
Balancer V2 is intentionally excluded from `int_execution_pools_fees_daily` and `fct_execution_pools_daily` because fee computation is not implemented for it. BalancerV2 has been operating on Gnosis Chain since November 2022 and has 25.9M decoded events. Every ecosystem-wide DEX volume, fee APR, TVL, and LP yield figure served via `api_execution_pools_*` marts and the MCP semantic layer silently omits BalancerV2. This exclusion is documented in `docs/model_review/execution-pools.md` but is not surfaced in any `api_*` mart description or schema.yml visible to external API consumers. Capital allocation decisions based on these figures will systematically undercount Gnosis Chain DEX activity.

### HIGH

**10. Curve 3pool has no decoded Swap/TokenExchange events — Curve DEX volume is a permanent gap in the analytics layer.**
`contracts_Curve3PoolLP_events` and `contracts_CurveGauge_events` decode only LP token Transfer/Approval and gauge Deposit/Withdraw events. No Swap or TokenExchange events are decoded for the Curve 3pool (0x7f90122bf0700f9e7e1f688fe926940e8839f353). Curve DEX trading volume and swap fees are therefore entirely absent from the warehouse. All DEX market-share breakdowns and protocol-level volume comparisons exclude Curve activity. This gap is not disclosed in any API-facing view description.

**11. CowProtocol GPv2Settlement actual deployment on Gnosis Chain is 2021-08-04, not 2021-04-01 — SQL and schema.yml start dates are incorrect.**
A bounded query on `execution.logs` for the GPv2Settlement address returns zero rows between 2021-04-01 and 2021-08-03. The contract was not active on Gnosis Chain until August 2021 (the same address exists on Ethereum mainnet from April 2021, but deployment dates differ by chain). The SQL `start_blocktime='2021-04-01'` and schema.yml `start_date: 2021-04-01` perpetuate an incorrect start date that misleads backfill operators about the protocol's Gnosis Chain history. No historical data is missing — there is no source data to recover for Apr–Aug 2021. Both SQL and schema.yml should be corrected to `2021-08-01`.

Affected: `models/contracts/CowProtocol/contracts_CowProtocol_GPv2Settlement_events.sql`

**12. BalancerV3 start_blocktime is 11 months before first on-chain data — SQL and schema.yml both stale.**
Data starts 2024-12-05 in the warehouse. SQL and schema.yml both state 2024-01-01. There is no backfill gap (the contract was not deployed until December 2024). The stale dates mislead backfill operators about the scanning window required for full-refresh runs.

Affected: `models/contracts/BalancerV3/contracts_BalancerV3_Vault_events.sql`

### MEDIUM

**13. contracts_whitelist.csv is manually curated with no documented inclusion criteria or automation — new pools silently absent.**
The 29 UniswapV3Pool and 12 SwaprPool entries have no documented TVL threshold, token-pair filter, or coverage target. New pools deployed after the last seed update are silently absent from all downstream pool analytics. The `contracts_UniswapV3_Factory_events` PoolCreated stream exists and is decoded but is not wired to any automated pool discovery process. The May 2026 whitelist additions without a corresponding full-refresh demonstrate the operational risk.

**14. BalancerV3 ERC4626 wrapper map is a static 5-entry hardcoded VALUES list — new Aave wrappers produce NULL TVL silently.**
The downstream `stg_pools__balancer_v3_pool_tokens` resolves wrapped token addresses via a hardcoded VALUES clause covering only `waGnoWETH`, `waGnowstETH`, `waGnoUSDCe`, `waGnoGNO`, `waGnoGHO`. GHO's underlying token address is absent from `tokens_whitelist.csv`. Any new ERC4626 wrapper (new Aave v3 asset, new yield-bearing token) will produce NULL price and zero TVL for affected BalancerV3 pools without any error signal.

### LOW

**15. CoW volume_usd joins on token symbol, not address — potential cross-symbol collision risk.**
The execution/cow pipeline joins token prices via ASOF on `token symbol` rather than token address. If two tokens with different addresses share the same symbol (e.g. bridged vs native USDC variants), the price join will silently attach the wrong price. This is a latent risk, not a confirmed current failure.

---

## Data findings

All queries executed against the warehouse during the two review rounds:

| Query | Result |
|---|---|
| `contracts_BalancerV2_Vault_events` count / min / max | 25.9M rows; min_ts 2022-11-01; max_ts 2026-06-08 |
| `contracts_BalancerV3_Vault_events` count / min / max | ~5M rows; min_ts 2024-12-05; max_ts 2026-06-08 |
| `contracts_CowProtocol_GPv2Settlement_events` count / min / max | 11.3M rows; min_ts 2021-08-04; max_ts 2026-06-08 |
| `contracts_UniswapV3_Pool_events` count / min / max / by address | 22 of 29 addresses produce data; 7 silent (all added May 2026) |
| `contracts_UniswapV3_Pool_events_live` count | 0 rows (dbt scheduler gap; source healthy) |
| `contracts_BalancerV2_Vault_events_live` count | 0 rows (same cause) |
| `contracts_BalancerV3_Vault_events_live` count | 0 rows (same cause) |
| `contracts_Swapr_v3_AlgebraPool_events_live` count | 0 rows (same cause) |
| `execution_live.logs` count / min / max | 151.4M rows; min_ts 2026-03-25; max_ts 2026-06-11T07:40 UTC (source healthy) |
| `contracts_Swapr_v3_AlgebraPool_events` count / min | 2023-10-06 (confirmed — all 12 pool addresses created by new factory Oct 2023–Jan 2024) |
| `contracts_Swapr_v3_AlgebraFactory_events` count / min | 2,259 rows; min_ts 2023-09-22 |
| `DESCRIBE TABLE contracts_Swapr_v3_AlgebraPool_events` | 8 columns only — confirms no flat columns exist |
| `DESCRIBE TABLE contracts_Swapr_v3_AlgebraFactory_events` | 8 columns only — confirms no flat columns exist |
| `execution.logs` bounded query for BalancerV2 Vault, 2021-01-01 to 2022-10-31 | 0 rows — contract not deployed |
| `execution.logs` bounded query for GPv2Settlement, 2021-04-01 to 2021-08-04 | 0 rows — contract not active on Gnosis Chain |
| `execution.logs` bounded query for 12 Swapr whitelist addresses, 2022-01-01 to 2023-09-01 | 0 rows — contracts not deployed |
| `contracts_UniswapV3_Pool_events` max block_number | 46596999 (2026-06-08) — watermark past all 7 new address addition dates |
| `git log seeds/contracts_whitelist.csv` | 3 commits; initial 2026-01-09 (22 addresses); +4 on 2026-05-14; +3 on 2026-05-21 |
| NULL event_name rate on BalancerV2 and CowProtocol | 0 null event_names — ABI coverage currently complete |

All historical "start date gap" findings previously flagged as potential backfill obligations were resolved as structurally empty periods — no source data exists to recover for any of them.

---

## Pros / Cons

**Pros:**
- All 19 models are thin macro wrappers with zero hand-written join, filter, or aggregation logic — correctness risk is concentrated in two well-audited macros rather than distributed across models.
- The `execution_live.logs` source feed is confirmed healthy (151M rows, current to within hours) — the live-trades architecture is sound and the `_live` tables self-heal on the next dbt run.
- All contract addresses cross-verified against schema.yml, docs, and warehouse evidence — no phantom addresses or misidentified contracts found.
- Deployment date "gaps" for BalancerV2, Swapr V3, and GPv2Settlement confirmed as structurally correct via bounded raw-log queries — no surprise backfill obligations.
- `max_block_size` memory tuning already applied to the two highest-throughput models (UniswapV3_Pool and GPv2Settlement).
- ClickHouse Map-key access pattern used consistently and correctly by all known downstream consumers.
- `contracts_CowProtocol_GPv2AllowListAuthentication_events` feeds `stg_cow__solvers` and `contracts_Swapr_v3_AlgebraFactory_events` feeds `stg_pools__v3_pool_registry` — both previously flagged as potential orphans are confirmed with active downstream lineage.
- CoW Protocol pre/post CIP-12 fee distinction is documented and enforced in the downstream pipeline, preventing revenue overstatement.

**Cons:**
- Seven newly-whitelisted UniswapV3Pool addresses added in May 2026 have no historical data and no mechanism to acquire it without a manual full-refresh.
- All four `_live` tables go to 0 rows silently whenever the dbt scheduler gaps exceed 2 hours — no monitoring, no fallback error signal to downstream API consumers.
- schema.yml for Swapr AlgebraPool and AlgebraFactory events documents flat columns that do not exist in the physical tables — actively misleading for any engineer or tool that reads schema.yml.
- Five models carry stale `start_blocktime` values (22 months stale for BalancerV2, 11 months for BalancerV3, 19 months for all Swapr V3 models) causing wasteful full-refresh scans and misleading backfill operators.
- BalancerV2 excluded from fee_apr and TVL analytics without disclosure at the API consumer level — this affects every ecosystem-wide DEX metric.
- Curve 3pool produces no decoded swap events — Curve DEX volume is entirely absent from all analytics.
- Four models have a `unique_key` weaker than their RMT `order_by`, leaving a gap between schema test coverage and engine dedup guarantee.
- BalancerV2 and BalancerV3 Vault models lack the `max_block_size` memory tuning applied to other high-volume models.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| URGENT | Execute targeted full-refresh of `contracts_UniswapV3_Pool_events` for the 7 new whitelist addresses (4 added 2026-05-14, 3 added 2026-05-21), with start date set to each pool's first deployment block. Without this, all historical swap/mint/burn events for these pools are permanently absent. | `contracts_UniswapV3_Pool_events`, `seeds/contracts_whitelist.csv` |
| URGENT | Add zero-row alerting for all four `_live` tables (query `count(*)` on each; alert if result = 0 and `execution_live.logs` max_ts > now() - 4h). Until the dbt scheduler run frequency is confirmed below 2 hours, this is the only signal that real-time consumers are receiving empty data. | All four `_live` models |
| HIGH | Rewrite `models/contracts/Swapr/schema.yml` column lists for `contracts_Swapr_v3_AlgebraPool_events` and `contracts_Swapr_v3_AlgebraFactory_events` to reflect the actual 8-column Map-based schema. Remove all flat-column documentation that does not exist in the physical tables. | `models/contracts/Swapr/schema.yml` |
| HIGH | Correct `start_blocktime` and schema.yml `start_date` across stale models: BalancerV2 (2021-01-01 to 2022-11-01), BalancerV3 (2024-01-01 to 2024-12-01), all Swapr V3 models (2022-03-01 to 2023-09-01), Curve3PoolLP SQL (2021-01-01 to 2021-09-01), CowProtocol CoWSwapEthFlow SQL (2023-01-01 to 2023-04-01), GPv2Settlement SQL and schema.yml (2021-04-01 to 2021-08-01). | 6 model files, 4 schema.yml files |
| HIGH | Add `not_null` test on `event_name` to all 6 schema.yml files covering decode models. A missing ABI entry silently produces NULL-event rows that pass all uniqueness tests. | All 6 schema.yml files in this unit |
| MEDIUM | Align `unique_key` with RMT `order_by` by adding `block_timestamp` to `unique_key` in `contracts_UniswapV3_Factory_events`, `contracts_UniswapV3_NonfungiblePositionManager_events`, `contracts_Swapr_v3_AlgebraFactory_events`, and `contracts_Swapr_v3_NonfungiblePositionManager_events`. | 4 model SQL files |
| MEDIUM | Add `SET max_block_size = 5000` pre_hook (and reset post_hook) to `contracts_BalancerV2_Vault_events` and `contracts_BalancerV3_Vault_events`, matching the pattern already applied to UniswapV3_Pool and GPv2Settlement. | `contracts_BalancerV2_Vault_events.sql`, `contracts_BalancerV3_Vault_events.sql` |
| MEDIUM | Add explicit caveats to all `api_execution_pools_*` mart descriptions and the pools semantic model schema.yml stating that BalancerV2 is excluded from fee_apr and TVL figures. This is currently documented only in internal implementation notes, not at the API consumer level. | `models/execution/pools/marts/schema.yml` |
| MEDIUM | Define and document a whitelist coverage policy for `contracts_whitelist.csv` — minimum TVL threshold or token-pair filter — and consider wiring `contracts_UniswapV3_Factory_events` PoolCreated events to a semi-automated pool discovery flagging process. | `seeds/contracts_whitelist.csv` |
| LOW | Evaluate adding `decode_logs` coverage for Curve 3pool Swap/TokenExchange events to close the Curve DEX volume gap. If deliberately excluded (UBO-only focus), document this explicitly in schema.yml and the API layer so consumers understand Curve is absent from all DEX trading analytics. | `models/contracts/Curve/` |

---

## Review log

| Round | Agent | Challenge | Resolution |
|---|---|---|---|
| 1 | Inspector | Identified BalancerV2 Vault start date as a potential 12-month backfill gap (Nov 2021–Oct 2022) | Resolved round 2: warehouse queries confirm zero raw logs for the Vault address before 2022-11-01 — contract did not exist on Gnosis Chain until November 2022; no backfill gap |
| 1 | Inspector | Flagged 7 silent UniswapV3Pool addresses as possible backfill gaps or speculative additions | Resolved round 2: git log confirms all 7 added post-initial seed (May 2026); current watermark is past their addition dates; never processed at all — requires full-refresh, not backfill correction |
| 1 | Inspector | Identified empty `_live` tables as possible indication that the `execution_live` source feed was down | Resolved round 2: `execution_live.logs` confirmed healthy (151M rows, max_ts 2026-06-11T07:40 UTC); empty tables are a TTL+dbt-scheduler gap, not a source failure |
| 1 | Context | Flagged Swapr AlgebraPool_events and AlgebraFactory_events schema.yml as possibly aspirational | Confirmed round 2: `DESCRIBE TABLE` confirms 8-column Map-based schema only; flat-column documentation does not exist in physical tables |
| 1 | Context | Flagged Swapr 19-month data gap (start_blocktime 2022-03 vs min_ts 2023-10) as possibly missing backfill | Resolved round 2: bounded raw-log queries confirm zero activity for all 12 whitelist addresses before 2023-10 — factory and pools are a full redeployment, not a backfill gap |
| 1 | Context | Flagged CoW Protocol Apr–Aug 2021 gap as possibly a backfill omission | Resolved round 2: bounded raw-log queries confirm zero `execution.logs` rows for GPv2Settlement before 2021-08-04 — contract was not active on Gnosis Chain until August 2021; no source data to recover |
| 1 | Context | Flagged `contracts_Swapr_v3_AlgebraFactory_events` and `contracts_CowProtocol_GPv2AllowListAuthentication_events` as potentially orphaned | Resolved round 2 (via code reading): AlgebraFactory_events feeds `stg_pools__v3_pool_registry`; GPv2AllowListAuthentication_events feeds `stg_cow__solvers` — both have active downstream consumers |
