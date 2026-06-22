# Model review (revisit 2026-06-21): contracts/lending-tokens-oracles

Baseline `docs/model_review/contracts-lending-tokens.md` (2026-06-11), re-verified 2026-06-21 across 3 rounds; all `19` cases re-checked. Headline: `0` resolved, `4` downgraded/changed (C08/C10/C11/C12 — coverage-gap NULL-pricing premises refuted, chainlink wasted-scan premise refuted), `15` still confirmed at baseline-or-reconciled severity; the Agave LendingPool 63.4M-row 100%-undecoded defect (C01, critical) and the silent 59-day-stale bC3M price served downstream (C03, escalated to high) remain the two load-bearing findings.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| CONTRACTSLENDINGTOKENS-C01 | P0-07 | Agave LendingPool ABI absent; 63.4M rows 100% undecoded | critical | CONFIRMED | critical | high | none | 3 |
| CONTRACTSLENDINGTOKENS-C02 | | schema.yml documents phantom columns, omits real decode outputs | high | CONFIRMED | high | high | none | 3 |
| CONTRACTSLENDINGTOKENS-C03 | | bC3M oracle stale; root cause = on-chain deprecation; stale price served downstream | high | CONFIRMED | high | high | none | 3 |
| CONTRACTSLENDINGTOKENS-C04 | P0-07 | Agave LendingPool orphan; burns daily scan, 63M useless rows | medium | CONFIRMED | medium | high | none | 3 |
| CONTRACTSLENDINGTOKENS-C05 | | GBCDeposit/wxdai calls grain latent trace-migration risk | medium | CONFIRMED | medium | high | none | 3 |
| CONTRACTSLENDINGTOKENS-C06 | | spark Pool start_blocktime predates launch by ~1 month | medium | CONFIRMED | low | high | none | 3 |
| CONTRACTSLENDINGTOKENS-C07 | | events models order_by (block_timestamp,log_index) not provable key | low | CONFIRMED | low | high | none | 3 |
| CONTRACTSLENDINGTOKENS-C08 | | chainlink start 2021-01-01 ~1yr early (wasted scan) | low | CHANGED | low | high | none | 3 |
| CONTRACTSLENDINGTOKENS-C09 | | aGnoWXDAI seed checksum case mismatch | low | CONFIRMED | low | high | none | 3 |
| CONTRACTSLENDINGTOKENS-C10 | | bTSLA no oracle model; prices to NULL downstream | high | CHANGED | low | high | none | 3 |
| CONTRACTSLENDINGTOKENS-C11 | | osETH-ETH / STETH-USD feeds absent; positions price to NULL | high | CHANGED | low | high | none | 3 |
| CONTRACTSLENDINGTOKENS-C12 | | atoken_reserve_mapping omits 9 spTokens | medium | CHANGED | low | high | none | 3 |
| CONTRACTSLENDINGTOKENS-C13 | | 'deposists' typo in model + semantic layer degrades NL routing | medium | CONFIRMED | medium | high | none | 3 |
| CONTRACTSLENDINGTOKENS-C14 | | stablecoin peg-proxy depeg-masking undocumented in SQL | medium | CONFIRMED | medium | medium | none | 3 |
| CONTRACTSLENDINGTOKENS-C15 | | spark AToken 96K count correct (not ABI gap) | low | CONFIRMED | low | high | none | 3 |
| CONTRACTSLENDINGTOKENS-C16 | | aave/spark PoolConfigurator events orphans (no consumers) | low | CONFIRMED | low | high | none | 3 |
| CONTRACTSLENDINGTOKENS-C17 | | aaveV3 PoolInstance 3.66M rows, fresh within daily lag | low | CONFIRMED | low | high | none | 3 |
| CONTRACTSLENDINGTOKENS-C18 | | chainlink feeds 1.17M rows, 0 dups, grain clean | low | CONFIRMED | low | high | none | 3 |
| CONTRACTSLENDINGTOKENS-C19 | | aaveV3 AToken 3.01M rows, 0 dups, grain clean | low | CONFIRMED | low | high | none | 3 |

Rollup (final): confirmed `15`, changed `4`, resolved `0`, new `0`, unverifiable/unresolved `0`.

## Delta vs baseline

### RESOLVED (0)
None. No case was fixed between `2026-06-11` and `2026-06-21`; every defect re-measured as still present in code or data.

### CHANGED (4)
- **C08** (chainlink start 2021-01-01): the *wasted-scan premise is refuted*. `contracts_chainlink_feeds_events` has `740,163` rows with `block_timestamp < '2022-01-01'` (= `63%` of `1,170,388` total), so the 2021-01-01 start captures real 2021 Gnosis Chainlink feed activity, not an empty year. Severity stays `low`; status CHANGED because the rationale no longer holds. No incident.
- **C10** (bTSLA no oracle, baseline high "prices to NULL"): *NULL-price premise refuted*. `int_execution_token_prices_daily` carries `BTSLA = 472` rows through `2026-06-20` via the Dune priority-3 fallback (`stg_crawlers_data__dune_prices` BTSLA = `472` rows). Structural gap holds (no `contracts_backedfi_bTSLA_Oracle_events`, bTSLA absent from the 9-bticker `int_execution_rwa_backedfi_prices` list and from `fct_execution_rwa_backedfi_prices_daily`), but no position prices to NULL. Severity high -> `low`. No incident.
- **C11** (osETH-ETH `0xD132Cf...` / STETH-USD `0xcC5a624A...` feeds absent, baseline high): feeds genuinely absent from `contracts_chainlink_feeds_events.sql` (only `wstETH-ETH 0x6dcF8CE1982Fc71E7128407c7c6Ce4B0C1722F55` present), but `osETH` has `0` occurrences anywhere under `models/`, plain `stETH` is not whitelisted, and `wstETH` is already priced via the wstETH-ETH feed (`int_execution_prices_oracle_daily`, June rows `0` null/zero). No tracked position prices to NULL. Severity high -> `low`; future build-plan item only. No incident.
- **C12** (atoken_reserve_mapping 6 rows omits 9 spTokens, baseline medium): seed asymmetry holds (`atoken_reserve_mapping.csv` = `6` Aave V3 rows vs `lending_market_mapping.csv` = `15` rows), but the sole consumer `int_execution_accounts_non_user_contracts` *also* unions `lending_market_mapping.supply_token_address` (lines `40-41`), which contains all 9 spTokens, so no SparkLend position is unflagged. Severity medium -> `low`. No incident.

### STILL CONFIRMED (15)
- **C01** (critical, P0-07): `contracts_agave_LendingPool_events` = `63,381,065` rows, `100%` blank `event_name` (`63,381,065` blank), date `2022-04-19` to `2026-06-21`; `event_signatures` for `0x5E15d5E33d318dCEd84Bfe3F4EACe07909bE6d9c` = `0` rows. Row count grew `63,381,046` -> `63,381,065`. Round-3 refinement: this is a *per-address registration gap*, not a globally-missing ABI — `ReserveDataUpdated`/`Deposit`/`Borrow`/`Repay` ARE registered for other addresses (`2`/`4`/`2`/`2` addrs). Fix = targeted per-address seed insert. No incident.
- **C02** (high): `decode_logs` emits 8 real columns (`block_number, block_timestamp, transaction_hash, transaction_index, log_index, contract_address, event_name, decoded_params`); `aave/schema.yml` PoolInstance still lists phantom `event_type/pool_address/reserve_asset/amount/user_address` and omits `decoded_params/event_name`; same pattern across backedfi/spark/tokens/GBCDeposit. Round-3: NO phantom column carries a dbt test (only `dbt_utils.unique_combination_of_columns` on `(block_timestamp, log_index)`), so this is doc-only drift, not a test-compile failure. AToken schema is the one corrected example. No incident.
- **C03** (high, escalated from baseline open question): `contracts_backedfi_bC3M_Oracle_events` frozen at `2026-04-23` (`1,835` rows), now `59` days stale vs other 8 oracles fresh `2026-06-18..2026-06-21`. RPC corroborated: `latestRoundData()` `updatedAt = 1776937321` (= `2026-04-23 09:42 UTC`) — genuine on-chain deprecation, ingestion correct. BUT `fct_execution_rwa_backedfi_prices_daily` (a VIEW with `WITH FILL` + `last_value(price) IGNORE NULLS`) carries `1,075` bC3M rows through `2026-06-20` with NO staleness guard -> silent 59-day-stale price served downstream. No incident (predates May/June outages).
- **C04** (medium, P0-07): `0` downstream `ref()` consumers of `contracts_agave_LendingPool_events`; tags = `['production','agave','contracts','events','microbatch']`, not disabled. `cron_preview.sh` `MANDATORY_STEPS=dbt-run` over `tag:production` via `dbt_incremental_runner` -> it IS in the daily selection, burning daily `execution.logs` scan for 63M undecoded rows. No incident.
- **C05** (medium): both `contracts_GBCDeposit_calls` (`15,082` rows) and `contracts_wxdai_calls` (`765,443` rows) key on `(block_timestamp, transaction_hash)`, source `execution.transactions`, `0` duplicates today. Latent trace-migration risk only. No incident.
- **C06** (severity-reconciled medium -> low): `contracts_spark_Pool_events.sql` `start_blocktime='2023-09-05'` unchanged; `min(block_timestamp)=2023-10-06`, `0` events before launch -> pure wasted full-refresh scan, cost-only. (Verifier R3 clerically wrote `medium`; orchestrator holds `low` consistent with its own basis and R2.) No incident.
- **C07** (low): all three events models still `order_by=(block_timestamp, log_index)` not the provable `(block_number, transaction_index, log_index)`; `0` dups on the provable key (AToken `3,040,374` rows). Latent/cosmetic. No incident.
- **C09** (low): `lending_market_mapping.csv` `0xd0Dd6cEF72143E22cCed...` (lowercase `ed`) vs `atoken_reserve_mapping.csv` `0xd0Dd6cEF72143E22cCED...` (uppercase `ED`); `decode_logs` lowercases before filtering, no functional impact. No incident.
- **C13** (medium): `int_GBCDeposit_deposists_daily.sql` filename + `semantic_models.yml` name/measure/metric/label/description all carry the `deposists` typo; only synonyms `validator deposits` and `who deposited to which validator` are correctly spelled. NL routing degraded but not eliminated. No incident.
- **C14** (medium, confidence medium): `int_execution_prices_oracle_daily` feed_symbols maps `CHF_USD -> (ZCHF, svZCHF)`, `DAI_USD -> (xDAI, WxDAI)`, `USDC_USD -> (USDC, USDC.e)`; consumed as collateral/valuation truth, no comment documenting that a token-vs-peg depeg is masked. Real risk = ZCHF-vs-CHF depeg (not CHF-vs-USD FX). Bounded modeling assumption. No incident.
- **C15** (low): `contracts_spark_AToken_events` = `96,265` rows (was `96,225`), `9` addresses, `6` event types, `0` blank, `0` dups; each spToken has `7` `event_signatures` rows (6 AToken + 1 proxy). Provably an activity floor, not an ABI gap. No incident.
- **C16** (low): both `contracts_aaveV3_PoolConfigurator_events` and `contracts_spark_PoolConfigurator_events` have `0` downstream consumers; aave_cfg decodes 30+ named event types cleanly with `15`/`435` blank (small specific-event ABI gap, fixable seed gap, not malformed logs). No incident.
- **C17** (low): `contracts_aaveV3_PoolInstance_events` = `3,696,179` rows (was 3.66M), `max(block_timestamp)=2026-06-21` (fresh), `0` dups; last 15 days contiguous non-zero. Append strategy, structurally immune to insert_overwrite incident A.
- **C18** (low): `contracts_chainlink_feeds_events` = `1,170,388` rows (was 1,167,766), `max=2026-06-21`, `0` dups on both declared and provable key. No incident.
- **C19** (low): `contracts_aaveV3_AToken_events` = `3,040,374` rows (was 3,011,808), `0` dups, `max=2026-06-21`; last 15 days contiguous non-zero (`2026-06-07..06-21`), no mid-history insert_overwrite hole. Elevated `2026-06-18/19/20` counts (`3941/4044/4031`) consistent with an incident-B logs backfill top-up, not a gap.

### NEW (0)
None.

### UNVERIFIABLE / UNRESOLVED (0)
None. Every case reached agreed status with high (C14: medium) confidence.

## Evidence appendix

### C01 — Agave LendingPool undecoded (critical)
```sql
SELECT count(*), sum(event_name=''), min(toDate(block_timestamp)), max(toDate(block_timestamp))
FROM dbt.contracts_agave_LendingPool_events;
SELECT count(*) FROM dbt.event_signatures
WHERE lower(contract_address)=lower('0x5E15d5E33d318dCEd84Bfe3F4EACe07909bE6d9c');
SELECT event_name, count(*), uniqExact(contract_address) FROM dbt.event_signatures
WHERE event_name IN ('ReserveDataUpdated','Deposit','Borrow','Repay') GROUP BY event_name;
```
Returned: `63,381,065` rows; `63,381,065` blank (100%); range `2022-04-19` to `2026-06-21`; address `0x5E15...6d9c` = `0` event_signatures rows; canonical events registered for OTHER addresses (`ReserveDataUpdated`=2 addrs, `Deposit`=4, `Borrow`=2, `Repay`=2).

### C02 — phantom columns (high)
`code_only`: `macros/decoding/decode_logs.sql:562-570` emits 8 columns. `aave/schema.yml` PoolInstance lists `event_type/pool_address/reserve_asset/amount/user_address` (phantom), omits `decoded_params/event_name`; PoolConfigurator lists `sender/new_config_value/old_config_value/event_data`; `backedfi/schema.yml` bC3M lists `oracle_id/round_id/answer/answered_in_round`. Only test anywhere = `dbt_utils.unique_combination_of_columns` on `(block_timestamp, log_index)`; AToken schema is correctly documented.

### C03 — bC3M stale + carried forward (high)
```sql
SELECT max(toDate(block_timestamp)), count(*) FROM dbt.contracts_backedfi_bC3M_Oracle_events;
-- 8-way UNION max(block_timestamp) for the other oracles
SELECT bticker, count(*), max(date) FROM dbt.fct_execution_rwa_backedfi_prices_daily
WHERE bticker='bC3M' GROUP BY bticker;
```
Returned: bC3M `max=2026-04-23`, `1,835` rows; others `2026-06-18..2026-06-21`; fct mart bC3M = `1,075` rows, max date `2026-06-20`. RPC `contract_call_function latestRoundData()` on `0x83Ec02059F686E747392A22ddfED7833bA0d7cE3` -> `roundId=915, answer=12620000000, updatedAt=1776937321` (= `2026-04-23 09:42 UTC`); `rpc_get_code` = `2,151` bytes (contract still exists). Mart is a VIEW: `last_value(price) IGNORE NULLS OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` on a `WITH FILL FROM '2020-01-01' TO today() STEP 1` spine; carries the last real point to `1,075` rows; no staleness guard in int/fct/api.

### C04 — Agave orphan, daily scan (medium)
`grep ref('contracts_agave_LendingPool_events')` across `models/` + `macros/` = `0` hits (only its own schema.yml). Config tags = `['production','agave','contracts','events','microbatch']`, not disabled. `cron_preview.sh` sets `MANDATORY_STEPS=dbt-run`, execs `run_dbt_observability.sh` over `tag:production` via `dbt_incremental_runner.py`.

### C05 — calls grain (medium)
```sql
SELECT 'gbc',count(*),count(*)-uniqExact((block_timestamp,transaction_hash)),max(block_timestamp) FROM dbt.contracts_GBCDeposit_calls
UNION ALL
SELECT 'wxdai',count(*),count(*)-uniqExact((block_timestamp,transaction_hash)),max(block_timestamp) FROM dbt.contracts_wxdai_calls;
```
Returned: GBCDeposit `15,082` rows / `0` dups / max `2026-06-21`; wxdai `765,443` rows / `0` dups / max `2026-06-21`. Both `decode_calls(tx_table=source('execution','transactions'))`.

### C06 — spark pre-launch scan (low)
```sql
SELECT min(block_timestamp), sum(block_timestamp<'2023-10-06'), count(*) FROM dbt.contracts_spark_Pool_events;
```
Returned: `min=2023-10-06T15:19:55`, `0` pre-launch events, `1,402,802` total. `start_blocktime='2023-09-05'` (line 23); `lending_market_mapping.csv` SparkLend start_date `2023-10-06`.

### C07 — order_by mismatch (low)
`code_only`: all three models `order_by='(block_timestamp, log_index)'` (line 6 each). Provable-key check:
```sql
SELECT count(*)-uniqExact((block_number,transaction_index,log_index)) FROM dbt.contracts_aaveV3_AToken_events;
```
Returned: `0` (over `3,040,374` rows).

### C08 — chainlink pre-2022 volume (low, CHANGED)
```sql
SELECT count(*), sum(block_timestamp<'2022-01-01') FROM dbt.contracts_chainlink_feeds_events;
```
Returned: `1,170,388` total; `740,163` before 2022-01-01 (`63%`). `start_blocktime='2021-01-01'` (line 49). Wasted-scan premise refuted.

### C09 — checksum case (low)
`grep aGnoWXDAI / 0xd0Dd6cEF`: `lending_market_mapping.csv` = `0xd0Dd6cEF72143E22cCed4867eb0d5F2328715533` (lowercase `ed`); `atoken_reserve_mapping.csv` = `0xd0Dd6cEF72143E22cCED4867eb0d5F2328715533` (uppercase `ED`). `decode_logs.sql` lowercases both sides (lines 110,130,141-142,262). Sole consumer `int_execution_accounts_non_user_contracts` selects `lower(atoken_address)`.

### C10 — bTSLA pricing (low, CHANGED)
```sql
SELECT upper(symbol),count(*),max(date) FROM dbt.int_execution_token_prices_daily WHERE upper(symbol) IN ('BTSLA','TSLAX') GROUP BY 1;
SELECT 'dune',count(*),max(toDate(date)) FROM dbt.stg_crawlers_data__dune_prices WHERE upper(symbol)='BTSLA';
```
Returned: `BTSLA=472` rows / max `2026-06-20` (`TSLAX=536`); Dune source `BTSLA=472` / max `2026-06-20`; native=`0`, backedfi_fct=`0`. `ls models/contracts/backedfi/` = 9 oracle models, none for bTSLA; bTSLA `0x14a5f...` in `tokens_whitelist.csv` line 37 (since 2024-09-12); on-chain `totalSupply=2,200e18`.

### C11 — osETH/STETH feeds (low, CHANGED)
`grep -rln 'osETH|oseth' models/` = `0`. `contracts_chainlink_feeds_events.sql` contains only `wstETH-ETH 0x6dcF8CE1982Fc71E7128407c7c6Ce4B0C1722F55`; `osETH-ETH 0xD132Cf1dd2e1FB75c7d97d591d87D5E07A681353` and `STETH/USD 0xcC5a624A98600564992753DafF5Cdfe7a2e58f67` absent (appear only in `docs/native_token_prices_build_plan.md`). `wstETH` priced in `int_execution_prices_oracle_daily`: June `20` rows, `0` null/zero, fresh to `2026-06-20`. spwstETH `0x6C76971f...` in `lending_market_mapping.csv`.

### C12 — atoken_reserve_mapping (low, CHANGED)
`wc seeds`: `atoken_reserve_mapping.csv` = `6` Aave V3 rows (no spTokens); `lending_market_mapping.csv` = `15` rows (6 Aave + 9 Spark). `grep -rln 'atoken_reserve_mapping' models/` = 1 consumer (`int_execution_accounts_non_user_contracts`), which also unions `lending_market_mapping.supply_token_address` (lines 40-41) covering all 9 spTokens.

### C13 — deposists typo (medium)
`find ... -iname '*deposists*'`: `int_GBCDeposit_deposists_daily.sql` present. `semantic_models.yml`: name `GBCDeposit_deposists_daily`, measure/metric `GBCDeposit_deposists_daily__amount_value`, label `Gbcdeposit Deposists Daily - Amount`, description repeats `deposists`. question_synonyms include correctly-spelled `validator deposits` and `who deposited to which validator`, plus misspelled `GBCDeposit deposists daily` / `... amount`.

### C14 — peg-proxy masking (medium)
`code_only`: `contracts_chainlink_feeds_events.sql` header lists `CHF/USD (0xbe18b8F4...,0x6E2482E0...)`, `DAI/USD (0x12A6B73A...,0xb6556628...)`, `USDC/USD (0xc15288Bc...,0x30bA871E...)`. `int_execution_prices_oracle_daily.sql` feed_symbols (lines 37-43): `CHF_USD->(ZCHF,svZCHF)`, `DAI_USD->(xDAI,WxDAI)`, `USDC_USD->(USDC,USDC.e)`. No documenting comment about depeg masking.

### C15 — spark AToken activity floor (low)
```sql
SELECT count(*),count(*)-uniqExact(block_timestamp,log_index),max(toDate(block_timestamp)) FROM dbt.contracts_spark_AToken_events;
SELECT contract_name,count(*),uniqExact(contract_address) FROM dbt.event_signatures WHERE lower(contract_address) IN (9 spToken addrs) GROUP BY contract_name;
```
Returned: `96,265` rows, `0` dups, max `2026-06-19`; `AToken=54` sigs across 9 addrs (6 each) + `InitializableImmutableAdminUpgradeabilityProxy=9` (1 each) = `7` per address.

### C16 — PoolConfigurator orphans (low)
`grep ref(...)` for both = `0` downstream consumers. aave_cfg decode:
```sql
SELECT event_name,count(*) FROM dbt.contracts_aaveV3_PoolConfigurator_events GROUP BY event_name ORDER BY count(*) DESC;
SELECT count(*) FROM dbt.contracts_aaveV3_PoolConfigurator_events WHERE event_name='';
```
Returned: 30+ named types (`SupplyCapChanged=74`, `ReserveInterestRateDataChanged=73`, `BorrowCapChanged=57`, `ATokenUpgraded=26`, ...) + exactly `15` blank. spark_cfg = `144` rows, `0` blank, max `2026-02-02`.

### C17 / C18 / C19 — freshness & grain (low)
```sql
SELECT count(*),max(toDate(block_timestamp)),count(*)-uniqExact(block_timestamp,log_index) FROM dbt.<table>;
SELECT toDate(block_timestamp),count(*) FROM dbt.<table> WHERE block_timestamp>=today()-14 GROUP BY 1 ORDER BY 1;
```
- C17 `contracts_aaveV3_PoolInstance_events`: `3,696,179` rows, max `2026-06-21`, `0` dups; last 15 days contiguous.
- C18 `contracts_chainlink_feeds_events`: `1,170,388` rows, max `2026-06-21`, `0` dups on declared AND provable `(block_number,transaction_index,log_index)` key.
- C19 `contracts_aaveV3_AToken_events`: `3,040,374` rows, max `2026-06-21`, `0` dups; last 15 days `2026-06-07..06-21` all non-zero (counts `1639..4044`, today partial `331`), no mid-history gap.

## Review log (>=3 rounds per case)

- **C01**: R1 CONFIRMED critical (100% blank, 0 sigs) -> challenge: prove no downstream reads decoded liquidityRate + quantify if ABI present -> R2 CONFIRMED (0 `ref()` consumers; 0/63.38M decode) -> R3 challenge: per-address vs global ABI gap -> R3 CONFIRMED (canonical events registered for OTHER addresses; targeted seed fix).
- **C02**: R1 CONFIRMED high -> challenge: quantify blast radius vs live describe_table -> R2 CONFIRMED (5 phantom + 2 omitted real cols per PoolInstance; systemic) -> R3 challenge: do phantom columns carry tests? -> R3 CONFIRMED (no tests on phantoms; doc-only drift).
- **C03**: R1 CHANGED low (deprecation, 0 raw logs) -> challenge: RPC corroboration + downstream serve check -> R2 escalated CONFIRMED high (RPC `updatedAt=2026-04-23`; mart serves 1,075 carried-forward rows) -> R3 challenge: prove carry-forward logic + staleness guard -> R3 CONFIRMED high (`WITH FILL`+`last_value`; no guard).
- **C04**: R1 CONFIRMED medium (0 consumers, not disabled) -> challenge: confirm daily-schedule selection vs full-refresh-only -> R2 CONFIRMED (microbatch/production tags, daily append) -> R3 challenge: ground in actual cron selector -> R3 CONFIRMED (`tag:production` in `cron_preview.sh`).
- **C05**: R1 CONFIRMED medium (unique_key + execution.transactions) -> challenge: prove 0 dups today -> R2 CONFIRMED (0 dups both) -> R3 CONFIRMED (code re-read, latent trace risk).
- **C06**: R1 CONFIRMED medium -> challenge: reconcile medium vs cost-only + prove 0 pre-launch events -> R2 CONFIRMED low (`0` events 2023-09-05..2023-10-06) -> R3 CONFIRMED low (basis holds; R3 new_severity field clerically `medium`, orchestrator holds low).
- **C07**: R1 CONFIRMED low -> challenge: verify 0 dups on provable key -> R2 CONFIRMED (0 dups on `(block_number,transaction_index,log_index)`) -> R3 CONFIRMED low.
- **C08**: R1 CONFIRMED low (start unchanged) -> challenge: prove ~0 pre-2022 rows -> R2 CHANGED low (`740,163` pre-2022 = 63%, premise refuted) -> R3 CONFIRMED-as-CHANGED low (orchestrator holds CHANGED).
- **C09**: R1 CONFIRMED low -> challenge: confirm macro lowercases + no case-sensitive join -> R2 CONFIRMED (lowercased both sides; only consumer case-insensitive) -> R3 CONFIRMED low.
- **C10**: R1 CONFIRMED high -> challenge: confirm real bTSLA holdings + mart absence -> R2 CONFIRMED high (`2,200` on-chain tokens, absent from mart) -> R3 challenge: does NULL reach a served surface? -> R3 CHANGED low (Dune fallback supplies `472` price rows).
- **C11**: R1 CONFIRMED high -> challenge: confirm tracked positions price to NULL -> R2 CHANGED low (osETH untracked, wstETH already priced) -> R3 challenge: confirm zero osETH/stETH presence -> R3 CHANGED low (`0` osETH occurrences).
- **C12**: R1 CONFIRMED medium -> challenge: which models consume the seed for positions? -> R2 CHANGED low (sole consumer is non-user-contract flagger) -> R3 challenge: are spTokens flagged via another path? -> R3 CHANGED low (model also unions lending_market_mapping).
- **C13**: R1 CONFIRMED medium -> challenge: is misspelled name the exposed registry id; any 'deposits' alias? -> R2 CONFIRMED (typo is canonical; one correct synonym) -> R3 challenge: full set of correct vs misspelled indexed tokens -> R3 CONFIRMED medium (most surfaces misspelled, 2 correct synonyms).
- **C14**: R1 CONFIRMED medium -> challenge: trace peg-proxy into a collateral/valuation consumer -> R2 CONFIRMED (consumed in `int_execution_prices_oracle_daily`) -> R3 challenge: CHF-vs-USD FX vs ZCHF-vs-CHF depeg materiality -> R3 CONFIRMED medium (real risk = token-vs-peg depeg, undocumented).
- **C15**: R1 CONFIRMED low (9 addrs, 6 types, 0 blank) -> challenge: confirm 7 event_signatures per address -> R2 CONFIRMED (7 each) -> R3 CONFIRMED low (6 AToken + 1 proxy = 7).
- **C16**: R1 CONFIRMED low (0 consumers) -> challenge: confirm freshness/decode health -> R2 CHANGED low (aave_cfg `15`/`435` blank, not fully decoded) -> R3 challenge: identify blank events (ABI gap vs malformed) -> R3 CONFIRMED low (30+ types decode; 15-row specific ABI gap).
- **C17**: R1 CONFIRMED low (3.70M, 2-day fresh) -> challenge: per-day 14-day contiguity -> R2 CONFIRMED (15 contiguous days, fresh to today) -> R3 CONFIRMED low.
- **C18**: R1 CONFIRMED low -> challenge: verify provable-key grain + 14-day coverage -> R2 CONFIRMED (0 dups provable key, fresh) -> R3 CONFIRMED low.
- **C19**: R1 CONFIRMED low -> challenge: verify provable-key grain + 14-day coverage -> R2 CONFIRMED (0 dups provable key) -> R3 challenge: measure 14-day contiguity directly on this table -> R3 CONFIRMED low (15 contiguous non-zero days; elevated 06-18/19/20 = incident-B top-up, not gap).

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P1 (KEEP, critical) | Register the Agave LendingPool ABI for address `0x5E15d5E33d318dCEd84Bfe3F4EACe07909bE6d9c` in `event_signatures` (targeted per-address seed insert — canonical events already exist for other addresses), or disable the model. Without it, 63.4M rows stay 100% undecoded. | `models/contracts/agave/contracts_agave_LendingPool_events.sql` |
| P2 (ESCALATE, high) | Add a staleness guard to the bC3M price path: stop carrying a 59-day-stale price forward unbounded, or emit a freshness flag so downstream USD valuation does not silently consume a deprecated oracle. | `fct_execution_rwa_backedfi_prices_daily`, `int_execution_rwa_backedfi_prices`, `models/contracts/backedfi/contracts_backedfi_bC3M_Oracle_events.sql` |
| P2 (KEEP, high) | Fix phantom-column drift across the contracts schema.yml files: document the real 8-column decode output (`decoded_params/event_name/transaction_index`) and remove fabricated typed columns; use `contracts/aave` AToken schema as the template. | `models/contracts/{aave,backedfi,tokens,GBCDeposit}/schema.yml`, `models/contracts/spark/schema.yml` |
| P3 (KEEP, medium) | If C01 is fixed, drop the orphan; otherwise it keeps burning a daily `execution.logs` scan via `tag:production`. Resolve jointly with P1. | `models/contracts/agave/contracts_agave_LendingPool_events.sql` |
| P3 (KEEP, medium) | Rename the `deposists` typo across model file, semantic model name, measure, metric, label, and synonyms to `deposits` to restore NL routing. | `models/execution/GBCDeposit/intermediate/int_GBCDeposit_deposists_daily.sql`, `semantic/authoring/execution/GBCDeposit/semantic_models.yml` |
| P3 (KEEP, medium) | Document the stablecoin peg-proxy assumption in SQL: CHF/USD priced for ZCHF/svZCHF (etc.) masks a token-vs-peg depeg; add a comment so consumers know the ground-truth limitation. | `models/contracts/chainlink/contracts_chainlink_feeds_events.sql`, `int_execution_prices_oracle_daily` |
| P3 (KEEP, medium) | Add the latent-grain caveat: if `GBCDeposit_calls`/`wxdai_calls` ever migrate from `execution.transactions` to traces, the `(block_timestamp, transaction_hash)` unique_key will silently collapse multiple internal calls per tx. | `models/contracts/GBCDeposit/contracts_GBCDeposit_calls.sql`, `models/contracts/tokens/contracts_wxdai_calls.sql` |
| P4 (DOWNGRADE/optional) | Source-quality only: migrate bTSLA off the Dune priority-3 fallback to a native BackedFi oracle (no NULL today, so not urgent). Build osETH-ETH / STETH-USD feeds per the build plan if/when osETH or plain stETH positions appear. | `models/contracts/backedfi/`, `models/contracts/chainlink/contracts_chainlink_feeds_events.sql` |
| P4 (KEEP, low) | Cosmetic/cost cleanups: trim `spark` start_blocktime to `2023-10-06`; switch events `order_by` to the provable `(block_number, transaction_index, log_index)`; align the aGnoWXDAI checksum case across seeds; register the 1-2 missing aave PoolConfigurator event signatures (15 blanks). | `models/contracts/spark/contracts_spark_Pool_events.sql`, `models/contracts/{aave,spark,chainlink}/*_events.sql`, `seeds/{lending_market_mapping,atoken_reserve_mapping}.csv` |
| — (DROP) | Drop baseline C08 wasted-scan recommendation (chainlink 2021 start): refuted — `740,163` (63%) of rows predate 2022. Drop baseline C10/C11 NULL-pricing escalations and C12 missing-SparkLend-positions: all refuted as no live NULL/drop. | n/a (resolved-by-refutation) |
