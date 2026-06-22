# Model review (revisit 2026-06-21): contracts/AMM-DEX

Re-verified against baseline `docs/model_review/contracts-amm-dex.md` (dated `2026-06-11`) over `3` rounds; all `16` cases (15 baseline + 1 NEW) settled **CONFIRMED** — `0` resolved, `0` changed, `16` still-confirmed, with the largest still-broken issue being BalancerV2 (the single largest AMM at `25,975,780` events) silently excluded from every served `api_execution_pools_*` volume/fee/TVL figure.

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| CONTRACTSAMMDEX-C01 | P0-05 | 7 newly-whitelisted UV3 pools never backfilled; watermark gate skips them | critical | CONFIRMED | critical | high | none | 3 |
| CONTRACTSAMMDEX-C02 | — | Swapr schema.yml documents flat columns vs 8-col `decoded_params` Map physical table | high | CONFIRMED | high | high | none | 3 |
| CONTRACTSAMMDEX-C03 | — | Four `_live` tables silently empty past 2h TTL; no alert/monitor | high | CONFIRMED | high | high | none | 3 |
| CONTRACTSAMMDEX-C04 | — | `unique_key` omits `block_timestamp` while RMT `order_by` includes it (4 models) | high | CONFIRMED | medium | high | none | 3 |
| CONTRACTSAMMDEX-C05 | — | Stale `start_blocktime` literals across 5 models cause over-scan on full-refresh | medium | CONFIRMED | medium | high | none | 3 |
| CONTRACTSAMMDEX-C06 | — | AlgebraPool/Factory `_calls` key on `(block_timestamp, tx_hash)`; second same-pool call collapses | medium | CONFIRMED | low | high | none | 3 |
| CONTRACTSAMMDEX-C07 | — | `max_block_size=5000` pre_hook missing on BalancerV2/V3 (largest tables) | medium | CONFIRMED | medium | high | none | 3 |
| CONTRACTSAMMDEX-C08 | — | `ANY LEFT JOIN` yields NULL `event_name` on unmatched topic0; no not_null test | low | CONFIRMED | low | high | none | 3 |
| CONTRACTSAMMDEX-C09 | — | BalancerV2 excluded from all `api_execution_pools_*` fees/fct; exclusion not surfaced | critical | CONFIRMED | critical | high | none | 3 |
| CONTRACTSAMMDEX-C10 | — | No Curve Swap/TokenExchange decoded; Curve DEX volume/fees entirely absent | high | CONFIRMED | high | medium | none | 3 |
| CONTRACTSAMMDEX-C11 | — | GPv2Settlement config `start_blocktime='2021-04-01'` 4mo before deployment (2021-08-04) | high | CONFIRMED | high | high | none | 3 |
| CONTRACTSAMMDEX-C12 | — | BalancerV3 config `2024-01-01` ~11mo before first data (2024-12-05) | high | CONFIRMED | high | high | none | 3 |
| CONTRACTSAMMDEX-C13 | — | `contracts_whitelist.csv` hand-curated, no criteria, no Factory->whitelist automation | medium | CONFIRMED | medium | high | none | 3 |
| CONTRACTSAMMDEX-C14 | — | Static 5-entry ERC4626 wrapper map; unmapped wrappers -> pools silently dropped from TVL | medium | CONFIRMED | high | high | none | 3 |
| CONTRACTSAMMDEX-C15 | — | CoW price join keys on token symbol not address (latent wrong-price risk) | low | CONFIRMED | low | high | none | 3 |
| CONTRACTSAMMDEX-N01 | — | Seed FILE (41 rows) diverged from deployed seed TABLE (34 rows); `dbt seed` never re-run | — (NEW) | CONFIRMED | high | high | none | 3 |

## Delta vs baseline

**RESOLVED (0)** — none. No baseline defect was fixed between `2026-06-11` and `2026-06-21`.

**CHANGED (0)** — none settled as CHANGED. Two cases showed transient/partial movement during round 1 but reverted to CONFIRMED:
- C03 flickered to CHANGED in round 1 (all four `_live` tables were populated: `uv3=423/balv2=1572/balv3=1176/swapr=1521`) but the underlying 2h-TTL + no-monitor design defect is unchanged, so it settled CONFIRMED. The round-1 populated state was a non-gap snapshot, not a fix.
- C14 was tagged CHANGED in round 1 (the wrapper map relocated from `stg_pools__balancer_v3_pool_tokens.sql` to `stg_pools__balancer_v3_token_map.sql` and GHO underlying `0xfc421ad3...` was added) but the core static-5-entry defect persists and now provably bites (see below), so it settled CONFIRMED at raised severity.

**STILL CONFIRMED (15)** — every baseline case re-verified true. Load-bearing numbers:
- **C09 (critical)** — `fct_execution_pools_daily.sql` filters `protocol IN ('Uniswap V3','Swapr V3','Balancer V3')` (line 75); `int_execution_pools_fees_daily` has fee CTEs only for UV3/Swapr/BalV3. BalancerV2 = `25,975,780` events (the single largest AMM), live since `2022-11-01`, excluded from every served figure. `models/execution/pools/marts/schema.yml:440` reads `'Uniswap V3, Balancer V2/V3, Swapr V3.'` — actively misleading that V2 is covered. No incident.
- **C01 (critical)** — `contracts_UniswapV3_Pool_events` resolves exactly `22` distinct pools (the `2026-01-09` cohort); the 7 May-2026 additions (`582f85e3, 0967d161, 52b249d0, c58f1492, e8a24962, beb0a58e, 1bb53efa`) return `count()=0`. `decode_logs.sql:234` gates `block_number > {{ _wm_bn }}` (watermark now `46806996`, `2026-06-21T07:26`), so even after `dbt seed` a normal incremental run will NOT backfill deployment-to-watermark history. No incident.
- **C10 (high)** — `contracts_Curve3PoolLP_events` event_name set = `{Transfer:696214, Approval:43086}`; `contracts_CurveGauge_events` = `{UpdateLiquidityLimit, Transfer, Deposit, Withdraw, Approval}`. Zero TokenExchange/Swap decoded anywhere; Curve DEX volume/fees entirely absent. No incident. (Confidence medium: the cited upstream swap address `0x7f90122bf0700f9e7e1f688fe926940e8839f353` is a wrong-chain mainnet/Polygon address, so raw TokenExchange volume could not be pinned to a verified Gnosis pool — but the decode-omission itself is certain.)
- **C11 (high)** — `contracts_CowProtocol_GPv2Settlement_events` min(block_timestamp) = `2021-08-04T15:21:25`; bounded `execution.logs` for the GPv2Settlement address over `2021-04-01..2021-08-03` = `0` rows. SQL `start_blocktime='2021-04-01'` (line 23) and schema.yml `start_date='2021-04-01'` are 4mo too early; not corrected. No data missing.
- **C12 (high)** — `contracts_BalancerV3_Vault_events` min = `2024-12-05T14:36:15`; SQL `start_blocktime='2024-01-01'` (line 23) and schema.yml `start_date='2024-01-01'` (line 40) ~11mo too early; not corrected. (Same fix as the BalancerV3 line-item of C05.)
- **C14 (high, raised from medium)** — `stg_pools__balancer_v3_token_map.sql` is still a static 5-entry list; wrapper `0xaf204776c7245bf4147c2612bf6e5972ee483701` (`84,785` June rows, `158,676` total) is unmapped, so only `2` distinct Balancer V3 pools survive into `fct_execution_pools_daily`. The "every token must have known metadata" filter silently DROPS pools with unmapped wrappers from served TVL.
- **C02 (high)** — both Swapr event tables are 8-col ending `decoded_params Map(String,Nullable(String))`; `models/contracts/Swapr/schema.yml` documents flat columns (AlgebraPool lines 273-315, AlgebraFactory lines 100-118) with NO `decoded_params` column. Swapr-only (UV3/Cow schemas were corrected).
- **C03 (high)** — TTL `block_timestamp + INTERVAL 2 HOUR` and self-heal `now() - INTERVAL 30 MINUTE` unchanged; no freshness/elementary monitor references the four `_live` tables (only `dbt_utils.unique_combination_of_columns`). Blast radius contained to the real-time `_live` tier — batch `api/MCP` marts read the non-live tables.
- **C04 (high->medium)** — all four models (`UniswapV3_Factory`, `UniswapV3_NonfungiblePositionManager`, `Swapr_v3_AlgebraFactory`, `Swapr_v3_NonfungiblePositionManager`) declare `order_by=(block_timestamp, transaction_hash, log_index)` but `unique_key=(transaction_hash, log_index)`. The dbt test enforces the full triple (STRONGER than the RMT key, opposite of baseline framing). `0` (tx_hash, log_index) pairs span >1 block_timestamp by construction; reorg trigger purely theoretical -> severity downgraded to medium.
- **C05 (medium)** — `start_blocktime` literals all stale: BalancerV2 `2021-01-01` vs min `2022-11-01` (~22mo); BalancerV3 `2024-01-01` vs `2024-12-05`; Swapr AlgebraPool `2022-03-01` vs `2023-10-06`; Curve3PoolLP SQL `2021-01-01` vs schema `2021-09-01`; CoWSwapEthFlow SQL `2023-01-01` vs schema `2023-04-01`. All incremental-append; no scheduled `--full-refresh` found in `scripts/*.sh` / `scripts/refresh/*.py`, so literals never bite on the daily path.
- **C06 (medium->low)** — both `_calls` models key on `(block_timestamp, transaction_hash)` in transactions mode (no `trace_address`). Full-history check: `0` transactions ever call the targeted Swapr pool `0x2de7439f...` more than once at the top-level grain. Realized blast radius = `0` rows -> severity downgraded to low.
- **C07 (medium)** — `contracts_BalancerV2_Vault_events.sql` and `contracts_BalancerV3_Vault_events.sql` pre_hook = `["SET allow_experimental_json_type = 1"]` only (line 13); UV3 Pool and GPv2Settlement carry the extra `SET max_block_size = 5000`. BalancerV2 = `25,975,780` rows = highest row count of the six event tables.
- **C08 (low)** — `decode_logs.sql:557` `ANY LEFT JOIN abi AS a`; UV3 Pool has `13` NULL/empty `event_name` rows (of `5,428,892`), all historical, `0` in 2026; BalancerV2/V3/Swapr/GPv2/Curve = `0`. No `not_null` test on `event_name` in any of the six schemas; rows do not feed numeric aggregates.
- **C13 (medium)** — `seeds/contracts_whitelist.csv` = `41` rows (`29` UV3 + `12` Swapr), no inclusion-criteria header. `contracts_UniswapV3_Factory_events` emits `136` PoolCreated events (~`114` discoverable pools silently excluded) vs `22` whitelisted; PoolCreated consumed only by `stg_pools__v3_pool_registry` (token0/1) and `int_execution_pools_fees_daily` (fee tiers), never for discovery/seed.
- **C15 (low)** — `int_execution_cow_trades.sql` ASOF LEFT JOINs `int_execution_token_prices_daily` on symbol (lines 85, 105), not address. `EURe` -> 2 addresses, `GBPe` -> 2 addresses in `stg_pools__tokens_meta`, but both are same fiat peg so realized price error ~0. Latent.

**NEW (1)**
- **N01 (high)** — root mechanism behind C01, flagged as a distinct operational defect. Deployed seed table `dbt.contracts_whitelist` = `22` UV3 + `12` Swapr (`34` rows) vs `seeds/contracts_whitelist.csv` = `29` UV3 + `12` Swapr (`41` rows). `dbt seed` was never re-run after the `2026-05-14` (`2e2ee6a5`, +4 UV3) and `2026-05-21` (`c91f2d8a`, +3 UV3) commits. Distinct from C01: N01 is fixed by `dbt seed`; C01's watermark gate still requires a `--full-refresh` to backfill the 7 pools' history even after re-seeding. No incident.

**UNVERIFIABLE / UNRESOLVED (0)** — none. One honest residual on C10: the cited swap address `0x7f90122bf0700f9e7e1f688fe926940e8839f353` is wrong-chain, so upstream raw TokenExchange volume on the live Gnosis 3pool could not be confirmed; the C10 CORE claim (no Swap/TokenExchange decoded into the warehouse) is independently certain from the decode inventory and settles CONFIRMED.

## Evidence appendix

**C01 / N01 (shared — seed table vs CSV vs events)**
```sql
SELECT contract_type, count() c, uniqExact(lower(address)) uq FROM dbt.contracts_whitelist GROUP BY contract_type;
-- UniswapV3Pool: 22 (uniqExact 22), SwaprPool: 12 (uniqExact 12)  => 34 rows deployed
SELECT uniqExact(lower(contract_address)) FROM dbt.contracts_UniswapV3_Pool_events;  -- 22
SELECT max(block_number), max(block_timestamp) FROM dbt.contracts_UniswapV3_Pool_events;  -- 46806996, 2026-06-21T07:26
```
CSV on disk = `41` data rows (`29` UV3 + `12` Swapr). The 7 added pools (`582f85e3, 0967d161, 52b249d0, c58f1492, e8a24962, beb0a58e, 1bb53efa`) each return `count()=0`. `git log -p` confirms TWO commits: `2e2ee6a5` (2026-05-14, +4) and `c91f2d8a` (2026-05-21, +3). Watermark gate: `decode_logs.sql:234` `AND block_number > {{ _wm_bn }}` where `_wm_bn = run_query("SELECT max(block_number) ... FROM this")` (line 229).

**C02 (Swapr schema drift)** — `describe_table` both Swapr event tables = 8 columns ending `decoded_params Map(String,Nullable(String))`. `models/contracts/Swapr/schema.yml`: AlgebraPool flat cols lines 273-315 (`pool_address`/`event_type`/`sender`/`recipient`/`amount0`/`amount1`/`sqrt_price_x96`/`liquidity`/`tick`/`amount0_delta`/`amount1_delta`), AlgebraFactory flat cols lines 100-118. No `decoded_params` documented for either.

**C03 (`_live` TTL + no monitor)**
```sql
SELECT 'UV3',count(),max(block_timestamp) FROM dbt.contracts_UniswapV3_Pool_events_live
UNION ALL SELECT 'BalV2',count(),max(block_timestamp) FROM dbt.contracts_BalancerV2_Vault_events_live
UNION ALL SELECT 'BalV3',count(),max(block_timestamp) FROM dbt.contracts_BalancerV3_Vault_events_live
UNION ALL SELECT 'Swapr',count(),max(block_timestamp) FROM dbt.contracts_Swapr_v3_AlgebraPool_events_live;
-- round3 counts: uv3=408, balv2=1207, balv3=1383, swapr=1232 (all populated; no gap in progress)
```
TTL `block_timestamp + INTERVAL 2 HOUR` (line 7), self-heal `now() - INTERVAL 30 MINUTE` (line 16). `grep tests/` and `schema.yml` — no freshness/elementary monitor on any `_live` table.

**C04 (RMT key)** — code-only: all four models `order_by=(block_timestamp, transaction_hash, log_index)` (L6-7), `unique_key=(transaction_hash, log_index)`. dbt test in schema.yml uses `(block_timestamp, transaction_hash, log_index)`. Warehouse: `0` (tx_hash, log_index) pairs spanning >1 distinct block_timestamp (RMT collapse-by-construction).

**C05 (stale start_blocktime)**
```sql
SELECT min(block_timestamp) FROM dbt.contracts_BalancerV2_Vault_events;        -- 2022-11-01 (config 2021-01-01, ~22mo)
SELECT min(block_timestamp) FROM dbt.contracts_BalancerV3_Vault_events;        -- 2024-12-05 (config 2024-01-01, ~11mo)
SELECT min(block_timestamp) FROM dbt.contracts_Swapr_v3_AlgebraPool_events;    -- 2023-10-06 (config 2022-03-01, ~19mo)
```
Curve3PoolLP SQL `2021-01-01` vs schema `2021-09-01`; CoWSwapEthFlow SQL `2023-01-01` vs schema `2023-04-01`. `grep` of `scripts/*.sh` / `scripts/refresh/*.py` found no scheduled full-refresh of any of the five models.

**C06 (calls key)**
```sql
SELECT count() FROM (
  SELECT transaction_hash FROM execution.transactions
  WHERE to_address='0x2de7439f52d059e6cadbbeb4527683a94331cf65'
  GROUP BY transaction_hash HAVING count()>1);  -- 0 (full history, 7.1s scan)
```
Both `_calls` models: `unique_key=(block_timestamp, transaction_hash)`, `decode_calls(tx_table=transactions)` (no `is_traces`).

**C07 (max_block_size)**
```sql
-- row-count ranking of the six event tables:
-- BalancerV2_Vault=25,975,780 (#1), GPv2Settlement=11,380,223, UV3_Pool=5,428,892,
-- BalancerV3_Vault=5,146,834, Swapr_AlgebraPool=4,347,060, Curve3PoolLP=739,300
```
`contracts_BalancerV2_Vault_events.sql:13` and `contracts_BalancerV3_Vault_events.sql:13` pre_hook = `["SET allow_experimental_json_type = 1"]` only; `contracts_CowProtocol_GPv2Settlement_events.sql:13` and UV3 Pool carry the extra `SET max_block_size = 5000`.

**C08 (NULL event_name)**
```sql
SELECT countIf(event_name IS NULL OR event_name='') n, count() tot FROM dbt.contracts_UniswapV3_Pool_events;  -- 13 / 5,428,892
-- BalancerV2=0, BalancerV3=0, Swapr=0, GPv2=0, Curve=0
```
`decode_logs.sql:557` `ANY LEFT JOIN abi AS a`. No `not_null` test on `event_name` in any of the six `schema.yml`.

**C09 (BalancerV2 exclusion)** — `fct_execution_pools_daily.sql:75` `protocol IN ('Uniswap V3','Swapr V3','Balancer V3')`; `int_execution_pools_fees_daily` fee CTEs ref `contracts_UniswapV3_Factory_events` / `contracts_Swapr_v3_AlgebraPool_events` / `contracts_BalancerV3_Vault_events` only. `BalancerV2 = 25,975,780` events. `models/execution/pools/marts/schema.yml:440` = `'Uniswap V3, Balancer V2/V3, Swapr V3.'` (misleading); lines 88/91 mention only V3. No BalancerV2-exclusion caveat in any `api_*` mart description. June 2026 `fct_execution_pools_daily` contains UV3 (7 pools)/Swapr V3 (4)/Balancer V3 (2), no V2 row.

**C10 (Curve swap omission)**
```sql
SELECT event_name, count() FROM dbt.contracts_Curve3PoolLP_events GROUP BY event_name;  -- Transfer 696214, Approval 43086
SELECT event_name, count() FROM dbt.contracts_CurveGauge_events GROUP BY event_name;    -- UpdateLiquidityLimit 12360, Transfer 7229, Deposit 4827, Withdraw 2366, Approval 2
```
`contracts_Curve3PoolLP_events.sql` line 20 targets LP token `0x1337BedC9D22ecbe766dF105c9623922A27963EC`. `search_models_by_address` for `0x7f90122b...` returns only `int_ubo_claims_curve_daily` (UBO supply), no `contracts_*` decode model. No TokenExchange/Swap anywhere.

**C11 (GPv2Settlement deploy date)**
```sql
SELECT min(block_timestamp), max(block_timestamp), count() FROM dbt.contracts_CowProtocol_GPv2Settlement_events;
-- 2021-08-04T15:21:25, 2026-06-21T07:16:45, 11,380,223
SELECT countIf(block_timestamp>='2021-04-01' AND block_timestamp<'2021-08-04')
FROM execution.logs WHERE address='0x9008d19f58aabd9ed0d60971565aa8510560ab41'
  AND block_timestamp>='2021-04-01' AND block_timestamp<'2021-09-01';  -- 0
```
SQL `start_blocktime='2021-04-01'` (line 23); schema.yml `start_date='2021-04-01'` (lines 39/91/116).

**C12 (BalancerV3 deploy date)**
```sql
SELECT min(block_timestamp), max(block_timestamp), count() FROM dbt.contracts_BalancerV3_Vault_events;
-- 2024-12-05T14:36:15, 2026-06-21T06:30:10, 5,146,834
```
SQL `start_blocktime='2024-01-01'` (line 23); schema.yml `start_date='2024-01-01'` (line 40). Same fix as the BalancerV3 entry of C05.

**C13 (manual whitelist)**
```sql
SELECT event_name, count() FROM dbt.contracts_UniswapV3_Factory_events GROUP BY event_name;
-- PoolCreated 136, FeeAmountEnabled 4, OwnerChanged 2
```
`seeds/contracts_whitelist.csv` = `41` rows, no criteria header. `grep -rln contracts_UniswapV3_Factory_events` -> only `stg_pools__v3_pool_registry.sql` and `int_execution_pools_fees_daily.sql` (fee tiers via `decoded_params['fee']`); no discovery/auto-seed consumer.

**C14 (static wrapper map)**
```sql
SELECT count() FROM dbt.stg_pools__balancer_v3_token_map;  -- 5 entries
-- wrapper 0xaf204776... mapped? 0 (NOT in map) yet appears in 84,785 June BalancerV3 Vault rows
SELECT count(DISTINCT pool_address) FROM dbt.fct_execution_pools_daily WHERE protocol='Balancer V3' AND ...;  -- only 2 BalV3 pools survive 2026-06
```
Map = `waGnowstETH / waGnoWETH / waGnoUSDCe / waGnoGNO / waGnoGHO`. GHO underlying `0xfc421ad3...` in map but absent from `seeds/tokens_whitelist.csv` (grep 0 hits). Round-2 cross-check also surfaced `0x417bc5b9...` (19 rows) unmapped.

**C15 (price join by symbol)**
```sql
SELECT token, uniqExact(token_address) n_addr FROM dbt.stg_pools__tokens_meta
WHERE token IS NOT NULL AND token!='' GROUP BY token HAVING uniqExact(token_address)>1;
-- EURe -> 2 (0x420ca0f9..., 0xcb444e90...), GBPe -> 2 (0x5cb90739..., 0x8e34bfec...)
```
`int_execution_cow_trades.sql` ASOF LEFT JOIN on symbol: line 85 (`pb.symbol = s.token_bought_symbol`), line 105 (`ps.symbol = s.token_sold_symbol`). Both collisions are same fiat peg -> realized error ~0.

## Review log (>=3 rounds per case)

- **C01** — R1 CONFIRMED (seed table 22 vs CSV 29) -> challenge: pivot from watermark to seed lag, verify git provenance + list 7 missing addresses -> R2 CONFIRMED (two commits `2e2ee6a5`/`c91f2d8a` proven; 7 addresses count()=0) -> challenge: quote watermark line, show full-refresh required post-seed -> R3 CONFIRMED (`decode_logs.sql:234` quoted; watermark `46806996`; `--full-refresh` required). critical throughout.
- **C02** — R1 CONFIRMED (8-col Map vs flat schema) -> challenge: prove Swapr-only -> R2 CONFIRMED (UV3/Cow use `decoded_params`) -> challenge: quote line ranges, confirm no `decoded_params` documented -> R3 CONFIRMED (AlgebraPool 273-315, AlgebraFactory 100-118). high throughout.
- **C03** — R1 CHANGED (all four populated 423/1572/1176/1521) -> challenge: show latent gap real + no monitor -> R2 CONFIRMED (within 2h TTL; no freshness test; severity restored high) -> challenge: size blast radius via downstream -> R3 CONFIRMED (only real-time `_live` tier affected; batch marts read non-live). low->high->high.
- **C04** — R1 CONFIRMED (order_by has block_timestamp, unique_key omits) -> challenge: confirm dbt test columns -> R2 CONFIRMED (test = full triple, STRONGER than RMT key; baseline framing inverted) -> challenge: is reorg trigger real -> R3 CONFIRMED but reorg purely theoretical, `0` realized -> orchestrator downgraded high->medium.
- **C05** — R1 CONFIRMED (all 5 literals stale) -> challenge: SQL-confirm BalancerV2 22mo + check scheduled full-refresh -> R2 CONFIRMED (incremental append; bites only on full-refresh) -> challenge: grep cron for full-refresh -> R3 CONFIRMED (no scheduled full-refresh found). medium throughout.
- **C06** — R1 CONFIRMED (key omits trace_address) -> challenge: show collapse materially possible -> R2 CONFIRMED but `0` collapses in 2026 -> challenge: check full history at source -> R3 CONFIRMED, `0` such transactions ever -> orchestrator downgraded medium->low.
- **C07** — R1 CONFIRMED (BalV2/V3 lack hook) -> challenge: rank row counts -> R2 CONFIRMED (BalancerV2 #1 at 25.98M) -> R3 CONFIRMED (re-read SQL line 13). medium throughout.
- **C08** — R1 CONFIRMED (ANY LEFT JOIN; no not_null) -> challenge: confirm null rate across all 6 -> R2 CONFIRMED (UV3=13, rest 0) -> challenge: decode the 13 topic0, confirm no aggregate feed -> R3 CONFIRMED (13 historical, none feed numeric aggregates). low throughout.
- **C09** — R1 CONFIRMED (V2 excluded from fct/fees) -> challenge: quote api mart descriptions, size blast radius -> R2 CONFIRMED (schema:440 misleads; V2 = largest AMM) -> challenge: confirm api-tier marts carry no caveat -> R3 CONFIRMED (no caveat; misleading text persists). critical throughout.
- **C10** — R1 CONFIRMED (only Transfer/Approval/Deposit/Withdraw) -> challenge: prove LP-token target + swap address absent -> R2 CONFIRMED (swap pool in no contracts_* model) -> challenge: confirm raw logs hold TokenExchange upstream -> R3 CONFIRMED core; cited address wrong-chain so upstream volume unpinned (confidence medium). high throughout.
- **C11** — R1 CONFIRMED (min 2021-08-04 vs config 2021-04-01) -> challenge: corroborate deploy via raw logs -> R2 CONFIRMED (0 pre-deploy logs) -> R3 CONFIRMED (config still 2021-04-01). high throughout.
- **C12** — R1 CONFIRMED (min 2024-12-05 vs config 2024-01-01) -> challenge: reconcile with C05, quote schema literal -> R2 CONFIRMED (same defect, fix once) -> R3 CONFIRMED (schema.yml line 40 = 2024-01-01). high throughout.
- **C13** — R1 CONFIRMED (manual, no criteria) -> challenge: negative-existence on Factory consumers -> R2 CONFIRMED (only registry + fee tiers) -> challenge: quantify discoverable pools -> R3 CONFIRMED (136 PoolCreated vs 22 whitelisted). medium throughout.
- **C14** — R1 CHANGED (map relocated, GHO mapped) -> challenge: prove residual defect bites -> R2 CONFIRMED (2 unmapped wrappers, 158,676 rows; severity raised medium->high) -> challenge: trace downstream to served TVL -> R3 CONFIRMED (only 2 BalV3 pools survive into fct; silent exclusion). medium->high->high.
- **C15** — R1 CONFIRMED (ASOF on symbol) -> challenge: query symbol->multi-address collisions -> R2 CONFIRMED (EURe/GBPe each 2 addresses) -> challenge: quantify realized error -> R3 CONFIRMED (same peg, ~0 error). low throughout.
- **N01** — R1 NEW (CSV 41 vs table 34) -> challenge: ensure not double-counting C01, prove genuine absence -> R2 CONFIRMED (7 addresses genuinely absent; distinct fix `dbt seed`) -> challenge: confirm staleness propagates downstream now -> R3 CONFIRMED (events resolve only 22 UV3 pools). high throughout.

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P0 (KEEP) | Surface BalancerV2 non-coverage in every served description, or implement V2 fee/volume. Fix `models/execution/pools/marts/schema.yml:440` which falsely claims `Balancer V2/V3` coverage. BalancerV2 = `25.98M` events, the single largest AMM, silently omitted from all `api_execution_pools_*`. | `int_execution_pools_fees_daily`, `fct_execution_pools_daily`, `models/execution/pools/marts/schema.yml`, `contracts_BalancerV2_Vault_events.sql` |
| P0 (KEEP) | Backfill the 7 new UV3 pools: (1) `dbt seed` to load the 29-row CSV into `dbt.contracts_whitelist` (fixes N01); (2) `--full-refresh` (or `refresh.py` batched rebuild) of `contracts_UniswapV3_Pool_events` to ingest deployment-to-watermark history (fixes C01). Sequential — `dbt seed` alone does NOT backfill history. | `seeds/contracts_whitelist.csv`, `contracts_UniswapV3_Pool_events.sql` |
| P1 (NEW) | Add a freshness/elementary monitor or `dbt seed` CI check that diffs the seed CSV against the deployed `contracts_whitelist` table so future additions cannot silently drift (root cause of C01/N01). | `seeds/contracts_whitelist.csv` |
| P1 (KEEP) | Decode Curve Swap/TokenExchange for the live Gnosis 3pool (verify the correct on-chain swap address first — the documented `0x7f90122b...` is wrong-chain) so Curve DEX volume/fees enter the warehouse; disclose the gap until then. | `contracts_Curve3PoolLP_events.sql`, new Curve swap decode model |
| P1 (KEEP, ESCALATED) | Replace the static 5-entry ERC4626 wrapper map with a dynamic derivation, or alert when an unmapped wrapper appears. `0xaf204776...` (`84,785` June rows) is silently dropping BalancerV3 pools from served TVL (only `2` survive). | `stg_pools__balancer_v3_token_map.sql`, `seeds/tokens_whitelist.csv` |
| P1 (KEEP) | Add a `_live`-tier freshness monitor that fires when a `_live` table goes empty past the 2h TTL; document the self-heal behavior for real-time consumers. | four `*_events_live.sql` |
| P2 (KEEP) | Correct stale `start_blocktime` / `start_date` literals to actual deployment dates (BalancerV2 `2022-11-01`, BalancerV3 `2024-12-05`, GPv2Settlement `2021-08-04`, Swapr `2023-10-06`, Curve3PoolLP `2021-09-01`, CoWSwapEthFlow `2023-04-01`) to avoid over-scan on full-refresh and end documentation drift. C05/C12 share the BalancerV3 fix. | `contracts_BalancerV2_Vault_events.sql`, `contracts_BalancerV3_Vault_events.sql`, `contracts_CowProtocol_GPv2Settlement_events.sql`, `contracts_Swapr_v3_AlgebraPool_events.sql`, `contracts_Curve3PoolLP_events.sql`, `contracts_CowProtocol_CoWSwapEthFlow_events.sql` |
| P2 (KEEP) | Correct `models/contracts/Swapr/schema.yml` to document the 8-col `decoded_params` layout (matching UV3/Cow); enables a schema-contract test that would currently fail. | `models/contracts/Swapr/schema.yml` |
| P2 (KEEP) | Add the `SET max_block_size = 5000` pre_hook to BalancerV2/V3 Vault models (the two largest tables) to prevent OOM on large full-refresh, matching UV3 Pool / GPv2Settlement. | `contracts_BalancerV2_Vault_events.sql`, `contracts_BalancerV3_Vault_events.sql` |
| P3 (KEEP, DE-ESCALATED) | Add `block_timestamp` to `unique_key` on the four Factory/NPM models so the RMT collapse key matches `order_by`. Latent/theoretical only (reorg trigger has no live path); downgraded high->medium. | `contracts_UniswapV3_Factory_events.sql`, `contracts_UniswapV3_NonfungiblePositionManager_events.sql`, `contracts_Swapr_v3_AlgebraFactory_events.sql`, `contracts_Swapr_v3_NonfungiblePositionManager_events.sql` |
| P3 (KEEP) | Add an automated UV3 Factory PoolCreated -> whitelist discovery path (or document inclusion criteria); `136` PoolCreated vs `22` whitelisted means ~`114` pools are silently excluded. | `seeds/contracts_whitelist.csv`, `contracts_UniswapV3_Factory_events.sql` |
| P3 (KEEP, DE-ESCALATED) | Switch AlgebraPool/Factory `_calls` to trace mode (`is_traces=true`) with `trace_address` in `unique_key`. Realized blast radius = `0` across full history; downgraded medium->low. | `contracts_Swapr_v3_AlgebraPool_calls.sql`, `contracts_Swapr_v3_AlgebraFactory_calls.sql` |
| P3 (KEEP) | Re-key the CoW price ASOF join on token address instead of symbol to remove the latent wrong-price risk; current realized error ~0 (EURe/GBPe same peg) so low priority. | `int_execution_cow_trades.sql`, `int_execution_token_prices_daily` |
| P4 (KEEP) | Add a `not_null` test on `event_name` (or a small-threshold warn test) to surface ABI-coverage gaps; `13` UV3 NULL/empty rows currently demonstrate the gap fires. | six `models/contracts/*/schema.yml`, `macros/decoding/decode_logs.sql` |
