# Build Plan — Native Token Price Feed (replace the Dune price source)

## Context

We currently ingest token prices from a Dune query into `crawlers_data.dune_prices`
(`block_date, symbol, price`), staged by
[`stg_crawlers_data__dune_prices`](../models/crawlers_data/staging/stg_crawlers_data__dune_prices.sql)
and consolidated by the hub view
[`int_execution_token_prices_daily`](../models/execution/prices/intermediate/int_execution_token_prices_daily.sql)
(`date, symbol, price`), which ~15 downstream models consume.

**Goal:** replace that external Dune dependency with a **fully native, zero-external,
historical** price feed for the whitelisted tokens
([`seeds/tokens_whitelist.csv`](../seeds/tokens_whitelist.csv)), built from data we
already index.

**Key decision reached:** the USD anchor is **Chainlink on-chain price feeds on Gnosis
Chain**, decoded from `AnswerUpdated` events exactly like the existing BackedFi RWA
model — *not* the Dune feed (external) and *not* CoW's `native_price` endpoint
(spot-only, cannot backfill). The DEX-ratio derivation is kept only for the handful of
tokens with no oracle (GBPe, BRLA, BRZ, COW, SAFE).

---

## Verification evidence (done — not assumed)

Confirmed against the live chain + our `execution.logs`:

1. **Feeds exist on Gnosis** (Chainlink `xdai-mainnet` registry).
2. **Aggregators emit `AnswerUpdated`** — topic0 `0559884fd3a460db…646fc5f`, the exact
   event [`int_execution_rwa_backedfi_prices`](../models/execution/rwa/intermediate/int_execution_rwa_backedfi_prices.sql)
   already decodes.
3. **Our `execution.logs` already holds the data**, both live (last 10 days) and
   historically (June 2023 / 2024 / 2025-26 all present; osETH only from 2025 because
   the feed itself is new).

| Feed | Proxy | Current aggregator (emits AnswerUpdated) | History verified |
|---|---|---|---|
| GNO/USD | `0x22441d81416430A54336aB28765abd31a792Ad37` | `0xcA16Ed36A7d1Ae2DC68873D62bce4f9BdCc2d378` | 2023-06 → now |
| ETH/USD | `0xa767f745331D267c7751297D982b050c93985627` | `0x059e7Bd8157e0d302dF3626E162B6C835340b311` | 2023-06 → now |
| WBTC/USD | `0x00288135bE38B83249F380e9b6b9a04c90EC39eE` | `0x5ED6A59735297Bc5D6CB4942913Ae7098E0cD703` | 2023-06 → now |
| EUR/USD | `0xab70BCB260073d036d1660201e9d5405F5829b7a` | `0x759be90a34E426042ed7d17916B78a5cD2567dd1` | 2023-06 → now |
| CHF/USD | `0xFb00261Af80ADb1629D3869E377ae1EEC7bE659F` | `0x6E2482E011EC31a1960a938791B6B4Ff5BAa3217` | 2023-06 → now |
| STETH/USD | `0x229e486Ee0D35b7A9f668d10a1e6029eEE6B77E0` | `0xcC5a624A98600564992753DafF5Cdfe7a2e58f67` | 2023-06 → now |
| wstETH-ETH (rate) | `0x0064AC007fF665CF8D0D3Af5E0AD1c26a3f853eA` | `0x6dcF8CE1982Fc71E7128407c7c6Ce4B0C1722F55` | 2023-06 → now |
| osETH-ETH (rate) | `0xc5f7665e7FdC5059B93Db8bEAB83F5ffA05Eb18e` | `0xD132Cf1dd2e1FB75c7d97d591d87D5E07A681353` | 2025-06 → now |
| USDC/USD | `0x26C31ac71010aF62E6B486D1132E266D6298857D` | (resolve at impl) | — |
| USDT/USD | `0x68811D7DF835B1c33e6EEae8E7C141eF48d48cc7` | (resolve at impl) | — |
| DAI/USD | `0x678df3415fc31947dA4324eC63212874be5a82f8` | (resolve at impl) | — |

> **Phase aggregators:** the *proxy* rotates its underlying aggregator over time; the
> address above is only the *current* phase. The current phase already reaches back to
> mid-2023 (covers most whitelist listing dates). For inception history of older tokens
> (e.g. GNO listed 2020), enumerate prior phases via the proxy's `phaseAggregators(1..N)`
> and feed the **list** of aggregator addresses to `decode_logs` (it accepts an array).

---

## Token → price-source mapping (final)

| Source mechanism | Tokens | How |
|---|---|---|
| **Chainlink USD oracle** (direct) | GNO, WETH(=ETH), WBTC | `AnswerUpdated.current / 1e8` |
| **Chainlink forex oracle** | EURe (EUR/USD), ZCHF + svZCHF (CHF/USD) | fiat-unit price ≈ stablecoin USD (peg assumption) |
| **Chainlink LST rate × ETH/USD** | wstETH, osETH | `rate/1e18 × ETH_usd`; stETH also has direct STETH/USD |
| **On-chain wrap ratio** | sDAI | vault `share_price` from [`int_yields_savings_xdai_rate_daily`](../models/execution/yields/intermediate/int_yields_savings_xdai_rate_daily.sql) × xDAI |
| **Stable peg = $1** (already in hub) | USDC, USDC.e, USDT, xDAI, WxDAI | hardcoded; can cross-check vs USDC/USDT/DAI oracles |
| **aToken/spToken inheritance** (already in hub) | 16 aGno*/sp* tokens | [`lending_market_mapping`](../seeds/lending_market_mapping.csv) → reserve price |
| **RWA oracle** (already native) | 10 bTokens | [`fct_execution_rwa_backedfi_prices_daily`](../models/execution/rwa/marts/fct_execution_rwa_backedfi_prices_daily.sql) |
| **Derived** | sGNO ≈ GNO | inherit / wrap ratio |
| **DEX/CoW ratio** (residual only) | GBPe, BRLA, BRZ, COW, SAFE | no Gnosis oracle for GBP/BRL/COW/SAFE; BRZ←BRLA fallback |

This collapses the DEX-ratio machinery from "most of the whitelist" to **5 residual tokens**.

---

## New / changed models

All under `models/execution/prices/` unless noted. Reuse, don't reinvent — the hub keeps
doing pegs, aToken inheritance, RWA union, and symbol display.

### 1. Contracts layer — `models/contracts/chainlink/contracts_chainlink_<feed>_Aggregator_events.sql`
One model per feed (or a Jinja loop, mirroring the backedfi folder). Each calls
[`decode_logs`](../macros/decoding/decode_logs.sql) on the feed's aggregator address(es):
```jinja
{{ decode_logs(
     source_table     = source('execution','logs'),
     contract_address = ['0x<current_agg>', '0x<phase2_agg>', ...],  -- phases for full history
     output_json_type = true,
     incremental_column = 'block_timestamp',
     start_blocktime  = '2021-01-01'
) }}
```
Config identical to `contracts_backedfi_*_Oracle_events` (incremental/append,
ReplacingMergeTree, monthly partition, `microbatch` tag).

### 2. `intermediate/int_execution_prices_oracle_daily.sql`
Daily price per feed from the decoded events — same shape as
`int_execution_rwa_backedfi_prices`:
```sql
SELECT '<symbol>' AS symbol,
       toStartOfDay(block_timestamp) AS date,
       argMax(toFloat64(decoded_params['current']) / pow(10, <decimals>), block_timestamp) AS price
FROM {{ ref('contracts_chainlink_<feed>_Aggregator_events') }}
WHERE event_name = 'AnswerUpdated' AND block_timestamp < today()
GROUP BY 1, 2
```
- USD feeds: `decimals=8`. Forex feeds: `decimals=8`. LST rate feeds: `decimals=18` →
  multiply by same-day ETH/USD to get USD.
- Emit base-token USD: GNO, WETH, WBTC, EURe(=EUR/USD), ZCHF(=CHF/USD), wstETH, osETH, stETH.

### 3. `intermediate/int_execution_prices_dex_ratios.sql` (residual tokens only)
Incremental, monthly insert_overwrite (mirror
[`int_execution_pools_dex_trades_raw`](../models/execution/pools/intermediate/int_execution_pools_dex_trades_raw.sql)).
**Source the UNPRICED trades** (`int_execution_pools_dex_trades_raw` + an unpriced CoW leg
built from [`stg_cow__trades`](../models/execution/cow/staging/stg_cow__trades.sql)) to avoid
the `amount_usd` circularity. For GBPe/BRLA/BRZ/COW/SAFE:
`median(ratio × bluechip_usd)` per day, `count() >= 5`, USD-size floor — Dune's logic.
CoW trades are best-execution/MEV-resistant, so prefer/weight them.

### 4. `marts/fct_execution_token_prices_native_daily.sql` — drop-in `(date, symbol, price)`
Materialize as a **full-refresh `table`** (rolling window + forward-fill rule out naive
incremental; matches `int_yields_savings_xdai_rate_daily`). CTE flow = Dune steps 5–8:
1. `direct` (priority 0): oracle USD + forex + LST×ETH + sDAI(vault) + pegs.
2. `dex_derived` (priority 1): from model #3.
3. dedup per `(date, symbol)` by priority (`row_number()`).
4. **30-day rolling median + MAD** outlier null (two stacked windowed-median CTEs — see below).
5. **Per-token spine forward-fill** via [`dim_time_spine_daily`](../models/shared/marts/dim_time_spine_daily.sql).
6. V1→V2 re-key (seed `date_start`/`date_end`, not hardcoded) + BRZ←BRLA fallback.
7. Final `(date, symbol, price)` `WHERE date < today()`.

### 5. Hub swap — [`int_execution_token_prices_daily`](../models/execution/prices/intermediate/int_execution_token_prices_daily.sql)
Change the `dune` CTE source from `ref('stg_crawlers_data__dune_prices')` to
`ref('fct_execution_token_prices_native_daily')`. **Keep** `backedfi`, `wxdai_from_xdai`,
`wrapper_prices`, `usd_pegs`, dedup, and whitelist symbol display. No downstream model changes.

---

## ClickHouse specifics (verified against repo usage)

- **Rolling median (window):** `quantileExact(0.5)(price) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW)` — works (see `fct_consensus_validators_income_total_daily`). Use `quantileExact` in windows for determinism; reserve `median` for plain GROUP BY.
- **MAD:** two stacked CTEs (a window can't reference another window) — `med30` then `mad30 = quantileExact(0.5)(abs(price-med30)) OVER (...)`; null where `mad30 > 0 AND abs(price-med30) > 3*mad30` (guard flat-price `mad30=0`).
- **Forward-fill:** `last_value(price) IGNORE NULLS OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` — explicit frame is mandatory.
- **Per-token spine:** cross join tokens × `dim_time_spine_daily` (column is `day`), bound `day >= per-token first_date`, LEFT JOIN sparse prices, then forward-fill. **Do not use `WITH FILL`** for multi-token (it fills along the whole result ORDER BY, not per partition — that's why the RWA model loops per ticker).
- **Materialization:** full-refresh `table` (MergeTree / ReplacingMergeTree, `order_by (date, symbol)`); small (tokens × days), sub-second rebuild, avoids all incremental rolling-window traps.

---

## Special handling & honest caveats

- **Forex = peg assumption.** EURe_usd ≈ EUR/USD assumes EURe holds its fiat peg. Defensible and clean; if we want depeg sensitivity, blend forex oracle with a DEX peg-deviation term. Same for ZCHF (CHF/USD).
- **No Gnosis oracle for GBP or BRL** → GBPe, BRLA, BRZ stay DEX-derived (BRZ←BRLA). COW/SAFE: DEX-derive or accept they remain thin.
- **wstETH/osETH** are *exchange-rate* feeds → must multiply by same-day ETH/USD. stETH also has a direct STETH/USD feed (cross-check).
- **Phase aggregators** needed for pre-2023 inception history (GNO/WETH/WBTC listed 2020). Start with the current phase (2023+), add earlier phases via `phaseAggregators` if downstream needs deeper history.
- **Heartbeat 86400 + deviation:** forex/LST feeds update sparsely (sometimes ~daily). Forward-fill handles this exactly as the backedfi model does — fine for a daily price model.

---

## Validation & migration (parallel-run, low-risk)

1. Build models #1–#4 **without touching the hub**; full backfill.
2. **Comparison query:** native vs Dune on `(date, symbol)` — per-symbol `abs(native/dune − 1)`
   distribution, % days within tolerance, and null-gap days. Bucket by token class.
3. **Acceptance gates:** stables/forex within ±0.5% on ≥95% of days; sDAI within ±0.1% of
   vault truth; GNO/WETH/WBTC within ±1–2% of Dune; oracle pegs exact.
4. **Cutover:** one-line source swap in the hub; watch Elementary anomaly tests + the
   comparison report 1–2 weeks; compare downstream `amount_usd` deltas in
   `int_execution_pools_dex_trades`.
5. **Decommission** the Dune prices crawler + `stg_crawlers_data__dune_prices` once nothing
   references them. **Rollback** = revert the one-line hub source swap.

---

## Open decisions (recommendations inline)

1. **EURe/ZCHF anchor:** forex oracle (recommended) vs DEX-derive vs blend. → forex oracle, optionally blended later.
2. **sDAI:** vault `share_price` (recommended) vs DAI/USD oracle × share. → vault rate.
3. **History depth:** current-phase only (2023+) first, add phase aggregators for pre-2023 if needed. → start current-phase.
4. **COW/SAFE:** DEX-derive vs drop from native (check downstream usage first).

---

## Critical files
- `models/contracts/backedfi/contracts_backedfi_bCSPX_Oracle_events.sql` — template for Chainlink aggregator decoding
- `models/execution/rwa/intermediate/int_execution_rwa_backedfi_prices.sql` — `argMax` daily oracle price pattern
- `models/execution/rwa/marts/fct_execution_rwa_backedfi_prices_daily.sql` — forward-fill + calendar pattern
- `models/execution/yields/intermediate/int_yields_savings_xdai_rate_daily.sql` — sDAI vault rate + spine forward-fill + `table` materialization
- `models/execution/pools/intermediate/int_execution_pools_dex_trades_raw.sql` — UNPRICED ratio source (no circularity)
- `macros/decoding/decode_logs.sql` — the decoder (accepts an array of aggregator addresses)
- `seeds/tokens_whitelist.csv`, `seeds/lending_market_mapping.csv` — token universe, V1/V2 split, aToken map
- `models/execution/prices/intermediate/int_execution_token_prices_daily.sql` — hub (one-line source swap)
