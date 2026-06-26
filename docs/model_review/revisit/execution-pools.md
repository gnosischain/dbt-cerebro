# Model review (revisit 2026-06-21): execution/pools

Baseline: [`docs/model_review/execution-pools.md`](../execution-pools.md) (dated `2026-06-11`); `25` cases re-verified over `3` rounds on `2026-06-21`. Headline: `2` resolved (dev-tag CI bypass, balances full-rebuild cap), `6` changed (mostly fixed or severity right-sized), `17` still confirmed; no new issues. The two highest-impact open defects remain: the **Balancer V2 omission** silently drops the chain's oldest AMM from every ecosystem volume/fee total (`C02`/`C17`, high), and the **LVR `$500` floor gap** still violates the `net <= fee` contract (`C03`/`C04`/`C18`).

## Remediation applied (2026-06-26): insert_overwrite lookback data-loss (NEW, fixed)

Found during the CoW review (same bug class as cow `C04`/`C05`). After the `delete+insert -> insert_overwrite` migration (commit `0d261e1`, 2026-06-02), three `pools` intermediates rebuild the **whole current-month partition** but joined transaction context through a hardcoded **day-level `addDays(max,-3)`** subquery. Under `insert_overwrite` the month partition is replaced in full, so every trade/event older than `max-3` in the rebuilt month lost its `tx_from`/`tx_to`, which also broke `coalesce(taker, tx_from)`.

Confirmed **live in prod** (`dbt` db): `int_execution_pools_dex_trades` and `int_execution_pools_dex_liquidity_events` showed `100%` NULL `tx_from` for Jun 14-20 (the rebuilt-month days outside the 3-day window), clean only for the last ~3 days.

Fix: replace the day-level join filters with the strategy-aware `apply_monthly_incremental_filter(...)` macro so the join window matches the whole-month recompute. Also aligned the `-30`-day price ASOF windows (here and in cow trades) onto the same macro (`lookback_days=31`), which additionally makes them honor the `price_lookback_days` refill var.

| model | join fixed | before | after |
|---|---|---|---|
| `int_execution_pools_dex_trades_tx_context.sql` | outer `t.block_timestamp` filter (root cause) | `AND ... >= addDays(max,-3)` | `apply_monthly_incremental_filter('t.block_timestamp','block_timestamp', add_and=True)` |
| `int_execution_pools_dex_trades.sql` | `tx` LEFT JOIN + 2 price ASOF joins | `-3` (tx), `-30` (price) | macro (tx), macro `lookback_days=31` (price) |
| `int_execution_pools_dex_liquidity_events.sql` | `tx_context` CTE | `AND ... >= addDays(max,-3)` | `apply_monthly_incremental_filter(..., add_and=True)` |
| `int_execution_cow_trades.sql` (cow, consistency) | 2 price ASOF joins | `-30` | macro `lookback_days=31` |

**Prod repair required** — the code fix only prevents recurrence; the already-corrupted current-month rows must be rebuilt once. Recompute the current month for the three pools intermediates and their downstream marts, e.g. `dbt run -s int_execution_pools_dex_trades_tx_context+ int_execution_pools_dex_liquidity_events+` for the live month (the next scheduled incremental run also self-heals since the macro now re-pulls the whole month).

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | conf | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONPOOLS-C01 | - | Balancer V3 negative TVL on ERC4626-wrapper pools | critical | CHANGED | medium | high | none | 3 |
| EXECUTIONPOOLS-C02 | - | Balancer V2 entirely absent from `fees_daily` | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONPOOLS-C03 | - | `lvr_apr_7d` lacks the `$500` TVL floor; `5e19` outliers | high | CONFIRMED | medium | high | none | 3 |
| EXECUTIONPOOLS-C04 | - | LVR sign contract violated (`always <= 0` but `1,534` positive) | high | CONFIRMED | medium | high | none | 3 |
| EXECUTIONPOOLS-C05 | - | 13 trades models carry `dev` tag bypassing CI guard | high | RESOLVED | resolved | high | microbatch_insert_overwrite | 3 |
| EXECUTIONPOOLS-C06 | - | `schema.yml` column contracts diverge from model output | high | CONFIRMED | medium | high | none | 3 |
| EXECUTIONPOOLS-C07 | - | `prev_balances` reads `FROM {{ this }}` without `FINAL` (incr path) | high | CONFIRMED | medium | high | none | 3 |
| EXECUTIONPOOLS-C08 | - | Balancer V3 `token_index` from observed Swap tokens, not registry | medium | CHANGED | low | medium | none | 3 |
| EXECUTIONPOOLS-C09 | - | Hardcoded 5-entry V3 wrapper map; GHO underlying off whitelist | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONPOOLS-C10 | - | Balancer V2 staging LEFT JOIN can emit NULL `pool_address` | medium | CONFIRMED | low | medium | none | 3 |
| EXECUTIONPOOLS-C11 | - | `balances_daily` full-rebuild plain table, >100-partition cap risk | medium | RESOLVED | resolved | high | none | 3 |
| EXECUTIONPOOLS-C12 | - | `nullIf(price,0)` workaround vs `join_use_nulls`; no `not_null` test | medium | CONFIRMED | low | high | none | 3 |
| EXECUTIONPOOLS-C13 | - | `net_apr_daily` filters `fee_apr_7d IS NOT NULL` (excludes LVR-only) | medium | CHANGED | low | high | none | 3 |
| EXECUTIONPOOLS-C14 | - | V3 pool registry (RMT) joined without `FINAL` downstream | low | CONFIRMED | low | medium | none | 3 |
| EXECUTIONPOOLS-C15 | - | 4 `fct_execution_trades_by_*`/lifetime tables: no engine/order_by | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONPOOLS-C16 | - | `lps_count_7d` non-standard `granularity:last_7d` tag | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONPOOLS-C17 | - | Ecosystem volume/fee totals understate by entire Balancer V2 | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONPOOLS-C18 | - | LVR sign contract wrong; net APR can exceed fee APR for LPs | high | CHANGED | medium | high | none | 3 |
| EXECUTIONPOOLS-C19 | - | Unique-LP counts inconsistent and exclude Balancer V3 | medium | CHANGED | medium | high | none | 3 |
| EXECUTIONPOOLS-C20 | - | No MetricFlow metrics for any pool KPI (`metrics: []`) | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONPOOLS-C21 | - | 46.8% of Balancer V2 balance rows have null `price_usd` | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONPOOLS-C22 | - | `trades_lifetime` sums at hop grain but doc says per-tx | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONPOOLS-C23 | - | Swapr V3 no-Fee-event pools report `fee_apr_7d = 0` not NULL | low | CONFIRMED | low | medium | none | 3 |
| EXECUTIONPOOLS-C24 | - | "Direct" aggregator label conflates single-hop router calls | low | CONFIRMED | low | medium | none | 3 |
| EXECUTIONPOOLS-C25 | - | Data findings snapshot (warehouse, 2026-06-11) | medium | CHANGED | low | high | microbatch_insert_overwrite | 3 |

Rollup: `2` RESOLVED, `6` CHANGED, `17` STILL CONFIRMED, `0` NEW, `0` unverifiable/unresolved.

## Delta vs baseline

### RESOLVED (2)

- **`EXECUTIONPOOLS-C05`** — dev-tag CI bypass closed. All 9 `api_execution_trades_stats_*` views, 4 `fct_execution_trades_*` tables, and `int_execution_trades_by_tx` now carry the `production` tag (`0` `dev` tags). `models/execution/pools/intermediate/int_execution_trades_by_tx.sql` is now `microbatch`/`production`, fresh to `2026-06-20` (was stale at `2026-06-10`), `9,621,597` rows (was `9.49M`). Incident attribution: `microbatch_insert_overwrite` (freshness recovery). Residual nit: the `api_*` views carry a bare `api` tag (not `api:/granularity:/tier:`), a separate C16-adjacent convention gap, not the named defect.
- **`EXECUTIONPOOLS-C11`** — full-rebuild cap risk removed. `models/execution/pools/intermediate/int_execution_pools_balances_daily.sql` now declares `partition_by='toStartOfMonth(date)'` (baseline reported none). `countDistinct(toStartOfMonth(date)) = 42` months, well under the `100`-partitions-per-insert cap, so a single-shot rebuild writes 42 partition blocks and never trips code 252. It is a plain `UNION ALL` with no `{% for %}` batching loop. Monitor at ~month 100 (~2029).

### CHANGED (6)

- **`EXECUTIONPOOLS-C01`** (critical -> medium) — the catastrophic wrapper pool is durably fixed: pool `0x6e6bb...` now shows `tvl_component_usd` POSITIVE `+$17.19` with zero negative `token_amount` (was `-710 wstETH` / `-$2.1M/day`). Worst single-row negative is now `-$13,067/day`, ~160x smaller. But negatives are a standing monthly condition (`63`/`67`/`69` pools in Apr/May/Jun) and DO reach the mart: `36` "complete" pools carried `-$38,402` over 7d. Share rose `28.1% -> 30.1%` of V3 (`5,808 -> 27,311` rows, table grew). Partial fix; residual small-magnitude negatives persist.
- **`EXECUTIONPOOLS-C08`** (medium -> low) — risk mooted. `stg_pools__balancer_v3_pool_tokens.sql` line 16 still derives `token_index` via `ROW_NUMBER()` over observed Swap tokens, but `int_execution_pools_balancer_v3_daily.sql` (lines 34-109) now resolves `token_index` from the authoritative `PoolRegistered` `tokenConfig`, and grep finds **no production consumer** of the ROW_NUMBER staging model in `models/execution/pools`. The misaligning path is effectively dead.
- **`EXECUTIONPOOLS-C13`** (medium -> low) — filter is inert, not material. `api_execution_pools_net_apr_daily.sql` line 16 still filters `fee_apr_7d IS NOT NULL`, but `net_apr_7d` is computed only when BOTH `fee_apr_7d_raw` and `lvr_apr_7d_raw` are non-null, so `countIf(fee_apr_7d IS NULL AND net_apr_7d IS NOT NULL) = 0` table-wide AND `= 0` for Balancer V3. The named LVR-only/sub-3-day population does not exist. Cosmetic filter-choice.
- **`EXECUTIONPOOLS-C18`** (high -> medium) — defect real but live magnitude negligible. `1,256` rows in `fct_execution_pools_daily` have `net_apr_7d > fee_apr_7d`, but over the last 30d only `72` rows exceed fee, by a median `+0.061 pp` and worst `+0.3644 pp`. At the consumer `fct_execution_yields_opportunities_latest`, `1` of `11` LP rows shows net>fee at ~`0` excess. The contract violation is live but the inflated-yield exposure is near-zero.
- **`EXECUTIONPOOLS-C19`** (medium, status CHANGED) — partially fixed. `int_execution_pools_lps_daily` now covers `3` protocols including Balancer V3 (`139` BV3 pools, `1,403` rows). But `fct_execution_pools_lps_latest` `pool_token_map` (lines 36-48) still joins ONLY `stg_pools__v3_pool_registry` (`0` Balancer V3 rows), dropping all `139` BV3 pools. `api_execution_pools_lp_activity_daily` surfaces BV3 while `lps_latest` reports `0` BV3 LPs for the same date.
- **`EXECUTIONPOOLS-C25`** (medium -> low) — snapshot re-measured; data healthy. Mart lag recovered `3 days (max 2026-06-08) -> 1 day (max 2026-06-20)`; `int_execution_trades_by_tx` advanced `2026-06-10 -> 2026-06-20`; grain duplicates still `0`. Incident attribution `microbatch_insert_overwrite` (matches the June 2026 insert_overwrite-wipe blast radius/dates). Worst V3 negative TVL collapsed `-$2.1M -> -$13K` (see C01).

### STILL CONFIRMED (17)

- **`EXECUTIONPOOLS-C02`** (high) — `int_execution_pools_fees_daily` has exactly `3` protocols (Uniswap V3, Swapr V3, Balancer V3); Balancer V2 fee rows `= 0` of `40,084` while holding `886,660` balances rows (`86.9%`). V2 = `$1.02M` of `$31.2M` 30d ecosystem volume (`3.27%`) but `290,777` swaps (the single largest swap-count protocol). V2 is `99.83%` priced on the volume side so the gross-up is negligible.
- **`EXECUTIONPOOLS-C03`** (medium) — `fct_execution_pools_il_daily.sql` line 18 still guards only `tvl_usd_7d_avg <= 0` (no `$500` floor that `metrics_daily` line 94 has). Pool `0x22eb73...` still emits `lvr_apr_7d = 5.02e19` on `2026-01-14`. Containment is structural via the top-pool-by-30d-TVL gate (that pool has `0` rows in `fct_execution_pools_daily`), not windowed averaging; worst lvr reaching the mart is `+22.37`.
- **`EXECUTIONPOOLS-C04`** (medium) — `fct_execution_pools_daily`: `1,534` positive `lvr_apr_7d`, `11,284` negative, `2,204` null; max `+22.37`, min `-237.32`; `1,256` rows have `net>fee`. `schema.yml` line 125 (and 163) still reads verbatim `Always <= 0`. No `not_null`/`accepted_range`/`expression` test guards it; only elementary anomaly tests (warn).
- **`EXECUTIONPOOLS-C06`** (medium) — residual scope is one model. `api_execution_pools_lp_activity_daily.sql` (lines 36-42) emits `(date, token, label, type, value)` but `schema.yml` (lines 359-374) documents `(date, token, mints, burns)`. `hop_distribution`/`size_distribution`/`net_flow` were reconciled (`time_window` + `bucket_order` now documented); the `trades_lifetime` sub-claim is deduped to C22.
- **`EXECUTIONPOOLS-C07`** (medium) — all four protocol daily models still read `prev_balances FROM {{ this }}` without `FINAL` on the incremental branch (`balancer_v2_daily` L110-120, `swapr_v3_daily` L170-180, `balancer_v3_daily` L216-226), while the backfill branch uses `FINAL`. Grain dups `= 0` across `1,020,418` rows; drift unobservable, latent-only (right-sized high -> medium).
- **`EXECUTIONPOOLS-C09`** (medium) — `stg_pools__balancer_v3_token_map.sql` line 16 still hardcodes 5 wrappers; GHO underlying `0xfc421ad3c883bf9e7c4f42de845c4e4405799e73` is absent from `seeds/tokens_whitelist.csv`. Live impact now measured: `2` GHO pools, `598` pool-day rows, all `598` with `token IS NULL` and `price_usd IS NULL`, failing the `balancer_v3_complete_pools` filter and dropping from `fct`.
- **`EXECUTIONPOOLS-C10`** (low) — both `stg_pools__dex_liquidity_balancer_v2.sql` (L42-43) and `stg_pools__dex_trades_balancer_v2.sql` (L29-30) LEFT JOIN the registry on `pool_id` (guarding `pool_id`, not `pool_address`); downstream filters only `amount_raw > 0`. Liquidity-side NULL `pool_address` = `0` over 7d (`342` rows). Trades-side bound never measurable within budget (timed out twice) — structurally latent-low. Since V2 is excluded from the user-facing pool marts (C02/C17), orphans don't reach consumers.
- **`EXECUTIONPOOLS-C12`** (low) — `fct_execution_pools_tvl_token_daily.sql` lines 50-51 still use `nullIf(p0.price,0)`/`nullIf(p1.price,0)` instead of the `join_use_nulls` hook; no `not_null` test on `tvl_in_token0/token1`. Output null-clean (`0`/`1,408` over last 30d); price source never emits `0` (min `0.18`). Inert-but-safe.
- **`EXECUTIONPOOLS-C14`** (low) — `stg_pools__v3_pool_registry` (RMT) joined without `FINAL` at 6 sites across 5 models (`fct_execution_pools_daily` L36/48/180, `int_execution_pools_fees_daily` L238, `int_execution_pools_il_swap_flows_daily` L106, `fct_execution_pools_lps_latest` L38/45). Registry is `2,397` rows / `2,397` unique pools (`0` dups). Latent fan-out risk.
- **`EXECUTIONPOOLS-C15`** (low) — all four `fct_execution_trades_by_aggregator/protocol/token_daily` + `fct_execution_trades_lifetime` use `config(materialized='table', tags=[...])` with NO engine and NO order_by (lines 1-6 each), unlike peers using `ReplacingMergeTree()` + order_by. Full-rebuild so no dup risk; consistency nit.
- **`EXECUTIONPOOLS-C16`** (low) — `api_execution_pools_lps_count_7d.sql` line 4 uses `granularity:last_7d` while siblings (`api_execution_pools_fees_7d`, `volume_7d`) use `granularity:snapshot`. `last_7d` is in `POINT_GRANS` so CI passes; selector-consistency nit.
- **`EXECUTIONPOOLS-C17`** (high) — `fct_execution_pools_daily` has exactly `3` protocols (V2 absent), max_date `2026-06-20`, `15,896` rows. The API tiles `api_execution_pools_volume_daily`/`api_execution_pools_tvl_daily` are pure SELECTs from `fct` with no V2 UNION, so they literally serve a V2-less total. Kept separate from C02 (C02 = `fees_daily` pipeline root cause; C17 = consumer-surface understatement). V2 = `3.3%` USD / `86.9%` of balances rows.
- **`EXECUTIONPOOLS-C20`** (medium) — `semantic/authoring/execution/pools/semantic_models.yml` line 108 is `metrics: []` verbatim; only graph-edge models (`lp_edges`, `token_edges`) + a measure-less `execution_pools_balances_daily` candidate. Every fee-APR/TVL/volume/LVR question routes to `semantic_coverage_gap` and raw SQL.
- **`EXECUTIONPOOLS-C21`** (medium) — `int_execution_pools_balances_daily` null `price_usd` = `441,488` (`43.3%`); Balancer V2 null-price = `415,368` (`94.1%` of all nulls, `46.8%` of V2 rows). Dominated by NULL-token BPT/LP (`414,365`) and minor RWA tokens (bCSPX, bTSLA, etc.) that are mostly IN the whitelist but lack price-feed coverage. Coverage gap, not code defect (the only genuine whitelist miss is GHO, owned by C09).
- **`EXECUTIONPOOLS-C22`** (medium) — `fct_execution_trades_lifetime.sql` lines 14-17 sum `amount_usd` at hop/swap grain (`$4,767,119,426`) while `schema.yml` line 503 still says `All-time sum of per-tx trade_usd`; tx-grain equivalent is `$2,845,049,422` (`1.68x` inflation). Per-pool ratios `1.26-1.44` across all top-5 pools (broad, not concentrated). Owns the C06 trades_lifetime sub-claim.
- **`EXECUTIONPOOLS-C23`** (low) — `int_execution_pools_swapr_v3_daily.sql` line 86 (`coalesce(ff.first_fee_ppm, 0)`) and `int_execution_pools_fees_daily.sql` line 114 mean no-Fee-event pools coalesce to `fee_ppm = 0`, yielding `fee_apr_7d = 0` not NULL. Live footprint: only `2` Swapr V3 rows show `fee_apr_7d = 0` (vs `5,806` >0, `234` NULL).
- **`EXECUTIONPOOLS-C24`** (low) — `int_execution_trades_by_tx.sql` lines 90-94: `multiIf(project_label IS NOT NULL, label, hop_count>=2, 'Other Router', 'Direct')`. `2,533,704` "Direct" rows (`26.3%`), `100%` single-hop-unlabeled — the exact population that may include simple router calls misclassified as direct. Labeling-precision nit.

### NEW (0)

None.

### UNVERIFIABLE / UNRESOLVED (0)

None. One open question persists (not blocking): the trades-side null-`pool_address` bound for `stg_pools__dex_trades_balancer_v2` (C10) was never measured across 3 rounds — the query timed out twice. The liquidity-side count is `0` and the code path is identical, so the risk is treated as structurally latent-low.

## Evidence appendix

### LVR / contract cluster (C03, C04, C18) — `fct_execution_pools_daily`, `fct_execution_pools_il_daily`

```sql
SELECT countIf(lvr_apr_7d>0), countIf(lvr_apr_7d<0), countIf(lvr_apr_7d IS NULL),
       max(lvr_apr_7d), min(lvr_apr_7d), countIf(net_apr_7d>fee_apr_7d)
FROM fct_execution_pools_daily
```
Returned: `1,534` positive, `11,284` negative, `2,204` null; max `+22.37`, min `-237.32`; `net>fee` = `1,256`.

```sql
SELECT date, lvr_apr_7d FROM fct_execution_pools_il_daily
WHERE startsWith(pool_address,'0x22eb73') AND lvr_apr_7d>1e6;        -- 5.02e19 on 2026-01-14
SELECT count() FROM fct_execution_pools_daily WHERE startsWith(pool_address,'0x22eb73');  -- 0
```
`il_daily` 5.02e19 outlier persists; pool has `0` rows in `fct` (top-pool gate contains it). `fct_execution_pools_il_daily.sql` line 18 guards only `<= 0`.

C18 last-30d magnitude:
```sql
SELECT countIf(net_apr_7d>fee_apr_7d), max(net_apr_7d-fee_apr_7d),
       quantile(0.99)(net_apr_7d-fee_apr_7d), quantile(0.5)(net_apr_7d-fee_apr_7d)
FROM fct_execution_pools_daily WHERE net_apr_7d>fee_apr_7d AND date>=today()-30
```
Returned: `72` rows, max `+0.3644 pp`, p99 `+0.3644 pp`, median `+0.061 pp`. Consumer `fct_execution_yields_opportunities_latest`: `1` of `11` LP rows net>fee, ~0 excess.

### Balancer V2 omission (C02, C17) and pricing gap (C21)

```sql
SELECT groupUniqArray(protocol), countIf(protocol='Balancer V2'), count()
FROM int_execution_pools_fees_daily
```
Returned: `3` protocols (Uniswap V3, Swapr V3, Balancer V3); Balancer V2 = `0` of `40,084`.

```sql
SELECT countDistinct(protocol), groupUniqArray(protocol), max(date)
FROM fct_execution_pools_daily
```
Returned: `3` protocols, V2 absent, max `2026-06-20`, `15,896` rows. 30d V2 volume `$1.02M` of `$31.2M` (`3.27%`), `290,777` swaps, V2 priced `99.83%`.

```sql
SELECT countIf(price_usd IS NULL), round(100.0*countIf(price_usd IS NULL)/count(),1),
       countIf(protocol='Balancer V2' AND price_usd IS NULL)
FROM int_execution_pools_balances_daily
```
Returned: `441,488` null (`43.3%`); Balancer V2 null-price `415,368` (`94.1%` of nulls). Whitelist grep: GHO underlying `0xfc421...e73` ABSENT; sDAI/bCSPX/bTSLA/bMSTR/bNVDA/bCOIN/GBPe present.

### Balancer V3 negative TVL (C01) — `int_execution_pools_balances_daily`

```sql
SELECT toStartOfMonth(date) mo, countDistinct(pool_address), round(sum(tvl_component_usd),0), round(min(tvl_component_usd),0)
FROM int_execution_pools_balances_daily
WHERE protocol='Balancer V3' AND tvl_component_usd<0 AND date>=toDate('2026-04-01') GROUP BY mo
```
Returned: Apr `63` pools / `-$543K` (worst `-$4,260`); May `67` pools / `-$1.15M` (worst `-$13,067`); Jun `69` pools / `-$39.8K` (worst `-$10,500`). V3 neg rows `27,311` (`30.1%` of `90,788`). Pool `0x6e6bb...` now `+$17.19`, no negative `token_amount`.

### GHO whitelist drop (C09)

```sql
SELECT countDistinct(pool_address), count(), countIf(token IS NULL), countIf(price_usd IS NULL)
FROM int_execution_pools_balances_daily
WHERE protocol='Balancer V3'
  AND (token_address='0xfc421ad3c883bf9e7c4f42de845c4e4405799e73'
       OR token_address='0x58d9acac48a4077e4909181c48decd00e5ba5de4')
```
Returned: `2` pools, `598` pool-day rows, all `598` NULL token AND NULL `price_usd` -> dropped from `fct` via `balancer_v3_complete_pools`.

### LP-count inconsistency (C19)

```sql
SELECT countDistinct(protocol), groupUniqArray(protocol) FROM int_execution_pools_lps_daily
```
Returned: `3` protocols incl Balancer V3 (`139` BV3 pools, `1,403` rows). `stg_pools__v3_pool_registry` Balancer V3 rows = `0`, so `fct_execution_pools_lps_latest.pool_token_map` (lines 36-48, registry-only INNER JOIN) contributes `0` BV3 LPs.

### Trades-lifetime grain (C22)

```sql
SELECT sum(amount_usd) hop, /* tx-grain */ ... FROM int_execution_pools_dex_trades;
SELECT sum(trade_usd) tx FROM int_execution_trades_by_tx;
```
Returned: hop-grain `$4,767,119,426` vs tx-grain `$2,845,049,422` = `1.676x`. `lifetime_trade_count` = `9,621,597` (tx grain). Top-5 pool ratios `1.261/1.258/1.351/1.439/1.408`. `schema.yml` line 503 still `All-time sum of per-tx trade_usd`.

### Snapshot re-measure (C25)

```sql
SELECT count(), max(date), count()-uniqExact(date,protocol,pool_address,token_address)
FROM int_execution_pools_balances_daily   -- 1,020,418 / 2026-06-20 / 0
-- fct_execution_pools_daily: 15,896 / 2026-06-20 / 0
-- int_execution_trades_by_tx: 9,621,597 / 2026-06-20 / 0; null trade_usd 178,155 (1.85%)
```

### Code-only confirmations (C05, C07, C08, C10, C11, C12, C14, C15, C16, C20, C23, C24)

- C05: `int_execution_trades_by_tx.sql` lines 12-24 `tags=['production',...,'microbatch']`, `0` `dev` tags across all 13 trades models.
- C07: `prev_balances` incremental branch `FROM {{ this }}` (no FINAL) in all 4 daily models; backfill branch uses `FINAL`.
- C08: `stg_pools__balancer_v3_pool_tokens.sql` line 16 ROW_NUMBER over swap tokens; `int_execution_pools_balancer_v3_daily.sql` L34-109 uses PoolRegistered `tokenConfig`; no production consumer of the staging model.
- C10: `stg_pools__dex_liquidity_balancer_v2.sql` L42-43 / `stg_pools__dex_trades_balancer_v2.sql` L29-30 LEFT JOIN on `pool_id`, no `pool_address` guard.
- C11: `int_execution_pools_balances_daily.sql` config `materialized='table'`, `partition_by='toStartOfMonth(date)'`; `42` distinct months.
- C12: `fct_execution_pools_tvl_token_daily.sql` L50-51 `nullIf(price,0)`; no `not_null` test on `tvl_in_token0/token1`.
- C14: grep — `stg_pools__v3_pool_registry` joined without `FINAL` at 6 sites; registry `2,397`/`2,397` unique.
- C15: 4 trades marts `config(materialized='table', tags=[...])`, no engine/order_by.
- C16: `api_execution_pools_lps_count_7d.sql` line 4 `granularity:last_7d`.
- C20: `semantic_models.yml` line 108 `metrics: []`.
- C23: `int_execution_pools_swapr_v3_daily.sql` line 86 `coalesce(ff.first_fee_ppm, 0)`; `2` live `fee_apr_7d=0` rows.
- C24: `int_execution_trades_by_tx.sql` L90-94 `'Direct'` fallthrough; `2,533,704` rows (`26.3%`), all single-hop.

## Review log (>=3 rounds per case)

- **C01**: r1 CHANGED/medium (catastrophic pool fixed, `27,311` V3 neg rows) -> challenge: quantify 7d blast radius + completeness filter -> r2 CHANGED/high (`-$38,402`/7d via 36 complete pools reaches mart, share 28.1%->30.1%) -> challenge: confirm standing not transient (monthly) -> r3 CHANGED/medium (Apr/May/Jun 63/67/69 pools; worst never re-approaches `-$2.1M`).
- **C02**: r1 CONFIRMED/high (3 protocols, V2=0 fee rows) -> challenge: quantify consumer-side magnitude -> r2 CONFIRMED/high (V2 `$1.02M`/30d, 47% of swaps) -> challenge: gross-up for null-price -> r3 CONFIRMED/high (V2 priced `99.83%`, gross-up negligible, `3.3%` USD).
- **C03**: r1 CONFIRMED/high (`il` max `5.0e19`, no `$500` floor) -> challenge: trace into fct after averaging -> r2 CONFIRMED/medium (5e19 contained, fct max `+22.37`) -> challenge: which mechanism contains it -> r3 CONFIRMED/medium (top-pool gate, pool `0x22eb73` has 0 fct rows).
- **C04**: r1 CONFIRMED/high (`1,534` pos, schema `Always <= 0`) -> challenge: check LP-facing latest-date surface -> r2 CONFIRMED/high (live at yields table) -> challenge: confirm doc untouched + test gap -> r3 CONFIRMED/medium (doc verbatim unchanged, no `not_null`/`accepted_range` test).
- **C05**: r1 RESOLVED (0 dev tags) -> challenge: confirm CI guard exercises them -> r2 RESOLVED (bare `api` tag escapes guard but dev-bypass closed) -> r3 RESOLVED (microbatch, fresh to `2026-06-20`).
- **C06**: r1 CHANGED/medium (hop/size/net_flow fixed; lp_activity phantom mints/burns remain) -> challenge: dedup trades_lifetime to C22, confirm net_flow reconciled -> r2 CHANGED/medium (only lp_activity remains) -> challenge: pin side-by-side lines -> r3 CONFIRMED/medium (SQL `label/type/value` vs doc `mints/burns`).
- **C07**: r1 CONFIRMED/high (no FINAL incremental branch, all 4) -> challenge: probe grain dups for drift -> r2 CONFIRMED/medium (dup_grain=0, latent) -> challenge: monotonicity of cumulative series -> r3 CONFIRMED/medium (no observable drift, latent holds).
- **C08**: r1 CONFIRMED/low (staging ROW_NUMBER; balances uses registry) -> challenge: trace live consumer + find misalignment -> r2 CONFIRMED/low (only liquidity path, limited impact) -> challenge: sample misalignment instance -> r3 CHANGED/low (no production consumer of staging model; risk mooted).
- **C09**: r1 CHANGED/medium (GHO in map, absent from whitelist) -> challenge: measure dropped pool-days -> r2 CONFIRMED/medium (mechanism sound, count unverified - INSUFFICIENT) -> challenge: settle pool-day count -> r3 CONFIRMED/medium (`2` pools / `598` rows NULL token+price).
- **C10**: r1 CONFIRMED/low (LEFT JOIN, 0 NULL downstream) -> challenge: check source-side NULLs -> r2 CONFIRMED/low (liquidity 0/342; trades timed out) -> challenge: 7d trades-side bound -> r3 CONFIRMED/low (trades-side still timed out; code path identical, latent).
- **C11**: r1 CONFIRMED/low (table, no incremental_strategy) -> challenge: count distinct months vs cap -> r2 RESOLVED (partition_by present, 42<100) -> challenge: confirm single-shot no batching loop -> r3 RESOLVED (plain UNION ALL, no `{% for %}`).
- **C12**: r1 CONFIRMED/medium (`nullIf`, no test) -> challenge: does price source emit 0/default -> r2 CONFIRMED/low (source never emits 0, min 0.18) -> challenge: output null-clean over 30d -> r3 CONFIRMED/low (`0`/`1,408` null).
- **C13**: r1 CONFIRMED/medium (`fee_apr_7d IS NOT NULL` filter) -> challenge: measure lvr-only rows dropped -> r2 CHANGED/low (0 dropped; net requires both) -> challenge: confirm 0 for Balancer V3 -> r3 CHANGED/low (0 BV3, cosmetic).
- **C14**: r1 CONFIRMED/low (RMT joined no FINAL) -> challenge: prove registry 1 row/pool -> r2 CONFIRMED/low (2,397/2,397 unique) -> r3 CONFIRMED/low (6 join sites, latent).
- **C15**: r1 CONFIRMED/low (no engine/order_by) -> challenge: dup risk vs nit -> r2 CONFIRMED/low (full-rebuild, no dup) -> r3 CONFIRMED/low (consistency nit).
- **C16**: r1 CONFIRMED/low (`granularity:last_7d`) -> challenge: enumerate sibling grans -> r2 CONFIRMED/low (siblings use snapshot) -> r3 CONFIRMED/low (CI passes, nit).
- **C17**: r1 CONFIRMED/high (3-protocol guard, V2 absent) -> challenge: identify distinct consumer vs C02 -> r2 CONFIRMED/high (api volume/tvl tiles serve V2-less totals) -> challenge: finalize dedup decision -> r3 CONFIRMED/high (kept separate from C02).
- **C18**: r1 CONFIRMED/high (`1,534` pos lvr -> net>fee) -> challenge: verify yields-table consumption -> r2 CONFIRMED/high (1 of 11 LP rows live) -> challenge: standing 30d magnitude -> r3 CHANGED/medium (72 rows, median `+0.06pp`, negligible).
- **C19**: r1 CONFIRMED/medium (lps_latest registry-only) -> challenge: quantify BV3 gap -> r2 CONFIRMED/medium (139 BV3 in activity, 0 in lps_latest) -> challenge: same-date user-visible gap -> r3 CHANGED/medium (lps_daily now 3 protocols; latest mart still BV3-less).
- **C20**: r1 CONFIRMED/medium (`metrics: []`) -> challenge: run preflight/discover_metrics -> r2 CONFIRMED/medium (YAML dispositive) -> challenge: discover_metrics path -> r3 CONFIRMED/medium (registry YAML authoritative).
- **C21**: r1 CONFIRMED/medium (`415,368` V2 null-price) -> challenge: characterize unpriced tokens -> r2 CONFIRMED/medium (99.8% NULL-token BPT + minor RWA) -> challenge: stable vs regressed; check sDAI -> r3 CONFIRMED/medium (RWA in-whitelist but no price feed; coverage gap).
- **C22**: r1 CONFIRMED/medium (hop-grain sum vs per-tx doc) -> challenge: measure inflation -> r2 CONFIRMED/medium (`1.68x`, owns C06 sub-claim) -> challenge: per-pool broad vs concentrated -> r3 CONFIRMED/medium (top-5 ratios `1.26-1.44`).
- **C23**: r1 CONFIRMED/low (coalesce to 0) -> challenge: measure live footprint -> r2 CONFIRMED/low (`2` rows) -> r3 CONFIRMED/low (code re-confirmed).
- **C24**: r1 CONFIRMED/low (hop=1 -> Direct) -> challenge: split bucket by hop/label -> r2 CONFIRMED/low (`2,533,704`, 100% single-hop) -> r3 CONFIRMED/low (3-way multiIf fallthrough).
- **C25**: r1 CHANGED/low (lag 3d->1d, growth) -> challenge: confirm recovery stable not transient -> r2 CONFIRMED/low (full-month contiguous) -> challenge: re-measure all snapshot figures -> r3 CHANGED/low (all re-measured, grain dups 0, worst V3 neg `-$2.1M->-$13K`).

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P1 — KEEP | Add Balancer V2 to the fee pipeline and the `fct` protocol guard so ecosystem volume/fee/TVL totals include the chain's oldest AMM (`3.3%` USD, `86.9%` of balance rows, largest swap-count). | `models/execution/pools/intermediate/int_execution_pools_fees_daily.sql`, `models/execution/pools/marts/fct_execution_pools_daily.sql`, `models/execution/pools/marts/api_execution_pools_volume_daily.sql`, `api_execution_pools_tvl_daily.sql` |
| P1 — KEEP | Add the `$500` TVL floor to `lvr_apr_7d` (parity with `metrics_daily` `fee_apr` line 94) to kill the `5e19` `il_daily` outliers at source and stop positive-lvr / `net>fee` contract violations; add a `not_null`/`accepted_range(<=0)` test on `lvr_apr_7d`. | `models/execution/pools/marts/fct_execution_pools_il_daily.sql`, `models/execution/pools/marts/fct_execution_pools_daily.sql`, `models/execution/pools/marts/schema.yml` |
| P2 — KEEP | Add the GHO underlying `0xfc421ad3c883bf9e7c4f42de845c4e4405799e73` to `tokens_whitelist.csv` (recovers `598` dropped pool-days / `2` pools) and add a CI guard so new V3 wrappers cannot silently resolve to NULL price. | `seeds/tokens_whitelist.csv`, `models/execution/pools/staging/stg_pools__balancer_v3_token_map.sql` |
| P2 — KEEP | Extend `fct_execution_pools_lps_latest.pool_token_map` to include Balancer V3 pools so unique-LP counts are consistent with `lp_activity_daily` (`139` BV3 pools currently dropped). | `models/execution/pools/marts/fct_execution_pools_lps_latest.sql` |
| P2 — KEEP | Fix `schema.yml` for `api_execution_pools_lp_activity_daily` to document actual output `(date, token, label, type, value)` instead of phantom `(date, token, mints, burns)`; fix `fct_execution_trades_lifetime` doc (line 503) to say hop-grain, OR re-grain `lifetime_volume_usd` to per-tx to match `trade_count` (`1.68x` ratio mismatch). | `models/execution/pools/marts/schema.yml`, `models/execution/pools/marts/fct_execution_trades_lifetime.sql` |
| P3 — KEEP | Investigate residual small-magnitude Balancer V3 negative TVL (standing `63-69` pools/mo, `-$38K`/7d reaching the mart) — wrapper-scale catastrophe fixed but residual sign behavior persists. | `models/execution/pools/intermediate/int_execution_pools_balancer_v3_daily.sql`, `int_execution_pools_balances_daily.sql` |
| P3 — KEEP | Define MetricFlow metrics for core pool KPIs (fee_apr/TVL/volume/LVR) so the semantic layer stops returning `semantic_coverage_gap`. | `semantic/authoring/execution/pools/semantic_models.yml` |
| P3 — KEEP | Add `FINAL` (or equivalent) to `prev_balances` on the incremental branch of all four protocol daily models for parity with the backfill branch (latent, no observed drift). | `int_execution_pools_balancer_v2_daily.sql`, `int_execution_pools_uniswap_v3_daily.sql`, `int_execution_pools_swapr_v3_daily.sql`, `int_execution_pools_balancer_v3_daily.sql` |
| P4 — KEEP | Convention cleanups: add `engine='ReplacingMergeTree()'`+`order_by` to the 4 trades marts (C15); normalize `granularity:last_7d` -> `granularity:snapshot` (C16); migrate `nullIf(price,0)` -> `join_use_nulls` hook + `not_null` test (C12); add `api:/granularity:/tier:` tags to the `api_execution_trades_stats_*` views (C05 residual). | `fct_execution_trades_by_*`, `api_execution_pools_lps_count_7d.sql`, `fct_execution_pools_tvl_token_daily.sql`, `api_execution_trades_stats_*.sql` |
| - — DROP | C05 dev-tag CI bypass — RESOLVED (all 13 trades models now `production`). | (no action) |
| - — DROP | C11 full-rebuild >100-partition cap — RESOLVED (`partition_by='toStartOfMonth(date)'`, `42`<`100` months; monitor ~2029). | (no action) |
| - — DOWNGRADE | C08 token_index (no production consumer of staging model), C13 (inert filter), C18 (negligible live magnitude), C25 (data healthy) — keep on backlog but low priority. | (low priority) |
