# Model review (revisit 2026-06-21): execution/live

Re-verification of the `execution/live` sector (live/low-latency streaming marts, 45s refresh loop) against baseline [`docs/model_review/execution-live.md`](../execution-live.md) dated `2026-06-11`, re-run over 3 rounds on `2026-06-21`: 15 cases re-verified (14 baseline + 1 newly discovered), with **2 RESOLVED** (`C02` freshness recovered, `N01` decode-lag overturned as a methodology artifact), **3 CHANGED** (`C07` materialized, `C10` and `C12` downgraded), and **10 STILL CONFIRMED** — the worst still-open defects being the Balancer V3 empty-token-address cascade (`C01`, high) and the total absence of CoW/GPv2 coverage (`C11`, high).

## Status summary

| case_id | P0 | claim (short) | orig sev | status | new sev | confidence | incident | rounds |
|---|---|---|---|---|---|---|---|---|
| EXECUTIONLIVE-C01 | — | Balancer V3 `IS NOT NULL` filter lets CH-map empty-string `''` token addresses through, cascading to NULL USD | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONLIVE-C02 | — | ~18h freshness lag vs 45s design; table held only 30 min of data | high | RESOLVED | resolved | high | none | 3 |
| EXECUTIONLIVE-C03 | — | Four `api_execution_live_*` marts exempt (not failing) the CI tag gate (`production` not in tags) | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONLIVE-C04 | — | RMT ORDER BY has Nullable keys (`transaction_hash`, `log_index`) + `allow_nullable_key=1`; silent-dedup risk | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONLIVE-C05 | — | `incremental_strategy='append'` without `microbatch` tag; allowlisted in `no_delete_insert.allow` (bypass) | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONLIVE-C06 | — | `live_trades_overlap_minutes` default `15` in SQL vs `120` documented in schema.yml | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONLIVE-C07 | — | Unguarded LEFT JOIN to static `balancer_v2_pool_registry`; NULL pool_address risk | low | CHANGED | low | high | none | 3 |
| EXECUTIONLIVE-C08 | — | Feed view `ORDER BY block_timestamp DESC` with no server-side LIMIT | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONLIVE-C09 | — | Five stats columns carry no `not_null` tests or `data_type` annotations | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONLIVE-C10 | — | Cross-mart USD inconsistency: `max(amount_usd)` per-tx tile vs `sum(amount_usd)` per-hop chart | high | CHANGED | medium | high | none | 3 |
| EXECUTIONLIVE-C11 | — | No CoW/GPv2 coverage: live feed excludes all CoW-settled trades; systematic understatement | high | CONFIRMED | high | high | none | 3 |
| EXECUTIONLIVE-C12 | — | Symbol-keyed pricing + unknown `0x2086…910` + unwhitelisted GHO create NULL-USD pockets | medium | CHANGED | low | high | none | 3 |
| EXECUTIONLIVE-C13 | — | Balancer V3 wrapper map is a static hardcoded 5-token view; no dynamic resolution / no test | medium | CONFIRMED | medium | high | none | 3 |
| EXECUTIONLIVE-C14 | — | No semantic-layer entries for any `api_execution_live_*` mart; not MCP-discoverable | low | CONFIRMED | low | high | none | 3 |
| EXECUTIONLIVE-N01 | — | (NEW) `contracts_*_Vault_events_live` decode tables ~25-45x sparser than `int_live` rows | medium | RESOLVED | resolved | high | none | 2 |

## Delta vs baseline

### RESOLVED (2)
- **`EXECUTIONLIVE-C02` — freshness recovered.** Baseline saw `max(block_timestamp) = 2026-06-10T12:30:50Z` against server time `~2026-06-11T06:19` (~18h lag) and only ~30 min of data. Re-measured across all 3 rounds: lag is now `~2.4 min` (`lag_seconds=146`), the full 48h window is populated (`span_hours=48`, `9722` rows), and round 2 proved durability with `49` contiguous hourly buckets and zero empty hours. This is normal cron/incremental operation, **not** an incident recovery (the table was never month-collapsed). Incident attribution: none.
- **`EXECUTIONLIVE-N01` — decode-lag claim overturned as a methodology artifact.** The "25-45x sparser" framing (round 2: `53`/`54` live-decode Swap rows vs `1316`/`2444` persisted `int_live` rows over "48h") compared a `~2h` rolling decode buffer against `int_live`'s 48h ReplacingMergeTree retention — apples-to-oranges. Re-measured over an identical in-buffer window (`2026-06-21 15:30–17:20`): distinct `(tx_hash, log_index)` Swap keys = `67` in `contracts_BalancerV3_Vault_events_live` vs `67` backing `int_live` BalV3 (ratio `1.0x`); orchestrator round-3 re-check (`16:10–17:30`) gave BalV3 `66=66=66` and BalV2 `55=55=55`. No per-swap under-decode exists. Incident attribution: none.

### CHANGED (3)
- **`EXECUTIONLIVE-C07` — latent risk materialized, then right-sized to low.** Baseline observed `0` NULL pool_address. Today the unguarded LEFT JOIN to the table-materialized snapshot `stg_pools__balancer_v2_pool_registry` yields `11` NULL pool_address rows (of `2354` BalV2). Anti-joining the live 4h decode window (`78` Swaps) shows `0` missing poolIds — proving the join key (lowercased `poolId`) is format-correct and the `11` NULLs come from pools registered *after* the static snapshot (registry staleness), not a coercion bug. Net: latent-low → materialized but still **low** (`11` rows, still no test). Incident attribution: none.
- **`EXECUTIONLIVE-C10` — downgraded high → medium.** The baseline headline (`[100,150,80]` 3-hop trade → tile `150` vs chart `330`, ~2.2x) was dominated by a window mismatch (30-min feed tile vs 48h hourly chart), not the aggregation semantics. Isolated over ONE identical window (last full hour, same `125`-tx set, `19` multihop): `max(amount_usd)`-per-tx summed = `7424.66 USD` vs `sum(amount_usd)` per-hop = `7681.63 USD`, ratio `1.035` — i.e. the pure semantic divergence is only ~3.5%. Code unchanged (`api_execution_live_trades.sql` line 57 `max`, `api_execution_live_trades_hourly_48h.sql` line 24 `sum`); still undocumented at the user-facing tile. Incident attribution: none.
- **`EXECUTIONLIVE-C12` — downgraded medium → low.** Overall NULL-USD rose to `1163/9722` (`12%`), but `1155` of those are the `C01` Balancer V3 empty-address cascade. De-overlapped (NULL-USD **excluding** BalV3) is `8/8459` = `0.095%` (vs baseline `5.4%`), and the unknown token `0x2086f52651837600180de173b09470f54ef74910` dropped from `25.6%` (`170` rows) to `0.6%` (`58` rows). The symbol-keyed-pricing + unwhitelisted-GHO defect persists in code but its present-day blast radius in isolation is near-zero. Incident attribution: none — headline 12% belongs to `C01`.

### STILL CONFIRMED (10)
- **`EXECUTIONLIVE-C01` (high)** — `1173/1263` BalV3 rows (`92.9%`) carry empty (`''`, not NULL) token addresses while the other 3 protocols are `0`; `1155` cascade to NULL USD. Worse than baseline `36/45` (`80%`). Code unchanged: `stg_live__dex_trades_balancer_v3.sql` lines 20-21 filter only `IS NOT NULL`; CH Map miss returns `''`. A `!= ''` guard would convert the silent-NULL contamination into an explicit ~93% row drop, leaving only `~90` priced BalV3 rows.
- **`EXECUTIONLIVE-C03` (medium)** — `check_api_tags.py` line 53 (`if "production" not in tags: continue`) skips all four marts (tagged `['live','execution','pools','trades','api']`, bare `api`, no `production`/`api:`/`granularity:`/`tier`). Doubly exempt: line 55 builds `api[]` from `api:`-prefixed tags only. No entry in `check_api_tags.allow`.
- **`EXECUTIONLIVE-C04` (medium)** — `transaction_hash Nullable(String)` and `log_index Nullable(UInt32)` both in ORDER BY with `allow_nullable_key=1`; no NOT NULL pre-filter. Latent: `0` NULL-key rows today.
- **`EXECUTIONLIVE-C05` (medium)** — `incremental_strategy='append'`, no `microbatch` tag, allowlisted at `no_delete_insert.allow` line 32. Consequence materialized: `53` `(tx_hash, log_index)` dupe groups / `249` rows pre-FINAL; correctness rests entirely on ReplacingMergeTree FINAL at read time.
- **`EXECUTIONLIVE-C06` (medium)** — `int_live__dex_trades_raw.sql` line 13 default `15` vs schema.yml documented `120 = 2h`; git pins original `120` (commits `1fda797`, `665d9df`) → `15` drift. Second stale-doc defect: schema prose says `delete+inserts` on an `append`-strategy model.
- **`EXECUTIONLIVE-C08` (low)** — no server-side LIMIT (`ORDER BY block_timestamp DESC` only); documented caller-pagination. Burst bounded by the ~30-min window (`~18-125` tx/window observed), not DoS-shaped.
- **`EXECUTIONLIVE-C09` (low)** — five stats columns (`trade_count`, `volume_usd`, `unique_traders`, `aggregator_share_pct`, `multihop_share_pct`) have descriptions only, no `not_null`/`data_type`. Reachable NULL path: `nullIf(count(),0)` yields NULL share on an empty window.
- **`EXECUTIONLIVE-C11` (high)** — no GPv2/CoW model under `models/execution/live`; only 4 AMM protocols UNIONed. rpc_scan_logs of GPv2Settlement (`0x9008D19f58AAbD9eD0D60971565AA8510560ab41`) round 3: `228` Trade events / `223` settlement tx over `~4.4h` → `~1.2k+` CoW trades/day entirely absent, ~25-33% systematic understatement.
- **`EXECUTIONLIVE-C13` (medium)** — `stg_pools__balancer_v3_token_map.sql` is a static 5-row UNION ALL (waGnowstETH/waGnoWETH/waGnoUSDCe/waGnoGNO/waGnoGHO), no event-driven source, no unmapped-token test. Present-day staleness: `0/5` currently-traded BalV3 token addresses match a `wrapper_address`.
- **`EXECUTIONLIVE-C14` (low)** — grep of `semantic/` and `scripts/semantic/` for `api_execution_live`/`live__dex_trades` = `0` matches; non-discoverability tied to the bare-`api` tag (`C03`).

### NEW (1)
- `EXECUTIONLIVE-N01` was discovered during re-verification (round 2) and resolved by round 3 — see RESOLVED above. No surviving new defect.

### UNVERIFIABLE / UNRESOLVED (0)
- None.

## Evidence appendix

### C01 — Balancer V3 empty-address cascade (CONFIRMED, high)
```sql
SELECT protocol, count() AS total,
       countIf(token_bought_address='' OR token_sold_address='') AS empty_addr,
       countIf(token_bought_address IS NULL OR token_sold_address IS NULL) AS null_addr,
       countIf(amount_usd IS NULL) AS null_usd
FROM dbt.int_live__dex_trades_raw FINAL GROUP BY protocol;
```
Round 3 returned: Balancer V3 `1173/1263` empty (`92.9%`), `null_addr=0`, `null_usd=1155`; Balancer V2 / Swapr V3 / Uniswap V3 each `empty_addr=0`. Sampled source row from `contracts_BalancerV3_Vault_events_live` shows keys `['pool','tokenIn','tokenOut','amountIn','amountOut','swapFeePercentage','swapFeeAmount']` with `tokenIn='0xaf204776c7245bf4147c2612bf6e5972ee483701'`; CH Map miss returns `''` and the `IS NOT NULL` filter (`stg_live__dex_trades_balancer_v3.sql` lines 20-21) lets it through. (Round 1: `1156/1259`=91.8%; round 2: `1214/1316`=92.3%.)

### C02 — freshness (RESOLVED)
```sql
SELECT max(block_timestamp), min(block_timestamp), now(),
       dateDiff('second',max(block_timestamp),now()) AS lag_seconds,
       dateDiff('hour',min(block_timestamp),max(block_timestamp)) AS span_hours, count()
FROM dbt.int_live__dex_trades_raw FINAL;
```
Round 3: `max_ts=2026-06-21T17:23:50Z`, `lag_seconds=146`, `span_hours=48`, rows=`9722`, `min_ts=2026-06-19T17:26:45Z`. Round 2 durability check (`GROUP BY toStartOfHour`): `49` contiguous hourly buckets, zero empty hours (counts `60`–`930`).

### C03 — CI tag gate exemption (CONFIRMED, code_only)
`check_api_tags.py` line 53: `if "production" not in tags: continue`; line 55: `api=[t for t in tags if t.startswith('api:')]`. All four marts (`api_execution_live_trades{,_stats,_hourly_48h,_freshness}.sql`) tagged `['live','execution','pools','trades','api']`. No `api_execution_live_*` entry in `check_api_tags.allow`.

### C04 — Nullable RMT keys (CONFIRMED, code_only + sql)
`describe_table`: `transaction_hash Nullable(String)`, `log_index Nullable(UInt32)`; config `order_by='(block_timestamp, transaction_hash, log_index)'`, `settings={'allow_nullable_key':1}`; no NOT NULL pre-filter before INSERT.
```sql
SELECT countIf(transaction_hash IS NULL OR log_index IS NULL) FROM dbt.int_live__dex_trades_raw;
```
Returned `0` of `~10036` (latent).

### C05 — append bypass + pre-FINAL duplicates (CONFIRMED)
Config: `incremental_strategy='append'`, tags lack `microbatch`; `no_delete_insert.allow` line 32 lists `model.gnosis_dbt.int_live__dex_trades_raw`.
```sql
SELECT count() AS dupe_groups, sum(c) AS rows_in_dupe_groups
FROM (SELECT transaction_hash, log_index, count() c
      FROM dbt.int_live__dex_trades_raw GROUP BY transaction_hash, log_index HAVING c>1);
```
Round 2 returned `53` dupe groups / `249` rows pre-FINAL (round 2 alt measure: `104` groups / `305` rows).

### C06 — overlap-minutes / delete-insert doc drift (CONFIRMED, code_only)
`int_live__dex_trades_raw.sql` line 13: `var('live_trades_overlap_minutes', 15)`; `intermediate/schema.yml` documents `default 120 = 2h` and `delete+inserts`. `git log -S` confirms original default `120` (commits `1fda797` 2026-04-15, `665d9df` 2026-04-17). No `dbt_project.yml` override.

### C07 — NULL pool_address materialized (CHANGED, low)
```sql
SELECT countIf(pool_address IS NULL) AS null_pool, count()
FROM dbt.int_live__dex_trades_raw FINAL WHERE protocol='Balancer V2';
-- anti-join live decode:
SELECT count(), countIf(r.pool_id IS NULL)
FROM contracts_BalancerV2_Vault_events_live e
LEFT JOIN stg_pools__balancer_v2_pool_registry r ON r.pool_id=lower(e.decoded_params['poolId'])
WHERE e.event_name='Swap';
```
`11` NULL pool_address / `2354` BalV2 rows (baseline `0`). Live 4h window: `78` Swaps, `0` poolIds missing from registry → staleness, not key-casing.

### C08 — no server-side LIMIT (CONFIRMED, code_only)
`api_execution_live_trades.sql` lines 124-125: `ORDER BY s.block_timestamp DESC` with no LIMIT; schema.yml: `No LIMIT is applied — dashboards should add their own.` Window ~30 min (lines 20-21: hwm-30min to hwm-60s); per-poll bounded to `~18-125` tx.

### C09 — untested/untyped stats columns (CONFIRMED, code_only)
`marts/schema.yml` lines 57-66: `trade_count`, `volume_usd`, `unique_traders`, `aggregator_share_pct`, `multihop_share_pct` each have description only, no `tests:` / `data_type:`. Stats SQL uses `round(100.0*countIf(...)/nullIf(count(),0),1)` → NULL share on empty window.

### C10 — cross-mart aggregation semantics (CHANGED, medium)
```sql
WITH last_hour AS (
  SELECT toStartOfHour(max(block_timestamp))-INTERVAL 1 HOUR AS h0,
         toStartOfHour(max(block_timestamp)) AS h1 FROM int_live__dex_trades_raw),
per_tx AS (
  SELECT transaction_hash, max(amount_usd) AS tx_max_usd, sum(amount_usd) AS tx_sum_usd, count() AS hops
  FROM int_live__dex_trades_raw FINAL
  WHERE block_timestamp>=h0 AND block_timestamp<h1 AND amount_usd IS NOT NULL
  GROUP BY transaction_hash)
SELECT count(), countIf(hops>1), sum(tx_max_usd), sum(tx_sum_usd), sum(tx_sum_usd)/sum(tx_max_usd) FROM per_tx;
```
Same-window, same `125`-tx set, `19` multihop: tile `7424.66 USD` vs hourly `7681.63 USD`, ratio `1.035`. Code: `api_execution_live_trades.sql` line 57 `max(amount_usd)`, `api_execution_live_trades_hourly_48h.sql` line 24 `sum(amount_usd)`.

### C11 — no CoW/GPv2 coverage (CONFIRMED, rpc + grep)
`rpc_scan_logs` on GPv2Settlement `0x9008D19f58AAbD9eD0D60971565AA8510560ab41`, event `Trade(...)`: round 3 returned `228` Trade events / `223` distinct settlement tx over blocks `46,810,887→46,814,043` (~4.4h) → `~1.2k+` trades/day (round 2: `1,746` events / `1,674` tx over ~1 day). `grep` of `models/execution/live` for gpv2/cow/settlement = `0`; only 4 AMM staging protocols.

### C12 — symbol-keyed pricing in isolation (CHANGED, low)
```sql
SELECT protocol, countIf(amount_usd IS NULL) AS null_usd, count()
FROM int_live__dex_trades_raw FINAL GROUP BY protocol;
```
NULL-USD excluding BalV3 = `8` (all BalV2) / `8459` = `0.095%`; overall `1163/9722` = `12%` (`1155` = BalV3/C01). Token `0x2086…910` = `58` rows (`~0.6%`). GHO `0xfc421ad3c883bf9e7c4f42de845c4e4405799e73` = `0` feed rows; absent from `tokens_whitelist.csv` (grep `0` hits) but present as waGnoGHO underlying in `stg_pools__balancer_v3_token_map` line 16. Price model keyed `(date, symbol)`.

### C13 — static Balancer V3 wrapper map (CONFIRMED, code_only + sql)
`stg_pools__balancer_v3_token_map.sql` = static 5-row UNION ALL (waGnowstETH, waGnoWETH, waGnoUSDCe, waGnoGNO, waGnoGHO), no event source, no unmapped-token test. Anti-join of current-window distinct non-empty BalV3 token addresses vs the 5 `wrapper_address` rows: `5` distinct tokens, `0` mapped, `5` unmapped passthrough.

### C14 — no semantic-layer entries (CONFIRMED, code_only)
```
grep -rniE 'api_execution_live|live__dex_trades' semantic/ scripts/semantic/  # = 0 matches
```
All four marts carry bare `api` tag (no `api:`/`granularity:`/`tier`).

### N01 — decode buffer vs retention (RESOLVED, sql)
```sql
WITH w AS (SELECT toDateTime('2026-06-21 15:30:00') AS t0, toDateTime('2026-06-21 17:20:00') AS t1)
SELECT uniqExact((transaction_hash,log_index)) FROM contracts_BalancerV3_Vault_events_live, w
WHERE event_name='Swap' AND block_timestamp BETWEEN t0 AND t1;  -- same for int_live BalV3
```
In-buffer window `15:30–17:20`: decode distinct swap keys `67` = `int_live` distinct swap keys `67` = `int_live` total rows `67` (ratio `1.0x`). `_live` buffers span only `~1.7-2h` (BalV3 `15:56–17:38`, BalV2 `15:53–17:50`) vs `int_live` 48h retention — confirming the original 48h-filter-on-a-2h-buffer comparison was structurally invalid.

## Review log (>=3 rounds per case)

- **C01**: R1 CONFIRMED (`1156/1259`=91.8% empty) → challenge: isolate WHY empty not NULL, quote raw `decoded_params` → R2 CONFIRMED (`1214/1316`=92.3%, sampled source keys, CH-map-miss traced) → challenge: quantify post-`!= ''`-guard survival → R3 CONFIRMED (`1173/1263`=92.9%; guard would leave `~90` priced rows). Severity high throughout.
- **C02**: R1 RESOLVED (lag 0h, 48h span, `9847` rows) → challenge: prove durability not single snapshot → R2 RESOLVED (`132s` lag, `49` contiguous buckets, zero empty hours) → R3 RESOLVED (`146s` lag, `9722` rows, not incident recovery). Resolved throughout.
- **C03**: R1 CONFIRMED (line-53 gate) → challenge: sharpen bare-`api` vs `api:` + check allowlist + factory enumeration → R2 CONFIRMED (line 55 `api:` prefix fail, no allowlist entry) → R3 CONFIRMED (doubly exempt). Medium throughout.
- **C04**: R1 CONFIRMED (Nullable keys, `allow_nullable_key=1`) → challenge: measure actual NULL-key rows → R2 CONFIRMED (`0` NULL-key rows, latent) → R3 CONFIRMED (latent-medium). Medium throughout.
- **C05**: R1 CONFIRMED (append, untagged, allowlisted line 32) → challenge: show overlap-append dupes pre-FINAL → R2 CONFIRMED (`53` dupe groups / `249` rows) → R3 CONFIRMED (FINAL-as-only-guard). Medium throughout.
- **C06**: R1 CONFIRMED (default `15` vs doc `120`) → challenge: note `delete+inserts` prose on append model + quantify stall tolerance → R2 CONFIRMED (second stale-doc defect; 15min = 3 cron cycles) → R3 CONFIRMED (git pins `120→15`). Medium throughout.
- **C07**: R1 CONFIRMED (unguarded LEFT JOIN, `0` nulls, low) → challenge: confirm registry is static snapshot + anti-join recent pools → R2 CHANGED (`11` NULL pool_address materialized, medium) → challenge: anti-join the 11 to pin staleness vs casing → R3 CHANGED (live window `0` missing poolIds → staleness; right-sized to low).
- **C08**: R1 CONFIRMED (no LIMIT, documented) → challenge: quantify realistic burst size → R2 CONFIRMED (`19-67` rows/poll, peak `238`/5min, bounded) → R3 CONFIRMED (bounded by 30-min window). Low throughout.
- **C09**: R1 CONFIRMED (5 columns no tests/data_type) → challenge: show NULL path is reachable → R2 CONFIRMED (`nullIf(count(),0)` on empty window) → R3 CONFIRMED. Low throughout.
- **C10**: R1 CONFIRMED (max vs sum, 5/53 multihop, high) → challenge: note schema.yml DOES document semantics; check same-window tile vs hourly → R2 CONFIRMED (tile `3491` vs hourly `922,126`, but window mismatch acknowledged) → challenge: isolate semantic component on ONE identical window → R3 CHANGED (same-hour ratio `1.035`; high→medium).
- **C11**: R1 CONFIRMED (no CoW model, 4 AMM only, high) → challenge: quantify understatement magnitude → R2 CONFIRMED (`1,674` settled tx/day ≈ 25-33%) → R3 CONFIRMED (`~1.2k+` trades/day on-chain). High throughout.
- **C12**: R1 CONFIRMED (NULL-USD `11.62%`, medium) → challenge: de-overlap from C01, compute isolated rate → R2 CHANGED (`0.093%` excl BalV3, low) → R3 CHANGED (`0.095%` excl BalV3, token `0x2086` `0.6%`). High→medium→low.
- **C13**: R1 CONFIRMED (static 5-row map, no test) → challenge: anti-join current tokens vs wrapper_address → R2 CONFIRMED (`0/5` matched, all passthrough) → R3 CONFIRMED. Medium throughout.
- **C14**: R1 CONFIRMED (zero semantic entries) → challenge: query MCP registry / confirm omission vs intentional → R2 CONFIRMED (grep `0` matches, tied to bare-`api`) → R3 CONFIRMED. Low throughout.
- **N01**: R2 NEW (medium; `53/54` live-decode vs `1316/2444` int_live "48h") → challenge: re-measure on a single in-buffer window with distinct-swap-key normalization → R3 RESOLVED (in-buffer `67=67=67`, ratio `1.0x`; buffer spans ~2h vs 48h retention — methodology artifact). 2 evidence rounds (first surfaced R2); orchestrator independently corroborated R3 (`66=66=66`, `55=55=55`).

## Refreshed recommendations

| priority | recommendation | affected models |
|---|---|---|
| P1 (KEEP/ESCALATE) | Add a `!= ''` guard on `tokenIn`/`tokenOut` in the WHERE so CH-map-miss empties are dropped (or routed to a quarantine), not carried as NULL-USD; `92.9%` of BalV3 rows are currently unpriced contamination. Pair with `C13` so unmapped wrappers are flagged before drop. | `models/execution/live/staging/stg_live__dex_trades_balancer_v3.sql`, `models/execution/live/intermediate/int_live__dex_trades_raw.sql` |
| P1 (KEEP/ESCALATE) | Build a CoW/GPv2Settlement live staging model so the feed/stats/hourly marts stop understating DEX activity by ~25-33%. | `models/execution/live/` (new GPv2 staging), `api_execution_live_trades_stats.sql`, `api_execution_live_trades_hourly_48h.sql` |
| P2 (KEEP) | Replace the static 5-row Balancer V3 wrapper map with an event-driven source (mirroring BalV2's registry) and add a test that alerts on unmapped wrappers (`0/5` current tokens matched today). | `models/execution/live/staging/stg_pools__balancer_v3_token_map.sql`, `stg_live__dex_trades_balancer_v3.sql` |
| P2 (KEEP) | Bring the four marts under the API tag convention: add `production` + `api:`/`granularity:`/`tier` tags so CI validates them and the factory can route them; also register semantic-layer entries (`C14`). | `api_execution_live_trades.sql`, `api_execution_live_trades_stats.sql`, `api_execution_live_trades_hourly_48h.sql`, `api_execution_live_trades_freshness.sql`, `scripts/ci/check_api_tags.py` |
| P2 (KEEP) | Either tag the model `microbatch` and remove it from `no_delete_insert.allow`, or add a NOT NULL pre-filter on `transaction_hash`/`log_index` before INSERT and drop the Nullable keys; today the append+Nullable-key combo (`C04`+`C05`) leaves dedup safety entirely to FINAL. | `models/execution/live/intermediate/int_live__dex_trades_raw.sql`, `scripts/ci/no_delete_insert.allow` |
| P3 (KEEP) | Reconcile the two stale-doc defects: set the SQL default and schema.yml doc to one agreed overlap value, and fix the `delete+inserts` prose on the `append` model. | `models/execution/live/intermediate/int_live__dex_trades_raw.sql`, `models/execution/live/intermediate/schema.yml` |
| P3 (KEEP, lowered) | Document the `max`-per-tx (tile/stats) vs `sum`-per-hop (hourly) semantics at the user-facing tile, or unify them; same-window divergence is now only ~3.5% but undisclosed where the user sees it. | `api_execution_live_trades.sql`, `api_execution_live_trades_hourly_48h.sql`, `marts/schema.yml` |
| P3 (KEEP) | Add a NULL guard/test on the Balancer V2 pool registry LEFT JOIN; `11` NULL pool_address rows are now materialized from registry staleness. | `models/execution/live/staging/stg_live__dex_trades_balancer_v2.sql`, `stg_pools__balancer_v2_pool_registry` |
| P3 (KEEP) | Add `not_null` tests + `data_type` annotations to the five stats columns. | `models/execution/live/marts/api_execution_live_trades_stats.sql`, `marts/schema.yml` |
| P4 (KEEP, lowered) | Whitelist GHO `0xfc421ad3c883bf9e7c4f42de845c4e4405799e73` and the unknown `0x2086…910`, and consider address-keyed pricing; isolated blast radius is now only `0.095%`. | `seeds/tokens_whitelist.csv`, `int_execution_token_prices_daily`, `int_live__dex_trades_raw.sql` |
| P4 (KEEP) | Document the no-LIMIT caller-pagination contract at the API boundary (already in schema.yml; surface to consumers). | `models/execution/live/marts/api_execution_live_trades.sql` |
| DROP | `C02` freshness — RESOLVED (`146s` lag, full 48h span); no action needed. | `int_live__dex_trades_raw.sql` |
| DROP | `N01` decode-lag — RESOLVED as a methodology artifact (in-buffer ratio `1.0x`); not a defect. | `contracts_BalancerV3_Vault_events_live`, `contracts_BalancerV2_Vault_events_live` |
