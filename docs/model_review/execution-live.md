# Model review: execution/live

**Convergence:** converged in 2 rounds — round-1 HIGH tag-compliance finding corrected (check_api_tags.py skips non-production models entirely; no CI failure occurs), and cross-mart USD inconsistency elevated to HIGH after direct code confirmation; all other findings agreed between rounds.

---

## Scope and inventory

The unit lives entirely on the `feat/live-trades` branch under `models/execution/live`. It is a purpose-built near-real-time DEX trade feed with a 45-second refresh cadence and a 48-hour rolling TTL window. The Trades dashboard sector backed by this unit is currently `enabled: false` in production.

| Layer | Count | Models |
|---|---|---|
| Staging (views) | 4 | `stg_live__dex_trades_uniswap_v3`, `stg_live__dex_trades_swapr_v3`, `stg_live__dex_trades_balancer_v2`, `stg_live__dex_trades_balancer_v3` |
| Intermediate (incremental) | 1 | `int_live__dex_trades_raw` |
| Marts (views) | 4 | `api_execution_live_trades`, `api_execution_live_trades_stats`, `api_execution_live_trades_hourly_48h`, `api_execution_live_trades_freshness` |

9 SQL files total. 3 schema.yml files plus `execution_live_sources.yml`. No semantic-layer registrations for any mart.

---

## Business context

The unit answers four dashboard questions for the "Live Feed" tab of the Gnosis Chain Trades dashboard:

1. What DEX swaps occurred on Gnosis Chain in the last 30 minutes? (`api_execution_live_trades` — paginated feed table with trader identity and per-hop breakdown)
2. What are the headline KPIs for that window? (`api_execution_live_trades_stats` — trade count, USD volume, unique traders, aggregator share, multi-hop share)
3. What was hourly DEX volume by protocol over the last 48 hours? (`api_execution_live_trades_hourly_48h` — stacked-bar chart feed)
4. How fresh is the data? (`api_execution_live_trades_freshness` — ingestion lag in seconds for a "Data as of X" banner)

**Canonical definitions:**

- **DEX trade:** one EVM transaction containing at least one on-chain Swap event from a covered AMM protocol. Multi-hop trades are collapsed to a single feed row using `argMin`/`argMax` on `log_index` for first-sold/last-bought token; per-hop detail is preserved in a `hops_detail` nested array.
- **amount_usd (conservative notional):** `LEAST(amount_bought_usd, amount_sold_usd)` when both sides are priced; the single priced side otherwise; NULL when neither side is priced. Resists long-tail inflation without dropping rows.
- **trade_usd (per transaction):** `MAX` of per-hop conservative notionals across all hops in the transaction (feed tile). Note: the hourly chart uses `SUM` per-hop row without tx-level deduplication — these are different aggregation semantics on the same underlying data.
- **trader:** `tx.from_address` — the signing EOA, not a router contract.
- **dust filter:** `live_trades_min_usd` var (default 1 USD); rows with NULL `trade_usd` are kept (unknown price is not the same as dust).
- **30-minute feed window:** anchored on `max(block_timestamp)` in `int_live__dex_trades_raw`, minus a 60-second reorg buffer at the top end — not wall-clock time.
- **48-hour TTL:** enforced on both `execution_live.logs` and `int_live__dex_trades_raw` via ClickHouse TTL expression.

**Contract context:** Uniswap V3 Factory `0xe32F7...B1` (29 whitelisted pools), Swapr V3/Algebra Factory `0xa0864...a766` (12 pools), Balancer V2 Vault `0xBA122...C8` (single-contract, pool address resolved from `stg_pools__balancer_v2_pool_registry`), Balancer V3 Vault `0xba133...a9` (single-contract, 5-token hardcoded wrapper map). Source: `execution_live` ClickHouse database populated by a cryo-live indexer with ~30s block latency and a 6-confirmation buffer. CoW Protocol has no live events model in this unit.

---

## Implementation assessment

### HIGH — Balancer V3 staging: IS NOT NULL does not guard empty-string decoded_params

`models/execution/live/staging/stg_live__dex_trades_balancer_v3.sql` filters `decoded_params['tokenIn'] IS NOT NULL`, but ClickHouse map lookups on a missing key return `''` (empty string), not NULL. Both COALESCE arms resolve to `''` on a miss, producing empty `token_bought_address` / `token_sold_address`. Confirmed in warehouse queries: 36/45 Balancer V3 rows (80%) carry empty addresses, cascade to NULL USD and NULL symbol, and are real trades invisible to pricing and symbol resolution. Fix: add `AND decoded_params['tokenIn'] != ''` (and `tokenOut`) to the staging WHERE clause.

**Affected:** `models/execution/live/staging/stg_live__dex_trades_balancer_v3.sql`, `models/execution/live/intermediate/int_live__dex_trades_raw.sql`

### HIGH — Data freshness: 18-hour lag vs 45-second design intent

`max(block_timestamp)` in `int_live__dex_trades_raw FINAL` = `2026-06-10T12:30:50Z` against server time ~`2026-06-11T06:19`. Lag is ~18 hours; the table held only 30 minutes of data rather than the intended 48-hour window. For a 45-second-cadence feed this indicates the CronJob is stopped/failing or the cryo-live source has stalled. Operational investigation required before the feed is trusted. (The Trades dashboard sector is `enabled: false` on `feat/live-trades`, so this may be a pre-rollout state — confirm before treating as a live incident.)

**Affected:** `models/execution/live/intermediate/int_live__dex_trades_raw.sql`

### MEDIUM — All api_* live marts exempt from (not failing) CI tag convention — undocumented exclusion

`check_api_tags.py` line 53 reads `if "production" not in tags: continue` — the entire validation block is skipped for any model not bearing the `production` tag. The four `api_execution_live_*` marts carry only `['live', 'execution', 'pools', 'trades', 'api']`, so they are completely invisible to the guard: no CI failure fires, but no enforced `granularity:`, `tier`, or typed-column contract exists either. The practical consequence is that these endpoints cannot be auto-routed by `cerebro-api`'s `factory.py` (which requires an `api:` tag to build a REST route) and are absent from the MCP semantic registry. If `live` is an intentional separate tier from `production`, the exemption should be documented in `check_api_tags.py` or the model config to prevent future reviewers from re-flagging it.

**Affected:** all four mart files under `models/execution/live/marts/`

### MEDIUM — ReplacingMergeTree ORDER BY keys are Nullable — silent NULL-merge risk

`int_live__dex_trades_raw` ORDER BY is `(block_timestamp, transaction_hash, log_index)`; `describe_table` shows `transaction_hash` as `Nullable(String)` and `log_index` as `Nullable(UInt32)`. The `allow_nullable_key=1` setting suppresses the engine error but ClickHouse treats all NULL-key rows as merge-collapsible — any malformed decode yielding NULL `transaction_hash` risks silent deduplication of newer rows against each other during background merges. Pre-filter `NOT NULL` on both key columns before INSERT.

**Affected:** `models/execution/live/intermediate/int_live__dex_trades_raw.sql`

### MEDIUM — append strategy without 'microbatch' tag (CI bypassed via allowlist)

`no_delete_insert.py` requires `incremental_strategy='append'` models to carry a `microbatch` tag as the formal no-overlap-watermark contract. `int_live__dex_trades_raw` uses append, lacks the tag, and is allowlisted in `no_delete_insert.allow` — the CI check is bypassed rather than the contract satisfied. The overlap logic exists in the SQL, but the tag is the formal contract. Tag the model `microbatch` (or migrate to a declared convention) and remove the allowlist entry.

**Affected:** `models/execution/live/intermediate/int_live__dex_trades_raw.sql`

### MEDIUM — overlap_minutes default 15 in code contradicts schema.yml documentation (120 / 2h)

`int_live__dex_trades_raw.sql` line 13: `var('live_trades_overlap_minutes', 15)`. The `intermediate/schema.yml` documents `default 120 = 2h`, and worktree history shows 120 as the original value. On a ~5-minute CronJob cadence, 15 minutes = 3 cycles — tight if cryo-live bulk-attaches blocks late after a stall. One value is wrong; reconcile code and docs and confirm the intended production value (or confirm a `dbt_project.yml` override sets 120 externally).

**Affected:** `models/execution/live/intermediate/int_live__dex_trades_raw.sql`, `models/execution/live/intermediate/schema.yml`

### LOW — Balancer V2 LEFT JOIN on pool registry — NULL pool_address untested

`stg_live__dex_trades_balancer_v2.sql` LEFT JOINs `stg_pools__balancer_v2_pool_registry` with no NULL guard on the result. 0 null pool addresses observed today, but any pool created after the historical registry snapshot would silently produce `NULL pool_address` with no test or warning. Add an explicit filter or a `not_null`/relationship test to make scope explicit.

**Affected:** `models/execution/live/staging/stg_live__dex_trades_balancer_v2.sql`

### LOW — api_execution_live_trades has no server-side LIMIT

The feed view orders by `block_timestamp DESC` with no LIMIT; pagination is caller-enforced. Row counts are small in-window today, but a long gap or high-activity burst could return thousands of rows per poll. Acceptable if documented, but the caller-pagination contract should be more prominent in `schema.yml`.

**Affected:** `models/execution/live/marts/api_execution_live_trades.sql`

### LOW — api_execution_live_trades_stats lacks column tests and data_type declarations

None of `trade_count`, `volume_usd`, `unique_traders`, `aggregator_share_pct`, `multihop_share_pct` carry `not_null` tests or `data_type` annotations. For a single-row header tile the risk is low, but upstream NULL propagation (e.g. all-NULL trader column) would silently return NULL with no test catching it.

**Affected:** `models/execution/live/marts/api_execution_live_trades_stats.sql`, `models/execution/live/marts/schema.yml`

---

## Business-logic assessment

### HIGH — Cross-mart USD inconsistency: feed tile uses MAX per-tx, hourly chart uses SUM per-hop

`api_execution_live_trades.sql` tx_summary CTE line 57 computes `max(amount_usd) AS trade_usd` — the largest single-hop conservative notional across the transaction. `api_execution_live_trades_hourly_48h.sql` line 22 computes `sum(amount_usd)` directly over hop-level rows with no tx-level deduplication. For a 3-hop trade with per-hop USD values of [100, 150, 80]: the feed tile reports 150; the hourly chart accumulates 330 as that trade's contribution to the hour. The hourly chart's `schema.yml` (lines 9-11) discloses this ("multi-hop trades contribute to every protocol they touch"), but the feed tile carries no equivalent disclosure. A user cross-referencing the tile USD against the chart cannot reconcile the figures without internal knowledge of both aggregation strategies. Decision required: anchor `trade_usd` to the entry-hop notional (`argMin(amount_usd, log_index)`) for economic value, and label the hourly chart explicitly as per-hop activity volume.

**Affected:** `models/execution/live/marts/api_execution_live_trades.sql`, `models/execution/live/marts/api_execution_live_trades_hourly_48h.sql`

### HIGH — No CoW Protocol coverage — live DEX volume systematically understates the market

CoW settles via a solver network and does not emit standard AMM Swap events, so the live feed excludes all CoW-settled trades (acknowledged in `cerebro-docs/protocols/index.md`: "no model yet"). CoW is a dominant Gnosis Chain venue. The headline `volume_usd`, `trade_count`, and per-protocol share in `api_execution_live_trades_stats` and `api_execution_live_trades_hourly_48h` are not a complete picture of DEX activity. Either add a `GPv2Settlement`-based live model or prominently label all volume/share outputs as "AMM Swap events only, excludes CoW" wherever figures are shown to external consumers.

**Affected:** `models/execution/live/marts/api_execution_live_trades_stats.sql`, `models/execution/live/marts/api_execution_live_trades_hourly_48h.sql`

### MEDIUM — Symbol-based pricing plus unknown and unwhitelisted tokens create systematic NULL-USD pockets

`int_execution_token_prices_daily` is keyed on `(symbol, date)`, not address. Unknown token `0x2086f52651837600180de173b09470f54ef74910` (~25.6% of current-window swaps) has NULL symbol and NULL price. GHO (waGnoGHO underlying `0xfc421ad3c883bf9e7c4f42de845c4e4405799e73`) is absent from `tokens_whitelist.csv`, so Balancer V3 GHO trades resolve to empty symbol and NULL USD after wrapper resolution. Overall NULL-USD rate: ~5.4% (36/665 rows), concentrated in these gaps. Triage the unknown token (legitimate new pool vs honeypot/test token) and add GHO to the whitelist or remove the wrapper-map entry; consider address-based pricing to remove the symbol-collision risk entirely.

**Affected:** `models/execution/live/intermediate/int_live__dex_trades_raw.sql`, `models/execution/live/staging/stg_live__dex_trades_balancer_v3.sql`

### MEDIUM — Balancer V3 wrapper map is static/hardcoded (5 tokens) — silent under-resolution of new wrappers

`stg_pools__balancer_v3_token_map` is a hardcoded view of 5 aave-wrapped tokens to underlying. Any new `waGno*` wrapper added on-chain after the last code change passes through with the raw wrapper address, distorting per-token volume and bypassing pricing. Unlike Balancer V2 (registry-driven via `PoolRegistered` events), this has no dynamic resolution path. Plan an event-driven approach (TokensRegistered / PoolRegistered from the Balancer V3 Vault), or at minimum add a test/alert when an unmapped Balancer V3 token appears in production.

**Affected:** `models/execution/live/staging/stg_live__dex_trades_balancer_v3.sql`

### LOW — No semantic-layer entries for any api_execution_live_* mart

Four API-tagged marts exist but none are registered as semantic metrics and none follow the `api:/granularity:/tier` convention, so they are not discoverable via the MCP semantic registry or auto-routable by `cerebro-api factory.py`. If REST/MCP exposure is intended for these endpoints, define the convention tags, add typed columns, and register semantic metrics. If intentionally dashboard-only, document that scope to prevent future convention-compliance questions.

**Affected:** all four mart files under `models/execution/live/marts/`

---

## Data findings

Eight warehouse queries were run during inspection (grain check, freshness, protocol breakdown with empty-address count, NULL-USD breakdown by symbol, FINAL vs pre-FINAL row comparison, Balancer V2 NULL pool check, price-table case check, price coverage for WxDAI/wstETH/sDAI).

| Metric | Value |
|---|---|
| Total rows in window | 665 (FINAL) |
| Duplicate (tx_hash, log_index) pairs | 0 — grain clean |
| Data lag at inspection | ~18 hours (`max(block_timestamp)` = 2026-06-10T12:30:50Z) |
| Balancer V3 rows with empty token addresses | 36 / 45 (80%) |
| Overall NULL-USD rate | 36 / 665 (5.4%) |
| Price coverage for 9 known symbols | 100% |
| Unknown token 0x2086...910 rows | 170 (~25.6% of window) |
| Pending merges (pre-FINAL vs FINAL delta) | 0 — no pending merges |

Price ASOF join is functioning correctly for all 9 known symbols (WETH, USDC, WxDAI, USDC.e, EURe, GNO, sDAI, wstETH, GHO excluded by whitelist absence). All NULL-USD originates from either the Balancer V3 empty-address bug or the unknown/unwhitelisted token gap, not from a broken price join.

---

## Pros / Cons

**Pros**

- Well-structured layered pipeline: 4 narrow staging views, 1 focused incremental intermediate, 4 purpose-specific mart views, each mapped to a concrete dashboard tile.
- Conservative USD notional (`LEAST` of priced sides, NULL when unpriced) correctly resists long-tail token price inflation without dropping rows.
- ReplacingMergeTree deduplication verified clean in-window: 0 duplicate `(transaction_hash, log_index)` pairs; FINAL and pre-FINAL counts identical.
- 48-hour TTL on both source and intermediate cleanly bounds rolling-window storage; full-refresh watermark self-bounds the bootstrap backfill.
- Signed-amount decoding for Uniswap/Swapr V3 and ASOF daily pricing join function correctly; 100% price coverage for all 9 whitelisted symbols.
- Per-hop detail preserved in `hops_detail` nested array while collapsing multi-hop routes to a single feed row — good for UI drill-down without losing the hop grain.
- Freshness mart provides an explicit ingestion-lag signal usable for indexer health monitoring and alerting.
- Scope and caveats are unusually well documented in `schema.yml` at every layer (TTL, dust filter, symbol-based pricing, reorg buffer, multi-hop aggregation semantics in hourly chart).

**Cons**

- Cross-mart USD inconsistency on the primary metric: the same multi-hop trade shows different USD on the feed tile (MAX per-tx) vs the hourly chart (SUM per-hop) — not reconcilable by a user without internal knowledge.
- Balancer V3 empty-string bug: 80% of in-scope rows carry empty token addresses and NULL USD due to an insufficient IS NOT NULL guard.
- 18-hour freshness lag against a 45-second design intent — the feed cannot be trusted in production until the cron/indexer root cause is identified.
- Four api_* marts are outside the convention-enforced catalogue: no `production`/`api:`/`granularity:`/`tier` tags, no auto-routing, no MCP registry registration.
- No CoW Protocol coverage — a material scope gap given CoW's market share on Gnosis Chain; volume and share figures are systematically incomplete.
- Symbol-based (not address-based) pricing, one unknown token (~25.6% of window), and unwhitelisted GHO leave systematic NULL-USD pockets.
- ReplacingMergeTree ORDER BY keys are Nullable (`allow_nullable_key=1`) — any NULL `transaction_hash` from a malformed decode risks silent cross-row deduplication.
- `overlap_minutes` is 15 in code but documented as 120 (2h) in `schema.yml`; the 15-minute value is tight for cryo-live bulk-attach stall recovery.

---

## Recommendations

| Priority | Recommendation | Affected models |
|---|---|---|
| 1 | Fix Balancer V3 staging: add `AND decoded_params['tokenIn'] != ''` (and `tokenOut`) to the WHERE clause so 36/45 empty-address rows are either correctly resolved or explicitly dropped. | `stg_live__dex_trades_balancer_v3.sql` |
| 2 | Investigate 18-hour freshness lag: check CronJob logs and cryo-live indexer health; confirm whether this is a pre-rollout artifact (`enabled: false`) or an active incident requiring remediation. | `int_live__dex_trades_raw.sql` |
| 3 | Resolve cross-mart USD semantics: anchor feed-tile `trade_usd` to the entry-hop notional (`argMin(amount_usd, log_index)`) for economic value; label the hourly chart explicitly as per-hop activity volume in schema and UI. | `api_execution_live_trades.sql`, `api_execution_live_trades_hourly_48h.sql` |
| 4 | Pre-filter `NOT NULL` on `transaction_hash` and `log_index` before INSERT to remove the Nullable ORDER BY silent-dedup risk. | `int_live__dex_trades_raw.sql` |
| 5 | Add `microbatch` tag to `int_live__dex_trades_raw` (or migrate to a declared no-overlap convention) and remove its `no_delete_insert.allow` entry so the append contract is enforced, not bypassed. | `int_live__dex_trades_raw.sql` |
| 6 | Reconcile `overlap_minutes`: set one intended production value (15 vs 120) in both code and `schema.yml`, sized for the real cron cadence and worst-case cryo-live bulk-attach delay. | `int_live__dex_trades_raw.sql`, `intermediate/schema.yml` |
| 7 | Triage unknown token `0x2086f52651837600180de173b09470f54ef74910` and GHO (`0xfc421...`): add legitimate tokens to `tokens_whitelist` (or move to address-based pricing), filter test/honeypot tokens. | `int_live__dex_trades_raw.sql`, token seeds |
| 8 | Document the `live` vs `production` tier exemption in `check_api_tags.py` or model config; if REST/MCP exposure is planned, apply `api:`/`granularity:`/`tier` tags, add typed columns, and register semantic metrics. | All four mart files |
| 9 | Address the CoW Protocol gap: add a `GPv2Settlement`-based live model or prominently label all volume/share outputs as "AMM Swap events only, excludes CoW" before external consumers see them. | `api_execution_live_trades_stats.sql`, `api_execution_live_trades_hourly_48h.sql` |
| 10 | Make the Balancer V3 wrapper map dynamic (TokensRegistered/PoolRegistered events) or add a test that fires when an unmapped `waGno*` token appears; add `not_null`/relationship test to Balancer V2 pool-registry LEFT JOIN. | `stg_live__dex_trades_balancer_v3.sql`, `stg_live__dex_trades_balancer_v2.sql` |

---

## Open questions

1. Is the 18-hour lag an active operational incident or an accepted pre-rollout state given `enabled: false`? Confirm before trusting any metric from this feed.
2. What is `0x2086f52651837600180de173b09470f54ef74910`? It accounts for 25.6% of current-window swaps with no USD valuation. Legitimate new token (add to whitelist) or test/honeypot token (filter)?
3. For `trade_usd` on multi-hop routes: is `MAX` across hops the intended user-facing semantic for the tile, or should it be anchored to the entry/exit hop? Decide before disclosing the figure externally.
4. Was the `overlap_minutes` change from 120 to 15 intentional (reduced scan cost), and should `schema.yml` be updated to match? Is there a `dbt_project.yml` override that sets 120 at the project level?
5. Are `api_execution_live_*` marts intentionally excluded from the `production`/`api:` convention (a separate `live` tier), or is this an oversight? If intentional, document the exemption.
6. Is there a plan for a CoW Protocol live events model? GPv2Settlement decoding is architecturally different from AMM Swap decoding.
7. Should GHO be added to `tokens_whitelist.csv` to close the Balancer V3 GHO pricing gap, or should the `waGnoGHO` entry be removed from `stg_pools__balancer_v3_token_map` until it is whitelisted?

---

## Review log

| Round | Action | Outcome |
|---|---|---|
| 1 | Inspector raises HIGH finding: all four api_* marts would fail check_api_tags.py CI. | Round 2 rebuttal: sustained — guard gates on `production` tag (line 53); non-production models are silently skipped, not failed. Finding downgraded to MEDIUM. |
| 1 | Inspector raises MEDIUM finding: MAX per-tx USD on feed tile may inflate multi-hop routes. | Round 2 confirmation: confirmed and elevated to HIGH — direct code read shows api_execution_live_trades_hourly_48h uses SUM per-hop (line 22) vs feed tile MAX per-tx (line 57); cross-mart inconsistency is irreconcilable for external users. |
| 2 | Both challenges resolved; all remaining findings agreed between inspector and context reports. | Converged. |
