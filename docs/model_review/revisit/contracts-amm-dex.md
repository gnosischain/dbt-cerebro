# Model review (revisit 2026-06-21): contracts/AMM-DEX

## Re-verification 2026-06-30 (live prod `dbt` + `playground_max` + code)

All 16 cases re-checked. `git log` since 06-21 touched this sector twice: `fbd7e35e` (06-24, "refactor logic for fees for balancer") and `0474230d` (06-23, "add contracts for revenue exclusion via non user accounts", touches `seeds/contracts_whitelist.csv`). **Net: 0 fully resolved; 1 genuine partial fix built but not yet in prod; C01/N01 untouched by the whitelist commit (it added unrelated addresses); C07 only half-fixed.**

### C09 (critical) — reframed: NOT a simple filter bug, a structural token-pricing coverage gap
Investigated the actual TVL blocker (per user query) via `int_execution_pools_balancer_v2_daily`, which already exists and is live in prod (`908,058` rows, `1,174` pools, current to today) computing per-pool/token TVL with a `LEFT JOIN stg_pools__tokens_meta` + ASOF price join. On the latest day: of `1,172` pools, only **`60` (5.1%) have every token priced**; **`1,112` (94.9%) have at least one token with `token IS NULL`** (unknown to `stg_pools__tokens_meta`) — `987` distinct unpriced token addresses in total. Root cause: `seeds/tokens_whitelist.csv` has only **`47` tokens total**, platform-wide (used by every TVL/pricing path, not just BalancerV2). This whitelist works for Uniswap V3 / Swapr V3 because those pools are manually curated (`contracts_whitelist.csv`, ~22-29 pools, mostly blue-chip pairs) — but BalancerV2 pools are auto-discovered from `PoolRegistered` events with no curation (`1,172` pools), so it has a vastly larger long-tail of exotic/small-cap tokens the platform has never priced. GHO specifically (`0xfc421ad3c883bf9e7c4f42de845c4e4405799e73`, the C14 wrapper-underlying) is confirmed **absent from `tokens_whitelist.csv` entirely** — `0` rows for that address anywhere in the V2 daily table.
**Concentration analysis (is this fixable by pricing "just a few" tokens?): No — confirmed genuine long tail, not a fixable head.**
- Token distribution across the `987` unpriced addresses: **`903` (91.5%) appear in exactly `1` pool**; `72` in 2-3 pools; only `12` appear in 4+ pools (max `16`, for `BAL`). There is no small set of tokens that explains the majority of affected pools.
- Identified the top unpriced tokens on-chain (`symbol()` calls): `0x7ef541...` = **BAL** (Balancer's own governance token, in `16` pools) and `0x0aa1e9...` = **crvUSD** (Curve's stablecoin) are both real, liquid, well-known tokens simply missing from the `47`-row whitelist — legitimate, cheap whitelist additions. But `0x2086f5...` = **staBAL3**, a nested Balancer stable-pool BPT (LP token) held as a constituent inside another pool — this needs *recursive* TVL computation (share price of the inner pool), not a simple oracle/whitelist price; it's a structural gap, not a data gap. Two further top-10 addresses failed basic `symbol()` calls entirely (`0x159e68...`: "function not found"; `0xfef5f9...`: "ABI not found") — non-standard contracts that a whitelist can't fix regardless.
- **Quantified test:** adding `BAL` alone to the whitelist would only fully resolve `3` of its `16` pools — the other `13` have at least one *additional* unpriced token in the same pool, so a single-token fix doesn't clear even its own best case.
- **Conclusion:** whitelisting the handful of legitimate, liquid tokens (BAL, crvUSD, and similarly-identified others) is worth doing — free, correct, no downside — but will not materially move the `94.9%`-unpriced number, because most affected pools combine multiple unpriced legs (long-tail exotic tokens + nested BPTs + non-standard contracts). This is not a "price a few tokens and you're done" problem.
**Implication for the fix:** simply adding `'Balancer V2'` to the `fct_execution_pools_daily.sql` protocol filter would surface only the `60` fully-priced pools (5.1% of the protocol) — a partial figure that could be more misleading than the current full exclusion (readers could mistake "V2 TVL = X" for the whole protocol when it is 5% of it). Whitelisting BAL/crvUSD/etc. would nudge this up marginally, not fix it. A correct fix needs either (a) a recursive BPT-pricing layer for nested pools (addresses the structural share, not the long tail), (b) sourcing prices from a broader feed for the genuine long tail — but `crawlers_data.dune_prices` is keyed by `symbol` only (no address column), so this would require an address-to-symbol resolution step first plus accepting symbol-collision risk (the same class of issue C15 already flagged for EURe/GBPe) — non-trivial, or (c) exposing V2's `60`+ known-token pools (plus BAL/crvUSD once whitelisted) as a clearly-labeled "partial coverage" slice while leaving the aggregate `api_execution_pools_*` totals V2-excluded and documented as such. Recommend: whitelist BAL/crvUSD now (free), ship (c) short-term, treat (a)/(b) as a real but separate project — not a quick fix.

**TVL dollar-value gap (not just pool count) — severity likely overstated relative to actual dollar impact.** Per user request, quantified the actual dollar gap rather than pool counts, using `int_execution_pools_balancer_v2_daily` on the latest day:
- `60` fully-priced pools: **`$170,847.94`** known TVL.
- `1,082` partially-priced pools (>=1 known leg, >=1 unknown leg): **`$297,937.33`** known on the priced leg(s) only.
- `30` pools with **zero** price signal on any leg: fully opaque, no basis for estimation.
- Note: the model's own `pool_implied_price_usd` column (designed to infer an unpriced leg's price from the other priced leg in the same pool) is **broken for this purpose** — verified `0` of `1,162` unpriced legs have a non-null implied price, because the underlying subtraction (`sum(tvl) - this_row_tvl`) propagates NULL when the row's own `tvl_component_usd` is NULL. Not usable as-is; would need a rewrite (e.g. `sum(tvl) FILTER not-self` instead of `window_sum - self`) to actually backfill.
- **Total known TVL floor today: `$468,785.27`.** If the `1,082` partial pools are assumed roughly balanced 2-leg pools (a reasonable approximation for most Balancer V2 Weighted pools, though not exact for ComposableStable/3+-token pools), the unpriced legs in those pools are plausibly a similar order of magnitude to their known legs (~`$300K` more) — i.e. **total plausible BalancerV2 TVL is on the order of `$500K-$1M`**, not a number commensurate with "single largest AMM by event count" (`25,975,780` events). Event count on a permissionless, uncurated AMM captures pool-creation churn and dust-value activity, not economic weight.
- **Recommendation: downgrade C09's severity** pending a firmer estimate (e.g. an actual BAL/crvUSD price lookup once whitelisted, to convert the `297,937` implied-leg estimate into a real number). The *documentation* problem (schema.yml claiming full V2/V3 coverage) remains real and should stay high-priority regardless of the dollar-size finding — but the underlying "missing TVL" is likely a few hundred thousand dollars, not a magnitude that justifies "critical."
**Also unrelated to C09 but confirmed same day:** the 06-24 `fbd7e35e` commit adds `contracts_BalancerV2_Pool_events` (decodes `SwapFeePercentageChanged`/`ProtocolFeePercentageCacheUpdated`, the fee-rate data the Vault Swap event lacks) feeding a **new, separate** model `int_execution_yields_balancer_lp_fees` (per-LP fee attribution for the yields/portfolio surface — the fix for `EXECUTIONYIELDS-C12`, a different sector's finding, not C09). Well-built, verified in `playground_max` (`1,722` rows, `1,106` pools, `0%` blank `event_name`), but **not deployed** — `contracts_BalancerV2_Pool_events` / `contracts_BalancerV2_pool_registry` return `UNKNOWN_TABLE` in prod `dbt`.

### C07 (medium) — half-fixed, and the fix targeted the wrong risk order; guard IS proven in prod
The new (06-24) `contracts_BalancerV2_Pool_events.sql` carries `pre_hook=["SET allow_experimental_json_type = 1", "SET max_block_size = 5000"]` / matching post_hook reset to `65505` — but this new model has only `1,722` rows in dev, negligible OOM risk. The two models that actually need the guard remain unfixed: `contracts_BalancerV2_Vault_events.sql` (**`25,975,780` rows — the single largest of all six event tables**) and `contracts_BalancerV3_Vault_events.sql` (`5,146,834` rows) both still have pre_hook = `["SET allow_experimental_json_type = 1"]` only, confirmed by direct read 2026-06-30.
**Is the guard proven to work? Yes, at prod scale up to `11.4M` rows.** `grep` for `max_block_size` across `models/contracts/` returns exactly `3` files: the new BalancerV2_Pool_events, `contracts_UniswapV3_Pool_events.sql` (`5,428,892` rows, prod-live, no OOM reported in either review pass), and `contracts_CowProtocol_GPv2Settlement_events.sql` (`11,380,223` rows — the second-largest table in the sector, prod-live, also clean). So the setting is a proven, working pattern in this exact codebase at up to `11.4M` rows. BalancerV2 Vault at `25,975,780` rows is `~2.3x` larger than the biggest proven case — applying the same guard is a low-risk, well-precedented extrapolation (same mechanism, same macro, same source table), but is not literally proven at that exact row count since no model of that size has used it yet. Recommend applying it — the downside of a conservative `max_block_size` is a minor performance cost, not a correctness risk.

**FIXED 2026-06-30.** Applied `pre_hook=["SET allow_experimental_json_type = 1", "SET max_block_size = 5000"]` / `post_hook=["SET allow_experimental_json_type = 0", "SET max_block_size = 65505"]` to both `contracts_BalancerV2_Vault_events.sql` and `contracts_BalancerV3_Vault_events.sql`, matching the proven pattern exactly. C07 now closed for all six event tables in the sector.

### C01 / N01 (critical / high) — untouched, still fully open; exact mechanism confirmed
`0474230d` (06-23) touched `seeds/contracts_whitelist.csv` but added 3 *unrelated* contract types (`AaveV3Collector`, `CowSwapSettlement`, `ATokenVault` — for revenue-exclusion purposes, a different workstream), not the 7 missing UniswapV3 pool addresses from the 05-14/05-21 commits. Confirmed live: deployed `dbt.contracts_whitelist` still resolves `22` UniswapV3Pool / `12` SwaprPool (34 rows, unchanged); CSV on disk now has `29` UniswapV3Pool + `12` SwaprPool + 3 other-type rows (44 total). `dbt seed` has still never been run.
**Two distinct, sequential defects, confirmed from source:**
1. **N01 — the model can't even see the new pools.** `contracts_UniswapV3_Pool_events.sql` line 20 reads `contract_address_ref = ref('contracts_whitelist')` — it decodes only addresses present in the *deployed seed table*, not the CSV on disk. Until `dbt seed` re-runs, the 7 new addresses are entirely invisible to the model, not merely missing history.
2. **C01 — even after `dbt seed`, a normal run won't backfill history.** `macros/decoding/decode_logs.sql` lines 226-236 implement the incremental gate as `block_number > {{ _wm_bn }}` where `_wm_bn` is the *target table's current max block_number* (confirmed today: `46,959,493`), fetched fresh at every render. The macro's own design comment (lines 212-216) states this is deliberate: "Daily no-overlap watermark... Reorg/late corrections go through a targeted `--full-refresh`, never the daily path" — chosen for performance (partition pruning: `9s` vs `112s` per run). This means the watermark can only move forward; it structurally cannot backfill a newly-whitelisted address's history, no matter how many days the daily job runs. `contracts_UniswapV3_Pool_events` still resolves exactly `22` distinct pools as of today, confirming the 7 new pools are still fully unbackfilled.
**Fix (sequential, not parallel):** (1) `dbt seed` to sync the whitelist table with the CSV — resolves N01 and makes the model *aware* of the 7 addresses; (2) `dbt run --full-refresh -s contracts_UniswapV3_Pool_events` (or the repo's batched `refresh.py` approach, given `execution.logs` is a large shared source table) to re-scan from `start_blocktime='2022-04-22'` for all 29 addresses and actually backfill the 7 new pools' history — resolves C01. Step 2 cannot be skipped or replaced by waiting for the daily incremental job.

### C02 (high) — FIXED 2026-06-30
Regenerated `models/contracts/Swapr/schema.yml` for `contracts_Swapr_v3_AlgebraFactory_events` and `contracts_Swapr_v3_AlgebraPool_events` against the verified 8-column decode output (confirmed live via `describe_table`: `block_number, block_timestamp, transaction_hash, transaction_index, log_index, contract_address, event_name, decoded_params`). Also checked the two `_calls` models (`AlgebraFactory_calls`, `AlgebraPool_calls`) — their existing schema.yml already matches the real 9-column calls layout exactly, no fix needed there. Zero risk: no test referenced the removed phantom columns.

### C05 (medium) — explained; not yet fixed
Mechanism confirmed by reading `macros/decoding/decode_logs.sql` directly: `start_blocktime` is applied unconditionally (line 202-203, `AND incremental_column >= toDateTime(start_blocktime)`), while the watermark bound (`block_number > max(block_number)`, line 226-236) is applied ONLY `{% if not flags.FULL_REFRESH %}`. So on a normal daily run the watermark supersedes `start_blocktime` once the table has data (no cost impact today). But on a `--full-refresh`, the watermark logic is skipped entirely — the ONLY lower bound left is the stale `start_blocktime` literal, so the engine has to evaluate/batch-scan `execution.logs` (in `batch_months`-sized chunks) across the entire dead window before the contract's real deployment, finding zero matching rows the whole time. Cost-only (no data risk), but avoidable: BalancerV2's `22`-month dead window and Swapr's `19`-month dead window are the largest offenders. Not yet fixed — mechanical, low-risk, straightforward to batch with C11/C12 (same literal-correction class).

### C13 (medium) — where to document: no clean existing slot, recommend a new doc
Checked for an existing documentation convention for seeds: `dbt_project.yml`'s `seeds:` block only configures ClickHouse `+column_types` (e.g. `contracts_whitelist` is typed `address: String, contract_type: String` — no room for a notes/criteria field), and there is no `seeds/schema.yml` or per-seed markdown anywhere in the repo (`Glob seeds/**/*.yml` = 0 hits). This repo's convention for platform-level explanations is free-standing files in `docs/` (e.g. `docs/native_token_prices_build_plan.md`, `docs/economic_concepts.md`). Recommend either (a) a new `docs/contracts_whitelist_criteria.md` following that pattern, or (b) a dbt-native `seeds/schema.yml` with a `seeds:` block and `description:` field (shows up in `dbt docs generate`/catalog, more discoverable long-term but a new pattern for this repo). Not yet created — awaiting a decision between (a)/(b).

### C10 (high) — investigated; REFUTED the baseline's "wrong-chain" verdict, gap is real and fixable
Called the target contract directly on Gnosis Chain: `coins(0)` = `0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d` (WxDAI), `coins(1)` = `0xDDAfbb505ad214D7b80b1f830fcCc89B60fb7A83` (USDC), `coins(2)` = `0x4ECaBa5870353805a9F068101A40E0f32ed605C6` (USDT) — all three match `seeds/tokens_whitelist.csv` exactly. **This definitively confirms `0x7f90122bf0700f9e7e1f688fe926940e8839f353` IS the live, correct Curve 3pool (WxDAI/USDC/USDT) on Gnosis Chain** — the 06-21 revisit's "wrong-chain mainnet/Polygon address" verdict (confidence medium) was itself incorrect. `search_models_by_address` shows this exact address is already referenced by `int_ubo_claims_curve_daily` ("Curve 3pool ... on Gnosis"), so the team has independently already identified it correctly elsewhere. Confirmed `dbt.event_signatures` has `0` rows for this address (no ABI registered) — no `contracts_*` decode model exists for it today; only the separate LP/BPT token contract (`0x1337BedC9D22ecbe766dF105c9623922A27963EC`, decoding Transfer/Approval only) is currently modeled. **The gap is real, but the pool is currently dormant — not worth building for volume.** Checked recent activity directly: `0` log events in the last `60` days, and `0` events over the last `400` days (13+ months) — the pool has had zero Swap/liquidity activity for over a year. It is not dead in the "abandoned, empty" sense: `balanceOf()` on the three constituent tokens confirms it still holds real stranded liquidity — `96,948.70` WxDAI + `103,511.23` USDC + `242,396.85` USDT, **~`$442,857` total** (assuming ~$1 stablecoin pegs), comparable in size to the entire known BalancerV2 TVL. But since there has been no trading for 13+ months, decoding `TokenExchange` would surface a one-time historical dataset with zero ongoing/current volume — it would not close any live "missing DEX volume" gap, because there is no current volume happening here to miss. **Recommendation: do not build the swap-decode pipeline for this pool.** The only remaining reason to touch it at all would be a one-off TVL completeness exercise (tracking the ~$443K stranded balance via simple balance snapshots, not a full swap/fee decode) — low priority given the pool's dormancy, and a fundamentally smaller/different task than what C10 originally proposed.

### C14 (high) — investigated; smaller, more tractable gap than V2, and real tokens identified
Ran the same TVL-based analysis as C09 against `int_execution_pools_balancer_v3_daily` (which already resolves prices via a broader "Dune ASOF join," not just the native whitelist). Results, latest day:

| | BalancerV2 | BalancerV3 |
|---|---|---|
| Total pools | 1,172 | **139** |
| Fully priced | 60 (5.1%) | **58 (41.7%)** |
| Partially priced | 1,082 | 56 |
| Fully opaque | 30 | 25 |
| Known TVL | $468,785 | **$52,886** |

BalancerV3 is a much smaller, more tractable surface (139 vs 1,172 pools) with meaningfully better price coverage already (41.7% vs 5.1% fully priced). Identified the top unpriced tokens on-chain: `0x3e76f9ca...` = **PAXG** (Paxos Gold — real, liquid, tokenized gold; appears in `18` of `139` pools, ~13% of the whole protocol) and `0x4f4f9b8d...` = **GIV** (Giveth's token — real project, `27.6M` raw units across `3` pools). Both are legitimate, identifiable assets simply missing from `tokens_whitelist.csv` — unlike V2, this doesn't look like a 900-token long tail; whitelisting PAXG alone could plausibly move a double-digit number of pools into "fully priced" given its 18-pool footprint. Recommend: whitelist PAXG and GIV, then re-run this same concentration check to see how much of the remaining `81` non-fully-priced pools clears — this gap looks solvable with a handful of additions, unlike C09.

### C03 (high) — FIXED 2026-06-30 (dbt-level), validated end-to-end against prod; infra confirmed correctly deployed
Verified `live.sh` deployment: `infrastructure-gnosis-analytics/aws/deployments/gnosis-analytics/dbt/prod/7_live.tf` runs a correct `kubernetes_deployment` (`dbt-cerebro-live`) executing `/app/live.sh`'s 45s `dbt run --select tag:live` loop — the K8s side is right. But the only `PodMonitor` in the infra repo (`8_podmonitor.tf`) targets the separate `dbt_cerebro_service` (`/metrics` HTTP endpoint), not this deployment — `dbt_cerebro_live` has no container port, no liveness probe, and no Prometheus coverage at all. Checked `elementary.freshness_anomalies` (the pattern used elsewhere, e.g. bridges/consensus): it's a **seasonal anomaly detector** (`test_freshness_anomalies.sql` requires `min_training_set_size`/`days_back`/`seasonality`), not a hard-SLA threshold check — a poor fit for a 45s-cadence table with a 2h TTL design intent. Added a plain singular test instead (matching this repo's existing `tests/*.sql` convention, e.g. `consensus_raw_validators_no_unmerged_dup.sql`): `tests/contracts_live_tables_freshness.sql` checks `dateDiff('minute', max(block_timestamp), now())` across all four `_live` tables against a `180`-minute threshold (`live_freshness_error_after_minutes` var), configurable, returns offending tables. A K8s-level liveness probe remains a possible belt-and-suspenders follow-up but wasn't added (would need a heartbeat file written by `live.sh` and an exec probe — separate infra change).
**Validated 2026-06-30**: ran `dbt test --select contracts_live_tables_freshness` twice with `CLICKHOUSE_DATABASE` overridden per-invocation — `FAIL` (4/4 tables, correctly) against `playground_max` (never refreshed by `live.sh`, only runs against prod/preview), then `PASS` against `dbt` (prod, where `live.sh` actively loops every 45s). Confirms the test logic is correct in both directions with genuine read-only access (no `store_failures` config, pure `SELECT`, no write permission needed). Still not wired into the automatic daily/preview cron (`build_test_batches()` gap noted above) — currently must be run manually.

**Expanded 2026-06-30 (coverage gap caught by user review): the original test only covered the 4 raw contract-decode `_live` tables — missed the rest of `tag:live`.** `grep` for `'live'` tags across the whole project found **13 models tagged `live`**, not 4: the 4 raw decode tables (`live.sh`'s 45s loop) -> 4 staging models (`stg_live__dex_trades_*`) -> 1 unified intermediate (`int_live__dex_trades_raw`) -> 4 API marts (`api_execution_live_trades`, `_hourly_48h`, `_stats`, `_freshness`). A break anywhere in the staging/intermediate/marts segment could leave the actually-served API surface stale while the original test still passed (raw tables fine, downstream broken).
Investigated each layer before rewriting: `api_execution_live_trades.sql`'s own docstring says explicitly *"For ingestion-level staleness, query `api_execution_live_trades_freshness` separately"* — the pipeline's original author already built and designated a dedicated freshness view (`lag_seconds` against `execution_live.logs` directly) for exactly this purpose; nobody was alerting on it. `int_live__dex_trades_raw`'s own description reveals a *different* refresh mechanism — a k8s CronJob on a ~5min cadence doing a heavier 2h delete+insert self-heal, distinct from `live.sh`'s 45s loop for the raw tables. The 4 `stg_live__dex_trades_*` staging models and the `api_execution_live_trades` / `_hourly_48h` / `_stats` marts are all confirmed `materialized='view'` with zero independent refresh lag beyond `int_live__dex_trades_raw` — checking that one table covers all of them, no redundant per-view checks needed.
Rewrote `tests/contracts_live_tables_freshness.sql` to check: the original 4 raw decode tables + `int_live__dex_trades_raw` (covers the whole staging/marts view layer) + `api_execution_live_trades_freshness`'s own `lag_seconds` column directly (the dedicated, already-documented signal) — 6 checks total, full-chain coverage with no redundancy.
**Threshold lowered from 180 to 60 minutes** per user input — still generous relative to both observed cadences (45s raw / ~5min intermediate) but catches a real stall far sooner than the original 3h. Configurable via `live_freshness_error_after_minutes` var.
**Re-validated 2026-06-30**: `dbt test --select contracts_live_tables_freshness` against `dbt` (prod) with the expanded 6-check version -> `PASS`. All of the raw decode layer, `int_live__dex_trades_raw`, and `api_execution_live_trades_freshness` are healthy and within the 60-minute threshold right now. C3 fully closed pending only the `build_test_batches()` orchestrator wiring decision (still outstanding).

### C04 (medium) — FIXED 2026-06-30 (code); requires a follow-up `--full-refresh` to take effect
Clarified the actual mechanism before fixing: in ClickHouse's `ReplacingMergeTree`, `order_by` is the ONLY key the engine uses for merge-time dedup — dbt's `unique_key` config does nothing for `incremental_strategy='append'` models (it only drives behavior for `delete+insert`/`merge` strategies). All four models already declared the *correct* logical key in `unique_key = '(transaction_hash, log_index)'`, but `order_by` included `block_timestamp` — meaning if a reorg ever caused the same `(transaction_hash, log_index)` to be reprocessed with a different `block_timestamp`, the engine would treat it as a new row instead of replacing the old one (permanent duplicate). Fix applied: changed `order_by` to `'(transaction_hash, log_index)'` on all four models (`contracts_UniswapV3_Factory_events.sql`, `contracts_UniswapV3_NonfungiblePositionManager_events.sql`, `contracts_Swapr_v3_AlgebraFactory_events.sql`, `contracts_Swapr_v3_NonfungiblePositionManager_events.sql`) — now identical to their own `unique_key`, closing the gap.
**Operational note (not yet executed): this code change does not take effect on the already-materialized prod tables on its own.** ClickHouse's `ALTER TABLE ... MODIFY ORDER BY` can only *append* columns to an existing sort key, never remove/reorder a leading column — so the four physical tables in prod still have the old `(block_timestamp, transaction_hash, log_index)` primary key until someone runs `dbt run --full-refresh -s <model>` on each. Flagging rather than executing, since a full-refresh is a real warehouse rebuild action (re-scans `execution.logs` for these contracts) — a decision for whoever owns the next deploy, not a side effect of a docs/code review pass.

### C06, C08, C11, C12, C15 — reviewed, no action (per user decision 2026-06-30)
- **C06**: accepted as-is — calls-layer risk, 0 realized collapses across full history, lower priority than events.
- **C08**: confirmed unfixed but static — still exactly `13` null/empty `event_name` rows on `contracts_UniswapV3_Pool_events` (unchanged count as the table grew from `5,428,892` to `5,465,699` rows), all from May 2025. Attempted to identify the specific orphaned event via raw `execution.logs` lookup; query timed out twice (30s) even filtered to a single `address`+`transaction_hash` — not pursued further given the low value of a dormant 13-row artifact.
- **C11, C12**: accepted as-is for now — same "config too early, never too late" class as C05 (zero data-loss risk, cost-only on a full-refresh); candidate for a future batched cleanup with C05.
- **C15**: investigated whether any other model in the codebase joins prices by address instead of symbol — **none do**, because `int_execution_token_prices_daily` (the platform's single price hub) is keyed *only* by `(date, symbol)`, with no address column anywhere. Every consumer (`int_execution_pools_balancer_v2_daily`, `_v3_daily`, `int_execution_prices_dex_ratios`, `int_execution_cow_trades`) joins by symbol for the same structural reason — a real fix would mean adding an address dimension to the hub itself (a bigger, platform-wide change), not a CoW-specific tweak. Accepted as-is given ~0 realized risk today.

---

**Sector status as of 2026-06-30: C01/N01 (UV3 backfill) and C09/C14 (BalancerV2/V3 TVL pricing gaps) remain open and require further work/decisions; C02, C03, C04, C07 fixed this session; C05/C11/C12 identified but deferred (batch candidate); C06/C08/C15 reviewed and accepted as low-priority/latent; C10 investigated and deliberately not pursued (dormant pool, no current volume); C13 investigated, documentation-only, deferred pending a decision on where to record it; BalancerV3 (C14) deliberately skipped for now per user request.

---

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
