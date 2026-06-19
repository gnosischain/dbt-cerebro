# cron_preview run — findings & fix tracker

Run started: 2026-06-07 19:31 UTC (container `dbt`, PID 56848)
Log: `logs/cron_preview_run.log`  |  Selector: full `tag:production` graph
Status: **COMPLETE** — 2026-06-08 03:43 UTC. Total run time **~8h12m**. 120 run batches; **7 failed steps**.

---

## Live status log
- 19:31 — cleanup (tmp-tables, dbt-trash, kill-failed-mutations) OK
- 19:31 — source-freshness OK
- 19:33 — circles-metadata-fetch OK (150 ok / 2 failed of 152, IPFS)
- 19:33 — dbt-run batches begin
- 19:34–19:36 — batches 001–004 (ESG + consensus) clean
- 19:36→19:53 — batch 005: `int_consensus_validators_snapshots_daily` microbatch — **17 min** (see F2 numbers)
- 19:53→19:54 — batch 006 (consensus consolidations/deposits) ~77s, clean
- 19:54→20:19 — batch 007 (consensus withdrawals/income/apy, 13 models) — **~25 min** (see F5)
- 20:19→20:22 — batches 008-010 (api views) fast, clean
- 20:22→20:47 — batch 011 (contracts AgentResultMapping/Balancer + stg_pools) **~25 min**: 48 inv / 47 reparses, 470s query vs 1530s wall (~70% reparse). Decode models ~10-14s each.
- 20:47→20:59 — batch 012 (Circles contracts) ~12 min
- 20:59→ — batch 013 (more Circles contracts). Many Circles contract models remain => long tail.
- Failures so far: **0** (no Code 241, no ERROR>=1)
- NOTE: contract decode itself is cheap (~10-14s); F2 reparse overhead dominates these batches too.
- 21:07→22:38 — batches 014-020: all Circles contracts + circles intermediates. Clean. Batch 016 (Hub events) ~22 min (largest table). Each batch ~8-25 min, dominated by F2 reparse x microbatch slices.
- Run elapsed ~3h7m, still in contracts layer; pools/prices/UBO chain still ahead.

### Batch 007 measured
- 1482s wall; 31 invocations, 30 reparses; **742.6s real query** (~50% here, because the model is genuinely heavy)
- Slowest slices: `int_consensus_validators_income_daily` 75s/61s/40s..., `int_consensus_validators_per_index_apy_daily` 53s

### Batch 005 measured (proof for F2)
- 1014s wall-clock; **40 dbt invocations, 39 full reparses (97.5%)**
- Actual query time = **99.2s** across 46 model builds
- => **~90% (~915s) was dbt parse/startup overhead, ~10% real ClickHouse work**

---

## Findings that need fixes

### F1 — UBO marts OOM (ClickHouse code 241, MEMORY_LIMIT_EXCEEDED)  [SEV: HIGH]
- Symptom (prior run): `fct_ubo_supply_claims_daily` used 10.81 GiB vs 10.80 GiB cap → failed → skipped all downstream (top-holders, venue-breakdown, coverage).
- Root cause: `int_ubo_claims_*_daily` build a per-(ubo,symbol,container) **daily calendar via `ARRAY JOIN range(num_days+1)`** then a cumulative `sum() OVER (... UNBOUNDED PRECEDING)` window — O(keys x days_of_history) when NOT incremental. Plus an `ASOF LEFT JOIN int_execution_token_prices_daily` for `balance_usd`.
- Trigger: runs as full build (is_incremental()=false) — e.g. first run post-refactor, table dropped, or `on_schema_change='sync_all_columns'` rebuild → calendar spans all history.
- Files: `models/execution/ubo/intermediate/int_ubo_claims_uniswap_v3_daily.sql` (calendar L328-346, window L349-376, price ASOF L394-400); same pattern in aave/balancer/curve/sdai/swapr claims.
- Fix candidates: enforce incremental on first build via bounded backfill (refresh.py monthly stages); cap calendar window; push `WHERE date >=` predicate into ASOF price subquery; consider `max_bytes_before_external_group_by` / external sort settings for these models.

### F2 — Microbatch per-slice FULL reparse overhead  [SEV: MEDIUM, broad]
- Symptom: each daily slice = `dbt run --vars {incremental_end_date:...}` → "Unable to do partial parsing because config vars ... have changed" → full project parse (~3000 nodes). 20 of 30 invocations reparsed.
- Impact (MEASURED on batch 005): 1014s wall-clock, 99.2s real query, 39/40 invocations reparsed → **~90% overhead**. Multiplied across every annotated incremental model x every backfill day.
- Files: `scripts/refresh/dbt_incremental_runner.py` (per-slice dbt invocation with changing --vars).
- Fix candidates: keep vars stable across slices (pass date via env/selector var that doesn't bust partial parse), or write/reuse a static manifest (`dbt parse` once + `--state`/`--defer`), or batch multiple days per invocation, or set PARTIAL_PARSE-friendly var passing.

### F3 — UBO parents materialized as `table` (full rebuild every run)  [SEV: MEDIUM]
- `int_ubo_claims_curve_daily`, `int_ubo_claims_sdai_daily`, `fct_ubo_known_containers_daily`, `fct_ubo_address_classification` are `materialized='table'` → reprocess all history each run.
- Fix candidates: convert to incremental where the grain allows, or accept full rebuild but bound history.

### F4 — contracts + prices + UBO converge into one lineage chain  [SEV: INFO]
- `fct_ubo_supply_claims_daily` has 23 contracts + 4 prices models upstream; `dbt_run_batches.py` groups whole chains → contracts/prices/UBO land in the same batch (the "contracts split into prices" observation). Not a bug; explains batch composition and blast radius when UBO fails.

### F5 — `int_consensus_validators_income_daily` genuinely slow query  [SEV: MEDIUM]
- Per daily slice 40-75s of real ClickHouse time (not parse overhead). `int_consensus_validators_per_index_apy_daily` ~53s too.
- These dominate batch 007 wall-clock alongside F2 reparse cost.
- File: `models/consensus/.../int_consensus_validators_income_daily.sql` (to inspect — likely large per-validator-per-day join/window over the full validator set).
- Fix candidates: review join/window, partition pruning, reduce per-slice scan; possibly widen batch_days so fixed parse cost amortizes over more days.

---

## IMPLEMENTATION RESULTS (2026-06-08, post-approval)

All fixes implemented + verified. Status by finding:
- **F9 classifier** — `classify_failed_nodes.py`: 241/MEMORY removed from transient (OOM no longer futile-retried); SSL/connection drops added. `refresh.py` own retry: bare OOM = permanent, OvercommitTracker victim retried (cap 3). Unit-checked. DONE.
- **F7 RWA** — bounded `start_month/end_month` branch + `meta.full_refresh` (start 2023-04-01, earliest oracle) + memory hooks. Rebuilds clean in 3.9s (was 028 OOM). DONE.
- **F3 / Step 3 memory hooks** — added (6/2 GiB, `=0` resets) to `fct_ubo_supply_claims_daily`, curve, sDAI, known_containers, second_level, RWA. NOT lowered on passing ASOF/windowed claim models. curve/sDAI kept as tables; their `full_refresh` meta removed so refresh.py won't run them unbounded. DONE.
- **F6 gpay** — drift audit found physical `(pay_wallet)` vs declared `(pay_wallet,owner)`. `--full-refresh` rebuild → Code 36 gone (both full + daily insert_overwrite runs pass); rebuilt = exact source-of-truth. Also fixed its hand-written incremental filter (was wiping earlier-in-month owners) → `apply_monthly_incremental_filter`. DONE.
- **Drift audit (Step 4)** — 3 drifts: gpay (fixed); `int_execution_circles_v1_balance_diffs` + `contracts_circles_v1_Hub_events` → Circles v1 is legacy → **moved to `dev`** (per owner) instead of rebuilt. 0 production drift remains.
- **F1 / F8 UBO chain (Step 5)** — `fct_ubo_supply_claims_daily` was 16 months stale (max 2025-02) → its incremental window spanned 16mo → the 092 OOM. Recomputed via refresh.py `--incremental-only` in 32×2-month batches (current 2021-09→2026-06, 52.4M rows). Several OvercommitTracker-victim OOMs along the way, all auto-recovered by the new retry policy. Then bootstrapped the MISSING downstream: `known_containers`, `second_level` (21/21), `resolved` (63/63, 53.2M), `coverage`/`venue`/`top_holders` + api views (the Code-60 / 093 failures). All 31 UBO correctness tests PASS (conservation, non-negative, uniqueness). Daily incremental path now 2.5s (was OOM). DONE.
- **F5 income** — `network_state` CTE date-pruned (pure optimization, no result change). DONE. **F5b** (per_index_apy rescope) deferred — needs rewiring multiple consumers; not a green-blocker.
- **F2 / Step 6 runtime** — per review: batch_days NOT raised uniformly (widens per-INSERT memory), threads kept at 1 (batcher groups by chain count, not memory). Reparse overhead is catch-up-only; structural fix deferred to investigation.
- **Source freshness (Step 1b)** — 5 genuinely-stale EXTERNAL sources: `crawlers_data.dune_prices`, `execution.{balance_diffs,code_diffs,nonce_diffs,storage_diffs}` — dead `dune-prices` / `cryo-*` ingestor containers (outside this repo). Gates left as ERROR (data is stale). Not mandatory in cron_preview. Needs ingestor restart.

Backups retained (drop after a full cron re-run validates): `fct_ubo_supply_claims_daily__bak_*` was not created (copy OOM'd; recompute is non-destructive `--incremental-only`); `int_execution_gpay_wallet_owners__bak_20260608`; baseline `ubo_baseline_20260608`.

---

## FINAL RESULTS (run complete 03:43 UTC)

Total wall-clock **~8h12m** (19:31 -> 03:43). Failed steps:

| Step | Model(s) | Error | Class |
|------|----------|-------|-------|
| source-freshness | (sources) | rc=1 | freshness |
| dbt-run:027 | contracts_backedfi_*_Oracle_events | Code 241 OOM (11.56 GiB) + SSL UNEXPECTED_EOF | F7 (prices src) |
| dbt-run:028 | int_execution_rwa_backedfi_prices chain | Code 241 OOM (11.05 GiB) | F7 (prices) |
| dbt-run:075 | int_execution_gpay_wallet_owners | Code 36 BAD_ARGUMENTS "Tables have different ordering" | F6 |
| dbt-run:092 | fct_ubo_supply_claims_daily | Code 241 OOM (10.80 GiB); skipped 6 downstream | F1 (reproduced) |
| dbt-run:093 | fct_execution_tokens_ubo_coverage_latest, ..._venue_breakdown_latest | Code 60 UNKNOWN_TABLE (fct_ubo_supply_claims_resolved_daily / fct_ubo_known_containers_daily missing) | F8 |
| dbt-run:retry-transient | fct_ubo_supply_claims_daily | OOM again on retry | F1 |

Everything else PASS: 113 run batches green, all dbt tests PASS, docs/semantic/edr-report PASS.

### F1 CONFIRMED this run
- `fct_ubo_supply_claims_daily` OOM'd at exactly the 10.80 GiB cap, **twice** (initial + transient retry). The TRANSIENT retry is futile for OOM — see F9.

### F6 — int_execution_gpay_wallet_owners: Code 36 "Tables have different ordering"  [SEV: HIGH, real bug]
- `models/execution/gpay/intermediate/int_execution_gpay_wallet_owners.sql`. ClickHouse BAD_ARGUMENTS — happens on INSERT into a *MergeTree whose ORDER BY differs from the SELECT's implied ordering, or a UNION/JOIN of tables with mismatched ORDER BY. Deterministic (not memory/network). Likely introduced/!exposed by the refactor.
- Fix: align the model's `order_by`/engine with its inputs, or drop+recreate the existing table whose ORDER BY drifted from the new model config.

### F7 — backedfi Oracle decode + RWA prices OOM (+ transient SSL)  [SEV: HIGH, prices]
- batch 027/028: `contracts_backedfi_*_Oracle_events` and `int_execution_rwa_backedfi_prices`/`fct_execution_rwa_backedfi_prices_daily` hit Code 241 (11.56 / 11.05 GiB). Also intermittent `SSLError: UNEXPECTED_EOF_WHILE_READING` connection drops to ClickHouse Cloud.
- This is a **prices** failure: backedfi -> int_execution_rwa_backedfi_prices -> int_execution_token_prices_daily. When it fails, downstream price-dependent models use partial/zero prices.
- Fix: same OOM remediation as F1 for the decode/price models; investigate the connection drops (keepalive / send_receive_timeout / retry).

### F8 — UBO base tables never built -> downstream Code 60 UNKNOWN_TABLE  [SEV: HIGH, cascade]
- `fct_execution_tokens_ubo_coverage_latest` and `fct_execution_tokens_ubo_venue_breakdown_latest` reference `fct_ubo_supply_claims_resolved_daily` and `fct_ubo_known_containers_daily`, which **do not exist** in the DB (never successfully built because the UBO chain OOMs at F1). This is a cascade of F1, but the hard "relation does not exist" means these base UBO tables have never had a successful first build.
- Fix: bootstrap the UBO base tables once via bounded backfill (refresh.py monthly stages) so the incremental path can take over; then F1 keeps them healthy.

### F9 — TRANSIENT classifier mishandles OOM and network drops  [SEV: MEDIUM]
- `scripts/refresh/classify_failed_nodes.py` regex marks **Code 241 (MEMORY_LIMIT) as TRANSIENT** -> it retries an OOM that will deterministically OOM again (wasted ~12 min on retry-transient this run).
- Conversely, the **SSL `UNEXPECTED_EOF_WHILE_READING` / `HTTPSConnectionPool`** drops are NOT matched by the regex -> backedfi got marked PERMANENT (no retry) even though it was a genuine transient network drop.
- Fix: remove 241 from TRANSIENT (or cap to 1 retry with a memory-reducing var); add SSL/HTTPSConnectionPool/RemoteDisconnected patterns to TRANSIENT.

## Answers to the open questions
- UBO batch OOM'd again (not incremental-safe yet) -> F1 still open, base tables missing (F8).
- TRANSIENT retry did NOT recover the OOM (re-OOM'd) -> F9.
- Slowest batches by wall-clock: 005 (~17m) and 007 (~25m) — F2 reparse dominates; 016/021 (~22-27m) are big contract decode tables.
