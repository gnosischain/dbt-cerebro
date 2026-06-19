# cron_preview remediation — implementation results

Companion to `docs/cron_preview_findings.md` (diagnosis) and the plan. Records what was actually changed, verified, and deferred.

## Summary
All 7 original failures from the 2026-06-07 run are fixed or removed from the production graph; the UBO chain (the largest blast radius) is rebuilt and current. Runtime fix (F2) is designed and pending one coordination point.

## Code changes (committed to working tree)
| Area | File(s) | Change |
|------|---------|--------|
| F9 classifier | `scripts/refresh/classify_failed_nodes.py` | OOM (241/MEMORY_LIMIT) removed from TRANSIENT (retry was futile); added SSL/UNEXPECTED_EOF/HTTPSConnectionPool/RemoteDisconnected/ConnectionResetError/Broken pipe. Unit-checked. |
| F9 retry policy | `scripts/full_refresh/refresh.py` | Split retry: network → 5 retries; memory → retried ONLY as OvercommitTracker victim, capped to 3; bare OOM = permanent (no 15-min futile backoff). |
| F7 RWA bounds | `models/execution/rwa/intermediate/int_execution_rwa_backedfi_prices.sql` + `schema.yml` | Added `start_month/end_month` branch + `meta.full_refresh` (start 2023-04-01, earliest oracle) + memory pre/post hooks. |
| F3 / memory | `int_ubo_claims_curve_daily.sql`, `int_ubo_claims_sdai_daily.sql` | Kept as `table` (NOT incrementalized — would corrupt proportional balances); added spill hooks; removed their `full_refresh` meta so refresh.py won't run them unbounded. |
| F1 memory | `fct_ubo_supply_claims_daily.sql`, `fct_ubo_known_containers_daily.sql`, `int_ubo_second_level_daily.sql` | Added memory pre/post hooks (6/2 GiB, `=0` resets = profile cap). Did NOT lower caps on ASOF/windowed claim models that pass today. |
| F6 gpay | `int_execution_gpay_wallet_owners.sql` | Replaced hand-written `became_owner_at > max()` (partition-wipe bug under insert_overwrite) with `apply_monthly_incremental_filter`; rebuilt via `--full-refresh` to fix Code 36 ordering drift. |
| F5 consensus | `models/consensus/intermediate/int_consensus_validators_income_daily.sql` | Added date predicate to `network_state` CTE so the join prunes partitions (pure optimization, no result change). |
| Circles v1 → dev | `contracts_circles_v1_Hub_{events,calls}.sql` | Re-tagged `production` → `dev` per owner (legacy v1); removed from production graph (0 production downstream). |

## DB operations (verified)
- **F6 gpay**: rebuilt; physical `ORDER BY` now `(pay_wallet, owner)`; rebuilt rows == source-of-truth (`current_owners ⋈ gpay_wallets`), 0 diffs; 2nd incremental run clean (no Code 36). Backup `int_execution_gpay_wallet_owners__bak_20260608`.
- **F1/F8 UBO chain bootstrap** (bounded via refresh.py + memory hooks):
  - `fct_ubo_supply_claims_daily`: recomputed 32/32 batches, 2021-09 → 2026-06-07, 52.4M rows (was 16 months stale at 2025-02 — the 092 OOM cause). Baseline aggregates in `ubo_baseline_20260608`.
  - `fct_ubo_known_containers_daily`: rebuilt.
  - `int_ubo_second_level_daily`: bootstrapped 21/21.
  - `fct_ubo_supply_claims_resolved_daily`: bootstrapped 63/63, 53.2M rows (was MISSING → Code 60).
  - `fct_execution_tokens_ubo_{coverage,venue_breakdown}_latest`: exist (were Code 60).
- **Drift audit**: of 255 refactor-touched models, 3 real drifts — gpay (fixed), and 2 Circles **v1** models (moved to dev, not rebuilt). `int_execution_circles_v1_balance_diffs` drift (3-col vs 7-col dedup, ~11.9K events lost) is now a dev-only concern. **0 production drift remains.**

## Notable cluster behavior
- ClickHouse Cloud was under heavy cross-tenant memory pressure; many UBO batches were `OvercommitTracker` victims (RSS at the 10.80 GiB cap before our query allocates). The new victim-retry policy recovered them; bare OOM never occurred on the bounded batches.

## F2 runtime — DONE & validated
Per-slice `--vars` forces a full project reparse (~20s/slice; `dbt/parser/manifest.py:877`). Env vars used in model SQL are re-parsed per-file (`dbt/parser/partial.py`). Fix: route the slice date + stage vars via `DBT_MB_*` env vars read by a new `mb_var()` helper (falls back to `--vars` so refresh.py is unchanged).
- New `macros/db/mb_var.sql`; wired into `apply_monthly_incremental_filter`, `decode_logs`, `decode_calls`, the 3 consensus validator models (date + validator ranges) and `int_execution_account_token_movements_in_daily`; runner `run_one_slice` sets `DBT_MB_*` instead of `--vars`.
- Measured: per-slice parse **~20s → ~8s** (partial, no full reparse). End-to-end: `int_execution_gpay_roles_events` ran 5 real slices via the runner with **0 full reparses, 0 errors**. Compile verified correct for env path, `--vars` path (refresh.py), and plain path (steady-state). Biggest win on the cheap-query consensus models whose 16–26 min batches were reparse-dominated.

## Deferred (not green-blockers)
- **F5b**: `int_consensus_validators_per_index_apy_daily` APY correctness (mod-32 / MaxEB) — owner actively updating this model (refresh completed 2026-06-08); SQL logic left untouched (only the var-reads were swapped to `mb_var`, no logic change).
- **Source freshness**: failing sources map to dead external `*-daily-ingestor` containers — outside this repo; needs ingestion restart or a warn-vs-error decision on the gate.

## Verification still to run (after F2 / clean window)
- Full `cron_preview.sh` re-run: `failed_batches/` empty, zero FAIL, reduced wall-clock.
- UBO dbt tests (conservation invariant `sum(balance) per (date,token) == pool reserve`, non-negative balances).
