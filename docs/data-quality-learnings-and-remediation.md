# Data-Quality: Learnings & Remediation

> **SNAPSHOT (2026-07).** The durable content of this investigation has been extracted
> into per-lesson records with status + evidence at [lessons/INDEX.md](lessons/INDEX.md)
> — read those for current state. This file is kept as the incident chronology; its
> §2–§5 proposals are tracked per-lesson, not here.

Compiled from the 2026-07 "impossible negative token balances" investigation. That
symptom turned out to be a **systemic class** of data-quality issues, not a one-off.
This doc is the learnings + a concrete remediation backlog the team can turn into issues.

> **Status of the fix that prompted this:** the negative balances themselves are fully
> resolved (0 negative real-holder balances, all tokens, all history — WxDAI 461→0,
> ZCHF 56→0, svZCHF 79→0, wstETH 271→0). The OC-sDAI supply-plot `$0` (a related
> pricing gap) was also fixed live, and the L7 weekly-revenue-cohort duplication
> (found + wiped + restored during this work) is resolved with `dup_excess = 0`.
> **Everything in §2–§5 below is proposed, not built.**

---

## TL;DR

| # | Learning | Symptom seen | Fix lever |
|---|---|---|---|
| L1 | **Append-watermark drops late-arriving logs** (all 77 `contracts_*_events` decode models) | Dropped inflows → negative balances (canary); silent undercount everywhere else | `gap_window_refresh.py` to recover a month; detect with §2 |
| L2 | **LATE_START mis-staging** (orchestrator `start_date` post-dates real activity) | wstETH negatives (Balancer Vault, Spark) | per-token stage `start_date`; §2 guard |
| L3 | **Raw `execution.logs` ingestion holes** (below dbt) | 100-block hole dropped 48 WxDAI inflows | raw re-index; detect with block-continuity §2 |
| L4 | **Duplicate-seed drift** in the balances append model | ~430 WxDAI negatives since 2023 | clean recreate (orchestrator batch-1 `--full-refresh`) |
| L5 | **Diagnosis pitfalls** (float vs Int256, block↔date, OOM) | fake "balanced", wrong dates, partition wipes | see playbook (appendix) |
| L6 | **Refresh-lever confusion** | wrong tool → OOM or no recovery | §L6 tool map |
| L7 | **Reprocess/backfill duplication + the unbatched `delete+insert` wipe** | Weekly revenue cohorts doubled at one week (×2 fees & users); a whole-window reprocess then wiped 2026-03→07 | scoped `append` only into empty months; run `reprocess_overwrite` **per `slice`** |
| L8 | **Global-frontier carry-forward drops thin series** (pool daily balances) | Balancer V3 Circles pool 0x155c reserves at 5/48 days; density tracked trade frequency | full-refresh to restore; anchor the incremental window at the **per-entity** frontier |
| OC-1 | **Unpriced wrapper/vault token** → `$0` USD downstream | OC-sDAI supply plot blank | derived-price branch (done for OC-sDAI) |
| OC-2 | **Never-seeded incremental model** | `int_revenue_ocsdai_user_balances_daily` = 0 rows | one-time `--full-refresh` seed |

---

## 1. Root-cause learnings

### L1 — Append-watermark drops late-arriving logs (SYSTEMIC: 77 decode models)
Every `contracts_*_events` model is a thin wrapper over `decode_logs()` with
`incremental_strategy='append'`. **Bug locus:** `macros/decoding/decode_logs.sql`
(~lines 226–244): at render time it runs `SELECT max(block_number), max(block_timestamp)
FROM {{this}}` and embeds `AND block_number > <max>` (+ `block_timestamp >= <max>` for
partition pruning). **No lookback.** A log **backfilled into `execution.logs` for an
already-passed month** has `block_number < max` → excluded **forever**.

- **Why it was invisible:** balances are a cumulative sum → a dropped *inflow* flips them
  negative and screams. Other decoded metrics (Circles mints/trust, pool swaps, CoW
  trades, lending events) are counts/aggregates → they just **silently undercount**.
  Balances were the canary; assume the rest can drift on any late backfill.
- The **same class** hits the inline decode in `int_execution_transfers_whitelisted_daily`
  (`insert_overwrite`, recomputes latest month only) → this dropped ZCHF/svZCHF Feb-2026
  inflows.

### L2 — LATE_START mis-staging
`int_execution_tokens_balances_native_daily`'s `meta.full_refresh` stages (in its
`schema.yml`) had wstETH in a `tokens_2025` stage (`start_date: 2025-01`), but wstETH is
live since **2023-02** (largest holders: Balancer V2 Vault `0xba12222222228d8Ba445958a75a0704d566BF2C8`,
Spark). A 2025-floored backfill made it **worse** (more outflows visible, older inflows
still missing). Fixed by giving wstETH its own `start_date: 2022-06` stage.
- **General risk:** any whitelisted token whose orchestrator `start_date` post-dates its
  real first activity silently produces negative/short balances.

### L3 — Raw `execution.logs` ingestion holes (layer BELOW dbt)
Confirmed a **100-block hole** — blocks 47,089,900–47,089,999 (2026-07-08), **zero logs
for all contracts** — that dropped 48 WxDAI inflows and left residual negatives. Not
fixable by any re-decode; the logs simply weren't in the source. `execution.logs` is
produced by an **external node crawler** (`models/execution/execution_sources.yml`),
outside this repo → needs a raw re-index of the block range.

### L4 — Duplicate-seed drift in the balances append model
~430 WxDAI negatives dating to 2023 came from a duplicate `(date, token, address)` row
(from an append-strategy rebuild) that the seed's `any(balance_raw)` read
non-deterministically → a constant offset carried forward every day. Only a **clean
recreate** clears it — the orchestrator's batch-1 `--full-refresh` does exactly this.

### L5 — Diagnosis pitfalls (these cost hours)
- **Float vs Int256:** summing ~1e20-wei values in `Float64` fabricated a fake "+0.64
  balanced" for an address that was actually short an inflow. **Always reconcile balances
  in exact `Int256`.** Decode a value with
  `reinterpretAsInt256(reverse(unhex(substring(data,1,64))))`. Topics/addresses in
  `execution.logs` are **bare hex, no `0x`**.
- **Block↔date mapping is non-linear** — do not estimate a date from a block delta; join
  `execution.blocks` / read `block_timestamp` directly (a wrong mental map sent the wstETH
  scope 3 years off).
- **Verify against the chain, not the model:** on-chain `balanceOf`=0 while the model said
  −54,875 is what proved the ZCHF drop. This is the entire motivation for §3.
- **Query-surface gotchas** (read MCP): correlated subqueries rejected; `SYSTEM`/DDL
  blocked. **CH memory:** a wide `delete+insert` OOMs (Code 241, ~10.8 GiB shared cap) and
  can wipe a partition mid-mutation — use `insert_overwrite` (atomic REPLACE PARTITION) or
  the orchestrator (recreate + non-overlapping append). **Never** a wide `delete+insert`.

### L6 — Refresh/reprocess levers (which tool, when)
| Need | Tool | Notes |
|---|---|---|
| Recover a **known backfilled month** (L1) | `scripts/refresh/gap_window_refresh.py --months … --select <decode>+` | Drops gap-month partition (lowers the decode watermark) + re-runs with `--vars {start_month,end_month}`; ~1–2h; resumable. |
| Full-history rebuild of a model | `scripts/full_refresh/refresh.py` + `meta.full_refresh` stages | batch-1 `--full-refresh` recreates (clears L4 drift), rest append non-overlapping. OOM-safe. |
| Daily forward catch-up | `scripts/refresh/dbt_incremental_runner.py` (microbatch) | Advances the watermark; **does NOT recover backfills** and can't seed an empty table (OC-2). |
| Force re-decode ignoring watermark | `dbt run --full-refresh -s <decode>` | Sets `flags.FULL_REFRESH`, skips the watermark; re-reads all of `execution.logs`. |

### L7 — Reprocess/backfill duplication, and the unbatched `delete+insert` wipe (FIXED live)
Two linked traps on windowed-incremental models that union many slices — canonical case
`int_revenue_fees_weekly_per_user` (strategy
`('delete+insert' if reprocess_overwrite else ('append' if start_month else 'delete+insert'))`).

- **Append-over-populated-data duplicates.** The `start_month` → `append` path is correct
  **only when the target months are empty / non-overlapping** (the `full_refresh` orchestrator's
  staged backfill guarantees that). Re-running a scoped `--vars {start_month,end_month}` over
  months that **already hold rows** appends a *second full copy* → exact 2× rows. The table is
  `ReplacingMergeTree` but the marts read it **without `FINAL`**, so both copies are live.
  **Symptom seen:** every weekly revenue cohort doubled in one step at `start_month`
  (`api_revenue_gpay_eure_cohorts_weekly`: 197,720→397,306 fees, 6,214→12,492 users at
  2026-03-02), because the cohort mart aggregates with `sum()` / `countIf(... > 0)`
  (**non-distinct**) over the doubled rows. The cohort chain reads the per-stream daily models,
  **not** `int_revenue_fees_unified_daily` — so this is unrelated to canonicalization.
- **Unbatched `delete+insert` reprocess OOMs *and wipes*.** The intended repair is
  `reprocess_overwrite=true` (→ `delete+insert`, the convention in
  `int_execution_tokens_balances_native_daily` / `int_execution_pools_uniswap_v3_daily`). But on
  a model that unions all streams, one whole-window run builds
  `DELETE WHERE (key) IN (SELECT … every slice …)` → OOM (Code 341, `CreatingSetsTransform`).
  **Worse:** the lightweight delete (`UPDATE _row_exists = 0`) **completes in the background
  after dbt has already reported failure** — verified via `system.mutations` (`is_done=1`) — so
  the window is fully deleted while the INSERT never runs → **silent wipe** (2026-03→07 dropped
  to single-digit live rows; partitions left physically bloated with masked rows).

**Fix lever:** reprocess **one `slice` at a time** (the model's built-in `slice` var, format
`stream:SYMBOL`) so each delete-set stays small — the same "batch to avoid OOM" rule as the
balances chain (L4/L6). A 9-slice loop restored **and** de-duplicated the window with every
slice `PASS`, no OOM; result verified `dup_excess = 0` across all streams/months and the cohort
step back to organic (+0.5%/wk). **Gotchas** if tempted to change the reprocess strategy instead:
`insert_overwrite` **rejects `unique_key`** (must be dropped) and REPLACEs *whole* partitions, so
it **cannot** be combined with `slice` (a single-slice run would wipe the co-slices sharing that
month partition); and model `settings={}` takes **storage** settings only — query knobs
(`max_threads`, `max_bytes_before_external_group_by/sort`) go in **`query_settings=`**
(cf. `int_execution_safes_owner_events.sql`). The monthly twin `int_revenue_fees_monthly_per_user`
is `insert_overwrite`-always and stays dup-safe on its own.

### L8 — Global-frontier carry-forward drops thin series (Balancer V3 pool balances) (FIXED live)
`int_execution_pools_balancer_v3_daily` builds daily pool reserves from a calendar spine +
cumulative carry-forward, but its **incremental** branch keyed `current_partition` /
`prev_balances` / `calendar` off a single **global** `max(date) FROM this WHERE date <
yesterday()`. The design assumes every pool emits a row every day: a pool that trades daily
stays pinned to the frontier (dense), but a **thin / sporadically-traded pool that skips a
day falls off the global frontier** — it drops out of `prev_balances`, and the calendar only
ever generates dates from the global frontier (never its own) — so it accretes **permanent
gaps** and only re-materialises a stray day when it happens to trade inside a run's window.

- **Tell-tale:** density (`distinct_dates / span`) tracks trade frequency — old active pools
  ~0.93, new/thin pools 0.10–0.55; the Circles s-gCRC/sDAI pool
  `0x155c95170edc84674d9739669ea005994c40f1a1` was worst at **5/48 days**, and its reserve
  dates didn't even match its trade dates. The Circles reserves mart
  (`api_execution_circles_v2_pools_reserves_token_daily`) surfaced the gaps raw because its
  Balancer V3 branch reads this model **without** re-densifying — unlike the UV3 branch,
  which builds its own daily spine + cumulative reserve.
- **Fix applied:** (1) restore — `dbt run --full-refresh -s int_execution_pools_balancer_v3_daily`
  (the non-incremental branch builds a per-pool dense calendar), then rebuild
  `int_execution_pools_balances_daily` and its 16-table / 3-incremental downstream (28
  downstream views auto-reflect). (2) durable — `current_partition` now anchors the window at
  the **earliest per-(pool,token) frontier** (`min(max(date)) GROUP BY pool_address,
  token_address`), so behind pools are re-densified together with the rest; safe under
  `insert_overwrite` because every touched partition is rebuilt with all pools present. Result:
  0x155c → 48/48, all Circles pools density 1.0.
- **General rule:** any event-driven daily model that forward-fills off a *shared* frontier
  date will silently drop its thinnest series. Anchor carry-forward **per entity**, and give
  the consuming mart its own spine (as the UV3 branch does) so an upstream gap can't reach a
  chart.

### OC-1 — Unpriced wrapper/vault token → `$0` USD (FIXED for OC-sDAI)
OC-sDAI (OpenCover ERC-4626 sDAI vault) had real supply (265,446 shares) but rendered as
`$0` on supply plots. `int_execution_token_prices_daily` derives wrapper prices for Aave/
Spark aTokens (`wrapper_prices` via `lending_market_mapping`) and RWA (`backedfi`), but
had **no ERC-4626 vault-share branch**, so OC-sDAI got no price →
`fct_execution_tokens_metrics_daily` computes `supply_usd = supply * coalesce(price,0) = 0`.
- **Fix applied:** an `ocsdai_price` CTE = `int_yields_ocsdai_share_price_daily.share_price
  × native sDAI price` (priority 2, mirrors `wrapper_prices`). Result: OC-sDAI ≈ $1.244/share,
  supply ≈ $330k. **General rule:** every new wrapper/vault token in `tokens_whitelist` needs
  a price path or it reads `$0` everywhere USD-valued (see also the
  `crc20-price-backfill-on-new-wrapper` note).

### OC-2 — Never-seeded incremental model
`int_revenue_ocsdai_user_balances_daily` sits at **0 rows** even though its exact SELECT
yields 32,943 rows / ~$16M today. Its `INNER JOIN` input (`int_yields_ocsdai_share_price_daily`)
came online **after** the model's table was first created empty; once an `insert_overwrite`/
incremental table is empty, the daily microbatch runner keys off `max(date) FROM this`
(= 1970) and can't sanely seed history, and a forward incremental never reaches back.
- **Fix:** one-time `dbt run --full-refresh -s int_revenue_ocsdai_user_balances_daily`, then
  `dbt run -s int_revenue_sdai_fees_daily+` (currently under-counting sDAI revenue by the
  OC-sDAI look-through).

---

## 2. Detection — ways to check missing data

All are cheap **singular tests** (return offending rows; 0 rows = pass). They are **not**
auto-run today: `run_dbt_observability.sh`'s `build_test_batches` only selects
`path:models/…`, so standalone `tests/*.sql` are skipped. **Wire them** by tagging
`data_quality` and adding a step: `run_step "dbt-test:data-quality" dbt test --select tag:data_quality`.

### 2a. Non-negative balance (symptom)
```sql
{{ config(severity='warn', tags=['production','data_quality','balances']) }}
-- A non-rebasing ERC-20 holder can't be negative on-chain; a negative balance_raw for a
-- REAL holder (not the 0x00..00 mint/burn sink) = a dropped inflow (L1/L3). -0.001 floor
-- skips rounding noise. WARN, not error: a transient residual can be a raw-layer gap.
SELECT symbol, address, date, balance_raw, balance
FROM {{ ref('int_execution_tokens_balances_native_daily') }}
WHERE date >= today() - 3
  AND address != '0x0000000000000000000000000000000000000000'
  AND balance < -0.001
ORDER BY balance ASC
```

### 2b. Raw `execution.logs` block-continuity (root cause: L3)
```sql
{{ config(severity='warn', tags=['production','data_quality','source_freshness']) }}
-- A run of consecutive block_numbers with ZERO logs (all contracts) between two present
-- blocks = a raw indexer skip. On a live chain a >5-block zero-log span is not "quiet
-- blocks" (confirmed 2026-07: 47,089,900-47,089,999 dropped 48 WxDAI inflows). Confirm a
-- flagged range on-chain (eth_getLogs) before re-indexing.
WITH recent_blocks AS (
    SELECT DISTINCT block_number FROM {{ source('execution', 'logs') }}
    WHERE block_number >= (SELECT max(block_number) - 1200000 FROM {{ source('execution', 'logs') }})
),
gaps AS (
    SELECT block_number AS b,
           leadInFrame(block_number) OVER (ORDER BY block_number ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS nb
    FROM recent_blocks
)
SELECT b + 1 AS gap_start_block, nb - 1 AS gap_end_block, nb - b - 1 AS missing_blocks
FROM gaps WHERE nb - b > 5 ORDER BY missing_blocks DESC
```

### 2c. Raw-vs-decoded parity (root cause: L1) — NEW
Per contract, per month: count in `execution.logs` (filtered to the contract) vs count in
`contracts_<x>_events`; a nonzero deficit = dropped decoded logs. Generalize the ad-hoc
month-gap query used in the investigation; loop over the high-value contracts (WxDAI, the
LSD tokens) using the `UNION ALL … HAVING` shape from `tests/contracts_live_tables_freshness.sql`.
This is the earliest, cheapest signal for L1 before it reaches balances.

### 2d. LATE_START guard (L2) — NEW
Flag any whitelisted token whose transfers `min(date)` is materially later than its
`tokens_whitelist.date_start` (mis-staged / never-backfilled history).
```sql
{{ config(severity='warn', tags=['production','data_quality']) }}
SELECT w.symbol, toString(w.date_start) AS whitelist_start, toString(t.first_seen) AS model_first_seen
FROM {{ ref('tokens_whitelist') }} w
INNER JOIN (
    SELECT symbol, min(date) AS first_seen
    FROM {{ ref('int_execution_transfers_whitelisted_daily') }} GROUP BY symbol
) t ON t.symbol = w.symbol
WHERE t.first_seen > addMonths(toDate(w.date_start), 1)
```

### 2e. Unpriced-token guard (OC-1) — NEW
Flag any whitelisted token with non-zero native supply but absent/zero USD price for a
recent date — catches new wrapper/vault tokens before they render `$0`.

### 2f. Source-freshness cleanup
The intentionally-stopped `execution.balance_diffs / code_diffs / nonce_diffs / storage_diffs`
inherit the source-level `freshness` in `models/execution/execution_sources.yml` and will
error forever. Add `freshness: null` to each of the four table entries (identical to how
`indexing_state`/`migrations` already opt out) so **real** stalls (e.g. Envio ~6 days,
Dune ~1.5 days) stand out instead of drowning in an intentional shutdown.

---

## 3. On-chain balance reconciliation (ALL whitelisted-token holders, daily)

The durable "ground truth": each day snapshot on-chain `balanceOf` for every non-zero
holder of every whitelisted token, land it in ClickHouse, and reconcile against
`int_execution_tokens_balances_native_daily`. Would have caught L1–L4 automatically.

**The end-to-end pattern already exists in-repo** (Circles avatar-metadata backfill) — mirror it:

1. **Landing table** `dbt.onchain_token_balances_snapshot` — provision with a one-shot macro
   (mirror `macros/circles/create_circles_avatar_metadata_table.sql`):
   `ENGINE=ReplacingMergeTree(fetched_at) ORDER BY (snapshot_date, token_address, address)`;
   columns `snapshot_date Date, block_number UInt64, token_address String, symbol String,
   address String, balance_raw String /* Int256 as text */, fetched_at DateTime`.
2. **Targets/queue view** `int_execution_tokens_onchain_recon_targets` — universe = latest-day
   non-zero holders from `int_execution_tokens_balances_native_daily` × `tokens_whitelist`
   (token, holder, decimals). Mirror `int_execution_circles_v2_avatar_metadata_targets`.
3. **Ingester** `scripts/onchain/snapshot_token_balances.py` — mirror
   `scripts/data_checker/compare_aave_scaled_balances.py` (threaded batched `eth_call`,
   `clickhouse-connect`, `load_dotenv`, dedup, block-per-day resolution) + the recursive
   rate-limit auto-split from `scripts/export_gpay_gno_balances.py`. For "all holders" scale:
   - **Multicall3 `aggregate3`** (batch ~500–1000 `balanceOf` per `eth_call`) — the existing
     scripts do 1 call/address, which won't scale. Multicall + `ThreadPoolExecutor` keeps it
     to a few-thousand HTTP requests → feasible in the daily window.
   - Snapshot at the **last block of the prior day** (align to the daily model grain), via
     `execution.blocks`; needs `GNOSIS_ARCHIVE_RPC_URL`.
   - **Token caveats (`tokens_whitelist`):** `xDAI = 0xeeee…eeee` is native → use
     `eth_getBalance`, not `balanceOf`; honor per-token `decimals`; use `date_start/date_end`
     for EURe/GBPe historical↔current address pairs.
   - Optional dust threshold per token (documented config, not silent) to bound cost.
4. **Source declaration** — add to `models/execution/auxiliary_sources.yml` (source `auxiliary`,
   schema `dbt`, `loaded_at_field: fetched_at`, freshness). Staging view mirrors
   `models/crawlers_data/staging/stg_crawlers_data__dune_prices.sql`.
5. **Reconciliation model + test** — `int_execution_tokens_balances_onchain_recon_daily`
   (materialized) joins model vs snapshot on `(snapshot_date, token_address, address)`,
   computes `diff_wei`; a mart/api for a dashboard; a singular test flagging
   `abs(diff_wei) > tolerance`. Mirror `tests/account_portfolio_holdings_balance_consistency.sql`
   (two-source join + flag) + the `ABS(diff) > 1e-6` idiom from
   `tests/consensus_income_balance_reconciliation.sql`. Materialized precedent:
   `int_execution_gnosis_app_gt_user_reconciliation.sql`.
6. **Schedule** — add two `run_step` lines to `scripts/run_dbt_observability.sh` step 1b
   (~lines 159–182): rebuild the targets view, then run the ingester. The external k8s
   CronJob (`cron.sh`) already runs this daily.

**Rollout:** stage on **one token** first (e.g. GNO — small holder set): ingest → recon
model near-zero diffs → spot-check 3 addresses vs `balanceOf`. Then scale to all tokens and
confirm the daily window is acceptable.

---

## 4. Durable decode-layer fix (no daily lookback)

**Decision:** keep `append`, **no daily lookback** — a 90-day lookback would reprocess months
of all-token data every run (rejected). Verified dup-safety: a lookback is dup-free **only**
on `insert_overwrite` models (REPLACE PARTITION); on `append` decode models it duplicates
until a background merge, and the transfers model reads the decode **without `FINAL`** →
double-count. So the posture is **detect-then-reprocess**:

- **Recommended:** a **weekly reconciliation sweep** — run §2 parity/continuity checks to list
  gap months, then `gap_window_refresh.py` recovers exactly those months. Cheap, targeted, no
  daily cost, no dup risk.
- **Long-term option (bigger):** convert the 77 decode models to `insert_overwrite`
  (partition by month) → a month-granular lookback becomes dup-safe and the drop bug
  disappears. One-time full-refresh per model; evaluate separately.

---

## 5. Suggested issue backlog (order)

1. **Source-freshness cleanup** (4 × `freshness: null`) + **wire the §2 tests** into
   `run_dbt_observability.sh`. Tiny, high-value.
2. **Raw-vs-decoded parity** test (2c) + **LATE_START guard** (2d) + **unpriced-token guard** (2e).
3. **OC-sDAI revenue reseed** (OC-2): `dbt run --full-refresh -s int_revenue_ocsdai_user_balances_daily`
   then `int_revenue_sdai_fees_daily+`. (The OC-sDAI supply price gap, OC-1, is already fixed.)
4. **Weekly reconciliation-sweep runbook** (§4 recommended).
5. **On-chain reconciliation** (§3) — the largest: land table → targets view → ingester
   (one token) → recon model/test → scale → schedule.

---

## Appendix — diagnosis playbook (queries that worked)

- **Exact per-address net from raw logs** (Int256, bare-hex topics, no `0x`):
  `sumIf(reinterpretAsInt256(reverse(unhex(substring(data,1,64)))), topic2=<padded_addr>) - sumIf(…, topic1=<padded_addr>)`
  filtered to the token contract, bounded by `block_number` for speed.
- **On-chain ground truth:** `balanceOf(addr)` (ERC-20) or `eth_getBalance` (native xDAI); on-chain
  `eth_getLogs` scan to confirm a raw hole is real (chain has logs, `execution.logs` doesn't).
- **Decode gap by month:** `execution.logs` count vs `contracts_<x>_events` count grouped by
  `toStartOfMonth(block_timestamp)` → nonzero deficit = dropped logs.
- **Block ranges:** never estimate from block deltas — read `block_timestamp` from
  `execution.logs`/`execution.blocks`.
