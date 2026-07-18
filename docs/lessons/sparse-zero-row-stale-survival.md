---
id: sparse-zero-row-stale-survival
title: A sparse table that drops zero rows can never overwrite a stale key with "zero"
status: observed
scope: >-
  incremental models that (a) filter out zero/empty state rows to stay sparse and
  (b) use delete+insert whose delete-set derives from the new rows (canonical:
  int_execution_tokens_balances_native_daily WHERE balance_raw != 0); any
  tombstone-less sparse state table
symptom: >-
  a handful of rows survive every reprocess unchanged — always keys whose
  CORRECTED value is exactly zero (e.g. addresses that spent their full balance);
  rebuilds fix everything else and reliably skip these
last_verified: 2026-07-17
evidence:
  - '2026-07-17: all 11 residual negative balances that survived TWO clean July reprocesses classified as this bug — every one had corrected cumulative balance exactly 0 (windowed diffs cum = 0, e.g. ZCHF EOA 0x9a76 spent 10,010 to zero; on-chain balanceOf confirmed ~0/75) while the stale negative row persisted'
  - 'mechanism: the final WHERE balance_raw != 0 keeps the table sparse, but delete+insert deletes keys IN (SELECT ... FROM new data) — a key whose corrected state is "no row" is absent from new data, so its stale row is never deleted. The daily frontier-recompute path has the same hole for spend-to-zero addresses'
  - 'fix: emit zero rows for keys active in the run window (bounded tombstones; they drop out on later days) — int_execution_tokens_balances_native_daily.sql final CTE, is_incremental branch'
  - '2026-07-17 cleanup: the tombstone-fixed window reprocess kept losing to OvercommitTracker (Code 341 on the delete-set even per-symbol-group under afternoon load), so the 11 stale rows were removed surgically (bounded 11-key ALTER DELETE on native + USD layers, mutation completion verified) — valid here because corrected-state-0 means "no row" IS the correct sparse state; negatives 11 -> 0. The model fix stays for future runs'
  - '2026-07-17 scale + status correction (second-agent audit via the MCP knowledge tools): the stale-POSITIVE class is far larger than the 11 negatives — accounting-identity residuals (sum of balance_raw over all addresses incl. 0x0 must be 0) on 16 tokens starting exactly 2026-07-15: EURe +24,514.81, svZCHF +8,928.54, sDAI +4,961.30, GNO +3,115.80 (78/78 spend-to-zero addresses stale, chain-verified: model 1,434,027.75 vs totalSupply() 1,430,920.92), WxDAI, SAFE, COW, ZCHF, BRZ, WETH, wstETH, bCSPX, USDC.e, USDC, USDT, WBTC'
  - 'why the fix read as "inert": it exists only in the UNCOMMITTED working tree — the production daily cron is a k8s CronJob running the CI-built image from merged main, so it re-arms the bug nightly; the sole local run containing the fix died pre-insert at a delete-mutation OOM. Status downgraded remediated -> observed accordingly: STATUS DESCRIBES THE DEPLOYED STATE, not the working tree'
  - 'detection now permanent: tests/data_quality/dq_daily_balance_conservation.sql (the identity check; catches stale positives the negative test cannot see)'
  - 'fix VERIFIED locally 2026-07-17 15:41 (GNO-only July reprocess with the tombstone branch): 9,252 tombstone rows emitted, 78/78 stale spend-to-zero addresses cleared, conservation exactly 0 every July day, model total 1,430,920.92 == on-chain totalSupply() to the cent. Correct and pending deploy only'
---

## Symptom
A reprocess that demonstrably works (fixes hundreds of rows) leaves a stubborn few
untouched, run after run. The stuck keys share one property: their correct current
value is zero/none.

## Root cause
Two individually-reasonable choices compose into a trap:
1. Sparse storage: rows with zero state are filtered out to keep the table small.
2. delete+insert scoped to the new batch's keys: only keys present in the new rows
   get their old rows deleted.
A correction whose result is "this key now has NO row" produces nothing to insert —
so nothing gets deleted, and the stale row (a negative balance, an outdated state)
survives every rebuild. Worse, the cumulative chain seeds the next day from the
stale row, propagating it forward.

## Forbidden action
Don't "fix" the stuck rows with a manual ALTER DELETE and move on — the next
spend-to-zero recreates the class. Don't widen the delete-set to the whole
partition either (that's insert_overwrite semantics; use that strategy explicitly
if you want it).

## Detection
Rows that survive N reprocesses unchanged; classify by recomputing the corrected
value from the source — corrected == 0 with a persistent nonzero row is this bug.
The negative-balance data-quality test surfaces the visible subset (negatives);
spend-to-zero keys stuck at a stale POSITIVE value are invisible to it.

## Safe remediation
Emit tombstone/zero rows on incremental runs for keys with activity in the window
(bounded: only that window's spent-to-zero keys; they naturally drop out of later
days). The zero row enters the insert batch, lands in the delete-set, and
overwrites the stale key via the RMT latest-version semantics downstream too.

## Ground truth
On-chain balanceOf for the stuck address.

## Enforcement
NOT YET DEPLOYED. The tombstone fix (zero-row retention for window-active keys on
incremental branches) sits in the working tree of
int_execution_tokens_balances_native_daily; production runs the pre-fix image and
re-creates stale rows nightly. Ladder: `remediated` when the fix is merged and the
CI image is live on the cron; `enforced` when dq_daily_balance_conservation is green
in a production run and the 16-token July re-clean has landed (re-clean BEFORE deploy
would be undone the next night). Other sparse delete+insert models should be audited
for the same compose (grep: "!= 0" filters in incremental models with unique_key
delete-sets).
