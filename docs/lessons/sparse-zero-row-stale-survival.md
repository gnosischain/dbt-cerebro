---
id: sparse-zero-row-stale-survival
title: A sparse table that drops zero rows can never overwrite a stale key with "zero"
status: enforced
scope: >-
  incremental models that (a) filter out zero/empty state rows to stay sparse and
  (b) use delete+insert whose delete-set derives from the new rows (canonical:
  int_execution_tokens_balances_native_daily WHERE balance_raw != 0); any
  tombstone-less sparse state table
symptom: >-
  a handful of rows survive every reprocess unchanged — always keys whose
  CORRECTED value is exactly zero (e.g. addresses that spent their full balance);
  rebuilds fix everything else and reliably skip these
last_verified: 2026-07-19
evidence:
  - '2026-07-17: all 11 residual negative balances that survived TWO clean July reprocesses classified as this bug — every one had corrected cumulative balance exactly 0 (windowed diffs cum = 0, e.g. ZCHF EOA 0x9a76 spent 10,010 to zero; on-chain balanceOf confirmed ~0/75) while the stale negative row persisted'
  - 'mechanism: the final WHERE balance_raw != 0 keeps the table sparse, but delete+insert deletes keys IN (SELECT ... FROM new data) — a key whose corrected state is "no row" is absent from new data, so its stale row is never deleted. The daily frontier-recompute path has the same hole for spend-to-zero addresses'
  - 'fix: emit zero rows for keys active in the run window (bounded tombstones; they drop out on later days) — int_execution_tokens_balances_native_daily.sql final CTE, is_incremental branch'
  - '2026-07-17 cleanup: the tombstone-fixed window reprocess kept losing to OvercommitTracker (Code 341 on the delete-set even per-symbol-group under afternoon load), so the 11 stale rows were removed surgically (bounded 11-key ALTER DELETE on native + USD layers, mutation completion verified) — valid here because corrected-state-0 means "no row" IS the correct sparse state; negatives 11 -> 0. The model fix stays for future runs'
  - '2026-07-17 scale + status correction (second-agent audit via the MCP knowledge tools): the stale-POSITIVE class is far larger than the 11 negatives — accounting-identity residuals (sum of balance_raw over all addresses incl. 0x0 must be 0) on 16 tokens starting exactly 2026-07-15: EURe +24,514.81, svZCHF +8,928.54, sDAI +4,961.30, GNO +3,115.80 (78/78 spend-to-zero addresses stale, chain-verified: model 1,434,027.75 vs totalSupply() 1,430,920.92), WxDAI, SAFE, COW, ZCHF, BRZ, WETH, wstETH, bCSPX, USDC.e, USDC, USDT, WBTC'
  - 'while the fix was working-tree-only it read as "inert" — the production cron is a k8s CronJob running the CI-built image from merged main, so days built before deploy re-armed the bug nightly. STATUS DESCRIBES THE DEPLOYED STATE, not the working tree'
  - 'detection now permanent: tests/data_quality/dq_daily_balance_conservation.sql (the identity check; catches stale positives the negative test cannot see) — deployed in the same image'
  - 'fix VERIFIED locally 2026-07-17 15:41 (GNO-only July reprocess with the tombstone branch): 9,252 tombstone rows emitted, 78/78 stale spend-to-zero addresses cleared, conservation exactly 0 every July day, model total 1,430,920.92 == on-chain totalSupply() to the cent'
  - 'DEPLOYED 2026-07-18: the tombstone fix + conservation test + whitelist seed correction landed in commit a96374d3 (committed 2026-07-18 17:46) and shipped in the production image b930150 (2026-07-18 17:49). Verified in git: a96374d3 is an ancestor of b930150; the deployed model SQL carries the tombstone rule. Status observed -> remediated accordingly'
  - '2026-07-19 forward-fix HOLDING, backlog PERSISTS (as designed): conservation residuals per day — 07-15 15 symbols, 07-16 15, 07-17 19 (worst 7.3e24), 07-18 19 (IDENTICAL to 07-17). The 15->19 / 300x jump is between 07-16 and 07-17, both PRE-deploy; 07-18 (first post-deploy day) added NO new stale rows, only carried 07-17 forward. So the forward-only fix prevents new spend-to-zero staleness but does not retro-clean the pre-deploy backlog — the cumulative chain carries it forward every day until the one-time re-clean reprocesses those token-months'
  - '2026-07-19 RE-CLEAN COMPLETE: per-symbol July reprocess (reprocess_overwrite=true) of the native model for all 19 affected tokens (BRZ..USDC, smallest->largest, mutation-guarded) -> native conservation 0 on every July day. Cascaded: balances_daily (USD) drop+append, then by_sector/cohorts/supply_holders drop+reprocess, then the tokens fct tables rebuilt. dq_daily_balance_conservation now PASSES (was WARN on 49 rows); full data_quality_daily suite 8 PASS/0 WARN; GNO supply 07-16 back to 1,430,920.92 == on-chain totalSupply(). Status remediated -> enforced: prevention deployed + detection deployed & green + backlog cleared'
  - 'CAUTION recorded during the re-clean: a Code 241 (server-saturation victim) that KILLS an append mid-insert can leave PARTIAL rows; a naive retry that appends on top produces identical RMT duplicates (SharedReplacingMergeTree) that FINAL-less downstreams double-count (9/18 July days hit this on balances_daily). Safe protocol: after a killed append, DROP the partition before retrying (never append-on-top), and use OPTIMIZE ... PARTITION .. FINAL as a dedup barrier before reading downstream. See refill-append-aggregator-inflation'
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
ENFORCED (2026-07-19). Three layers, all in place:
1. PREVENTION — the tombstone fix (zero-row retention for window-active keys on
   incremental branches) is live on the production cron since image b930150
   (commit a96374d3, 2026-07-18). Demonstrated forward: 07-18 (first post-deploy
   day) added no new stale rows.
2. DETECTION — dq_daily_balance_conservation (tag data_quality_daily) ships in the
   same image and runs every cron; it PASSES post-reclean (was WARN on 49 rows).
3. BACKLOG CLEARED — the pre-deploy stale-positive rows (19 tokens, residuals
   2026-07-15..18, carried forward by the cumulative chain) were reprocessed
   2026-07-19: per-symbol native July reprocess (reprocess_overwrite=true) +
   downstream cascade (USD balances_daily, by_sector, cohorts, supply_holders,
   tokens fct tables). Native conservation 0 on every July day; full
   data_quality_daily suite 8 PASS/0 WARN.

Re-clean protocol (for the next occurrence): reprocess per-symbol smallest->largest,
mutation-guarded (kill only FAILED mutations); a Code 241 that kills an append
mid-insert can leave partial rows — DROP the partition before retrying, never
append-on-top, and OPTIMIZE ... FINAL the partition as a dedup barrier before
reading downstream (SharedReplacingMergeTree dups are invisible to row counts and
double-counted by FINAL-less readers). Other sparse delete+insert models should be
audited for the same compose (grep: "!= 0" filters in incremental models with
unique_key delete-sets).
