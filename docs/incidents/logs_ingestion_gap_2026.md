# Incident: `execution.logs` ingestion gaps (May 30 & June 14, 2026)

**Status:** RESOLVED — all three instances of this class (2026-05-30, 2026-06-14,
2026-07-08) are raw-backfilled, decode-reprocessed and verified as of 2026-07-18,
including cumulative carry-forward. See Resolution (May/June) and the addendum
(July).
**Layer:** raw chain ingestion (`execution.logs`) — **not** a dbt model bug, **not** the June `insert_overwrite` wipe
**Surfaced by:** a missing Circles v2 `Trust` event in `int_execution_circles_v2_trust_updates`
(tx `0xe727ad599ae1b82d6ac4d6693602347a9a4afc134870cb015ee6a59fa4a38165`)

---

## TL;DR

The raw `execution.logs` table is **missing every log for 165 blocks** across **two short
ingestion outages** — **11,550 logs total**. The corresponding **transactions were ingested fine**
(`execution.transactions` has them), so this is a failure in the **logs** loader only, not the
transactions loader, and not anything in dbt.

| Window (UTC) | Block range | Blocks | Missing logs | Txs in those blocks | Duration |
|---|---|---|---|---|---|
| **2026-05-30 04:32:20 – 04:37:50** | 46434334 – 46434399 | 65 | 5,017 | 916 | ~5.5 min |
| **2026-06-14 08:20:10 – 08:28:35** | 46689500 – 46689599 | 100 (contiguous) | 6,533 | 2,257 | ~8.5 min |
| **Total** | | **165** | **11,550** | **3,173** | |

Every block in these windows is **chain-verified**: `eth_getLogs` returns logs on-chain, but
`execution.logs` has zero rows for them.

Full per-block list: [`logs_ingestion_gap_CONFIRMED.csv`](logs_ingestion_gap_CONFIRMED.csv)
(`block_number, block_timestamp_utc, tx_count, onchain_log_count`).

---

## How it was found

The user reported that tx `0xe727…a38165` has a Circles v2 `Trust` event but is absent from
`int_execution_circles_v2_trust_updates`. Tracing the lineage downward:

1. **Model** `int_execution_circles_v2_trust_updates` — 0 rows for the tx. ✅ correct (its source is empty).
2. **Decode** `contracts_circles_v2_Hub_events` (decodes Hub `0xc12C…13e8` from `execution.logs`,
   `event_name='Trust'`) — 0 rows for the tx.
3. **Raw** `execution.logs` — **0 rows** for the tx. But `execution.transactions` **has** the tx
   (block has 16 txs). The decode watermark is current (block ~46.77M, June 19), so the decode is
   not lagging.
4. **On-chain** (`eth_getTransactionReceipt`, direct JSON-RPC) — the tx is real: **block 46,689,583,
   status success, 4 logs, of which 2 are Hub `Trust` events** (topic0 `0xe60c754dd8ab…`, logIndex 24
   & 25).

Conclusion: the data is missing **one layer below dbt** — the raw `execution.logs` loader never
ingested that block's logs. Every dbt layer is correct given its (incomplete) source.

> **Gotcha for anyone reproducing this:** tx hashes are stored in the DB **without** the `0x`
> prefix. Queries must compare `lower(replaceAll(transaction_hash,'0x',''))` and should keep a
> `block_timestamp`/`block_number` predicate so the query stays partition-pruned (otherwise it
> full-scans `execution.logs`).

---

## Scope & validation method

- **Candidate set:** blocks since 2026-05-01 with ≥2 transactions but **zero** rows in
  `execution.logs` → 2,933 blocks
  ([`logs_ingestion_gap_blocks.csv`](logs_ingestion_gap_blocks.csv)).
- **The candidate heuristic is noisy:** most candidates are *legitimately* log-free (Gnosis has
  heavy native-transfer bot traffic; e.g. a 124-tx block that emits 0 logs on-chain). So the
  candidate list must **not** be used directly.
- **Confirmation:** each candidate was checked against the chain with `eth_getLogs` per block. Only
  blocks where the **chain returns logs but the DB has none** are real gaps → **165 blocks**, all
  inside the two windows above. Everything else outside those windows was a false positive.

The two windows are short, near-contiguous block ranges — classic symptoms of the logs ETL stalling
for a few minutes and skipping a window while transactions continued to load.

---

## Impact on dbt models

Anything that reads `execution.logs` is missing data **for those two time windows only**:

- **All `contracts_*` decode models** (Hub events/calls, ERC20, pools, bridges, safes, etc.) — any
  event emitted in the two windows is absent.
- **Everything downstream** of those decodes: Circles (trusts, transfers, mints, balances), gpay,
  gnosis_app swaps, pools/DEX trades, token transfers, bridges, etc. — missing those windows' events.
- **Transactions-based models are NOT affected** (the transactions loaded correctly).

For most daily aggregates a ~5–9 minute hole is a small dent; for individual records (like the
reported `Trust` event) it is a real missing row.

---

## Why it will not self-heal

The decode models (`contracts_*`) are `incremental_strategy='append'` with a `block_timestamp`
watermark that is already **well past** May 30 / June 14. Backfilling logs into `execution.logs`
alone will **not** be picked up — the append decode only scans `block_timestamp > max(seen)`.

---

## Remediation plan

1. **Re-ingest the raw logs** into `execution.logs` for the two block ranges (raw pipeline /
   `eth_getLogs` backfill):
   - `46434334 – 46434399` (May 30)
   - `46689500 – 46689599` (June 14)
   Use [`logs_ingestion_gap_CONFIRMED.csv`](logs_ingestion_gap_CONFIRMED.csv) as the authoritative
   block list (it includes the on-chain log count expected per block).
2. **Reprocess the decode layer** for the affected partitions (decodes partition by
   `toStartOfMonth(block_timestamp)` → the **2026-05** and **2026-06** partitions). Because append
   won't re-scan, either re-run the affected `contracts_*` models for those months
   (e.g. via `scripts/full_refresh/refresh.py --select contracts_* --start-month 2026-05 --end-month 2026-06`)
   or a targeted reprocess of the two block ranges.
3. **Rebuild downstream** intermediates + marts for May/June so the recovered events flow through
   (Circles trusts/transfers/mints/balances, gpay, pools, tokens, bridges, …).
4. **Verify**: re-check the reported tx and a sample of blocks from each window — e.g.
   `int_execution_circles_v2_trust_updates` should contain tx `e727…a38165` (2 Trust rows,
   logIndex 24 & 25) after step 3.

> Prerequisite: this is a **raw-ingestion** fix and must happen in the chain-ingestion pipeline
> (outside this dbt repo); steps 2–4 are the dbt-side follow-up.

---

## Related finding — MCP RPC tool bug (fixed)

While confirming the on-chain receipt, the cerebro-mcp tools `contract_decode_receipt_logs` and
`contract_decode_transaction_input` failed with `'str' object has no attribute 'get'`.

**Cause:** `GnosisRpcManager.retry` (`cerebro-mcp/src/cerebro_mcp/clients/web3.py`) wrapped every RPC
result in `normalize_value(...)`. web3.py returns an `AttributeDict` (a `Mapping`, **not** a `dict`),
which misses `normalize_value`'s `isinstance(value, dict)` branch and falls through to
`return str(value)` — stringifying the whole receipt, so `receipt.get("logs")` /
`process_receipt(receipt)` broke.

**Fix:** `retry` now returns the **raw** web3 result; JSON-normalization stays at each tool's output
boundary (the call path already did `normalize_value(result)`). Verified: `eth_getTransactionReceipt`
via the manager now returns an `AttributeDict` with working `.get("logs")` (4 logs, 2 Hub Trust events
for the reported tx).

---

## Resolution (verified 2026-07-18)

All four remediation steps completed for the two windows above. Warehouse-verified:

| Check | Expected | Found |
|---|---|---|
| `execution.logs` May 30 window (46434334–46434399) | ~5,017 logs / 65 blocks | **5,020 logs / 66 blocks** |
| `execution.logs` June 14 window (46689500–46689599) | 6,533 logs / 100 blocks | **6,533 logs / 100 blocks** (exact) |
| `contracts_circles_v2_Hub_events` for tx `e727…a38165` | 2 Trust events | **2** (decode reprocessed past the old watermark) |
| `contracts_circles_v2_Hub_events` inside the May window | > 0 | **53 events** (May partition reprocessed too) |
| `int_execution_circles_v2_trust_updates` for tx `e727…a38165` | 2 rows | **2 rows** — the originally reported symptom is fixed |

Detection for this class is now automated:
`tests/data_quality/dq_daily_raw_logs_block_continuity.sql` (block-continuity scan on
the observability schedule). Class record: `docs/lessons/raw-logs-ingestion-holes.md`.

**Cumulative carry-forward: VERIFIED 2026-07-18.** The table above proves the raw +
decode + event-log chain; cumulative downstreams (`{{ this }}` readers) needed a
separate check, since they integrate state forward and a pre-recovery frozen day
would keep an offset forever (frontier-day-incomplete-inputs). Method: on
swaps-only Balancer V2 pools (pools whose only Vault events that day are Swaps, so
reserve delta MUST equal decoded swap net exactly), compare
`int_execution_pools_balancer_v2_daily` day-over-day `reserve_amount_raw` deltas
against the decoded full-day net.

- 2026-05-30: **7/7 pool/token pairs match to the raw unit** across 3 pools.
- 2026-06-14: top-volume pool matches (|diff| = 7e5 on 5.4e19 — float dust).

So the May/June recovery did rebuild the cumulative layer; no residual offsets and
no cumulative extension back to May is required. (Also ruled out en route: the
May-partition writes visible in `system.parts` on 2026-07-18 were `MutatePart`
rewrites from the daily models' lightweight-delete mutations, not a concurrent
recovery; and `contracts_BalancerV2_Pool_events` showing raw≠decoded in the gap
windows is its normal partial-topic coverage, confirmed against a healthy control
window.)

## Addendum — third instance, 2026-07-08 (RESOLVED 2026-07-18)

A later hole of the same class: blocks **47,089,900–47,089,999** (~8 min on
2026-07-08; first analyzed in docs/data-quality-learnings-and-remediation.md §L3).

**Recovered 2026-07-18** via `gap_window_refresh.py --months 2026-07-01` over the
15 affected decode families + downstream (96 models, topo order, 0 failures,
~23 min). Verification below.

Pre-recovery state (for the record):

- **Raw: backfilled.** `execution.logs` has 16,895 logs across all 100 blocks.
- **Decode: NOT recovered.** The raw backfill landed below the append watermark
  (decode-watermark-late-logs), so e.g. the Circles Hub decode had **0** events in
  the window while raw held **60** Hub logs. All `contracts_*` families were in the
  same state; decode-based downstreams were missing this window's events.

### Affected decode families (enumerated from the window's raw addresses)

21 families had logs in the window; 5 `_live` rolling variants excluded (they
self-heal), 2 were already consistent (`contracts_wxdai_events` — the tokens chain
had already reprocessed July; `NameRegistry`). The 15 recovered, by raw log count:
sdai 1244, ConditionalTokens 714, BalancerV2_Vault 705, BalancerV2_Pool 354,
UniswapV3_Pool 248, aaveV3_PoolInstance 182, Swapr_v3_AlgebraPool 159,
BalancerV3_Vault 94, circles_v2_Hub 60, CowProtocol_GPv2Settlement 18, spark_Pool 8,
Realitio_v2_1 5, circles_v2_score_policy 5, Curve3PoolLP 3,
FPMMDeterministicFactory 2.

### Verification (2026-07-18, post-recovery)

| Check | Result |
|---|---|
| decoded == raw in window, per family | **16/16 exact** (BalancerV2_Pool excluded: registry family decodes only pool-admin topics — 57 `SwapFeePercentageChanged` in all of July vs 126,944 raw registry-address logs, i.e. 0-in-window is its normal coverage, confirmed on a healthy control window) |
| sort-key duplicate scan, touched July partitions | **0 duplicate key groups** across 8 event tables + 4 daily tables (keys read from `system.tables.sorting_key`) |
| boundary day-series 07-06→07-10 | smooth on all checked marts (e.g. balances_daily 240,216 → 243,680 monotonic; pools 2,420 → 2,434) — no step, no doubling |
| cumulative offset check, 2026-07-08 | **9/9 pool/token pairs MATCH**: `int_execution_pools_balancer_v2_daily` d-o-d reserve delta == decoded full-day swap net, to the raw unit (swaps-only pools, so the identity must hold exactly) |
| dq suite (`tag:data_quality_daily`) | 7 PASS, 1 WARN — the WARN is the **unrelated** pre-deploy `sparse-zero-row-stale-survival` class (dates 07-15/16/17 only; gap day 07-08 and 07-14/07-18 have zero residuals) |

The tokens chain needed no recovery: `int_execution_tokens_transfers_daily` for
WxDAI on 2026-07-08 shows 86,089 transfers against 84,235 raw Transfer logs (model
counts mints/burns too), i.e. the 2026-07-17 July reprocess already included the
backfill. Its four heavy models were `--skip`ped from the run for that reason.
- **Recovery lever used**: `scripts/refresh/gap_window_refresh.py --select <decode>+
  --exclude '*_live+' --months 2026-07-01` — drops the gap-month partition to lower
  the watermark, then re-runs scoped (never the daily runner). **`--months` must span
  gap month through the CURRENT month** if any month boundary has been crossed since
  the gap: cumulative downstreams are wrong from the gap day FORWARD, and a
  gap-month-only rebuild leaves later frozen days on the pre-recovery state
  (backfill-order-cumulative, gap-recovery corollary). Here the gap month WAS the
  current month, so one month sufficed. Blast radius measured 2026-07-18 from two
  decode families alone (`contracts_BalancerV2_Vault_events` +
  `contracts_circles_v2_Hub_events`): 662 transitive downstream models, 18 of them
  cumulative. Enumerate the full decode set from the window's raw addresses before
  selecting. Run in a quiet window (no lock against the 30-min cron; the microbatch
  runner's missing-slice refusal is a backstop, not a lock) — decode writes were
  observed to cluster in the evening cron burst only.
- Note: models that read `execution.logs` directly with month windows (the tokens
  balance chain) were reprocessed for all of July on 2026-07-17 — they picked the
  backfill up IF it had landed by then (backfill landing time not established;
  verify the window's token flows before relying on them). The `contracts_*`
  decode chains and their downstreams are definitively still affected.

## Data files

| File | Contents |
|---|---|
| [`logs_ingestion_gap_CONFIRMED.csv`](logs_ingestion_gap_CONFIRMED.csv) | **165 chain-verified** missing-logs blocks (the actionable backfill list) |
| [`logs_ingestion_gap_blocks.csv`](logs_ingestion_gap_blocks.csv) | 2,933 raw candidates (≥2 tx, 0 DB logs) — noisy, kept for transparency only |
