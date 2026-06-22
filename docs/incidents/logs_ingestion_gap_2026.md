# Incident: `execution.logs` ingestion gaps (May 30 & June 14, 2026)

**Status:** root cause confirmed, raw-data remediation pending
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

## Data files

| File | Contents |
|---|---|
| [`logs_ingestion_gap_CONFIRMED.csv`](logs_ingestion_gap_CONFIRMED.csv) | **165 chain-verified** missing-logs blocks (the actionable backfill list) |
| [`logs_ingestion_gap_blocks.csv`](logs_ingestion_gap_blocks.csv) | 2,933 raw candidates (≥2 tx, 0 DB logs) — noisy, kept for transparency only |
