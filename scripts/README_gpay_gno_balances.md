# Gnosis Pay GNO balances export

`export_gpay_gno_balances.py` writes a CSV of **GNO held per Gnosis Pay user**,
keyed by each user's current (canonical) Safe.

Output columns:

| column | meaning |
|--------|---------|
| `pay_wallet` | the user's canonical **new** Safe address (no old-Safe addresses appear) |
| `amount` | GNO balance, exact decimal (18 dp, trailing zeros trimmed) |
| `balance_source` | how `amount` was derived (see below) |

`balance_source` values:

| value | meaning |
|-------|---------|
| `single` | non-migrated wallet, or a new Safe holding only its own GNO |
| `old_safe_only` | funds are still entirely on the **old** Safe, shown here under the **new** Safe. The new Safe itself holds 0 — so an on-chain `balanceOf(pay_wallet)` will **not** match `amount` |
| `combined` | old Safe + new Safe balances merged under the new Safe |

## Why two modes

The actual on-chain read (`balanceOf` for ~66k wallets) used to be a one-off
`rpc_batch_call` that dumped results into a temporary `scratch.rpc_calls_*`
table. Those tables **auto-expire after ~7 days**, after which the old export
script hit `HTTP 404` (ClickHouse "unknown table"). This script removes that
dependency — it gets balances from a durable source every run.

## Prerequisites

- Repo `.env` with ClickHouse connection: `CLICKHOUSE_URL`, `CLICKHOUSE_PORT`,
  `CLICKHOUSE_SECURE`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`.
- **On-chain mode only:** a Gnosis RPC endpoint. Add to `.env`:
  ```
  GNOSIS_RPC_URL="https://your-gnosis-rpc"
  ```
  or pass `--rpc-url`. For `--block latest` any full node works; for a
  historical `--block N` you need an **archive** node.
- No third-party Python packages — standard library only (works under any
  Python 3.8+, incl. system/anaconda).

## Usage

### Mode 1 — dbt (default, instant, recommended)

Reads the already-built dbt balances layer (`int_execution_gpay_balances_daily`)
as of its latest built day. No RPC. Runs in ~1 second.

```bash
python scripts/export_gpay_gno_balances.py
```

Writes `gpay_gno_balances_<as-of-date>.csv` in the repo root. The balances are
event-derived (cumulative ERC-20 transfers) and refreshed by the pipeline; for
GNO this equals on-chain state to the day.

### Mode 2 — onchain (ground truth, ~minutes)

Reads `balanceOf(GNO)` live from the RPC node for every Gnosis Pay wallet
(batched JSON-RPC `eth_call`).

```bash
# current block
python scripts/export_gpay_gno_balances.py --source onchain --block latest

# a historical block (needs an archive node)
python scripts/export_gpay_gno_balances.py --source onchain --block 46779442
```

Writes `gpay_gno_balances_<date>.csv` (or `gpay_gno_balances_block-<n>.csv`).

### Common options

| flag | applies to | default | notes |
|------|-----------|---------|-------|
| `--source dbt\|onchain` | both | `dbt` | data source |
| `--out PATH` | both | auto-named in repo root | output CSV path |
| `--block latest\|N` | onchain | `latest` | historical N needs archive |
| `--rpc-url URL` | onchain | `$GNOSIS_RPC_URL` else public | overrides endpoint |
| `--batch-size N` | onchain | `500` | `eth_call`s per JSON-RPC batch; the script auto-splits a batch the endpoint rejects, so lower this only if you keep seeing failures |

## Migration handling (both modes, identical)

Mirrors `int_execution_gpay_balances_user_daily` for the June-2026 Safe
migration:

- **Exploited / refunded ("lost") old Safes** are dropped from their
  `first_refund_at` onward — the residual is recovery-entitled, not user funds.
- **Every other old Safe** is remapped onto its migration new Safe and summed.
  The GNO was generally never moved off the old Safe (new Safes typically hold
  only dust), so this folds the real balance onto the canonical new Safe rather
  than dropping it.

Sources used: `int_execution_gpay_wallets` (wallet universe, onchain mode),
`int_execution_gpay_safe_switchover` (old→new map + `is_lost` / `first_refund_at`),
`int_execution_gpay_balances_daily` (balances, dbt mode).

## Sanity check

Total GNO should land near the dbt user-holdings aggregate
`fct_execution_gpay_balances_by_token_daily` (symbol `GNO`) for the same day.
The two modes agree within one day of on-chain movement (GNO is not rebasing).

## Troubleshooting

- `HTTP 404` / `UNKNOWN_TABLE` — a referenced table doesn't exist. This script
  uses only durable dbt tables, so check that the dbt models are built and that
  `CLICKHOUSE_*` in `.env` point at the right database.
- `HTTP 516` / auth error — wrong `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD`.
- On-chain run slow or many "read 0" — the public RPC is rate-limiting; set
  `GNOSIS_RPC_URL` to a dedicated endpoint (and an archive node for historical
  blocks).
