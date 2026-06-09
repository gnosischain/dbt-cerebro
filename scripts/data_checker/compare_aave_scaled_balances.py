#!/usr/bin/env python3
"""
compare_aave_scaled_balances.py
================================
Compares int_execution_lending_aave_user_balances_daily (DB scaled_balance)
with on-chain scaledBalanceOf() at the last block of each UTC day.

Everything is queried live — no hardcoded addresses or blocks.

Output CSV columns:
  date, block, protocol, symbol, reserve_address, atoken_address, atoken_symbol,
  user_address, db_scaled_balance, rpc_scaled_balance, diff_wei, diff_abs_wei

Install:
    pip install clickhouse-connect requests

Run:
    python compare_aave_scaled_balances.py \\
        --rpc        https://your-gnosis-archive-rpc \\
        --ch-host    your-clickhouse-host \\
        --ch-user    user \\
        --ch-password pass \\
        [--top-n     20]                      # users per market, default 20
        [--dates     2025-03-01,2025-05-01]   # default: 7 monthly snapshots
        [--output    comparison.csv]
        [--workers   8]                       # parallel HTTP workers

Environment variable fallbacks:
    GNOSIS_ARCHIVE_RPC_URL
    CLICKHOUSE_HOST / CLICKHOUSE_PORT / CLICKHOUSE_USER / CLICKHOUSE_PASSWORD
"""

import argparse
import csv
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import requests

# ── ABI ──────────────────────────────────────────────────────────────────────
# keccak256("scaledBalanceOf(address)")[:4] = 0x1da2c1ec
SCALED_BALANCE_OF_SEL = "1da2c1ec"

DEFAULT_DATES = [
    "2024-11-01", "2024-12-01",
    "2025-01-01", "2025-02-01", "2025-03-01",
    "2025-04-01", "2025-05-01",
]

BATCH_SIZE = 250   # eth_call requests per HTTP batch


# ── ABI encoding ─────────────────────────────────────────────────────────────

def encode_call(user_address: str) -> str:
    """Encode calldata for scaledBalanceOf(address)."""
    addr = user_address.lower().replace("0x", "").zfill(64)
    return f"0x{SCALED_BALANCE_OF_SEL}{addr}"


def decode_uint256(hex_val: Optional[str]) -> Optional[int]:
    if not hex_val or hex_val in ("0x", "0x0", ""):
        return None
    try:
        return int(hex_val, 16)
    except ValueError:
        return None


# ── JSON-RPC ─────────────────────────────────────────────────────────────────

def rpc_batch(session: requests.Session, rpc_url: str, calls: list[dict]) -> list[Optional[str]]:
    """Send one JSON-RPC batch; return raw hex results in call order."""
    payload = [
        {
            "jsonrpc": "2.0",
            "id": i,
            "method": "eth_call",
            "params": [
                {"to": c["to"], "data": c["data"]},
                hex(c["block"]),
            ],
        }
        for i, c in enumerate(calls)
    ]
    resp = session.post(rpc_url, json=payload, timeout=120)
    resp.raise_for_status()
    by_id = {r["id"]: r.get("result") for r in resp.json()}
    return [by_id.get(i) for i in range(len(calls))]


def call_all(
    rpc_url: str,
    specs: list[dict],
    workers: int = 8,
) -> list[Optional[int]]:
    """Execute all specs in parallel batches; return results in input order."""
    n = len(specs)
    results: list[Optional[int]] = [None] * n

    batches = [
        (list(range(i, min(i + BATCH_SIZE, n))), specs[i : i + BATCH_SIZE])
        for i in range(0, n, BATCH_SIZE)
    ]

    session = requests.Session()
    session.headers["Content-Type"] = "application/json"
    done = 0

    def run(indices, batch_specs):
        raw = rpc_batch(session, rpc_url, batch_specs)
        return [(indices[j], decode_uint256(raw[j])) for j in range(len(indices))]

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(run, idx, s): len(idx) for idx, s in batches}
        for fut in as_completed(futures):
            for pos, val in fut.result():
                results[pos] = val
            done += futures[fut]
            print(f"  {done}/{n} RPC calls done", end="\r", flush=True)

    print()
    return results


# ── ClickHouse helpers ────────────────────────────────────────────────────────

def ch_rows(client, sql: str) -> list[dict]:
    r = client.query(sql)
    return [dict(zip(r.column_names, row)) for row in r.result_rows]


def sql_list(values: list[str]) -> str:
    return ", ".join(f"'{v}'" for v in values)


# ── ClickHouse queries ────────────────────────────────────────────────────────

def sql_last_blocks(dates: list[str]) -> str:
    return f"""
SELECT
    toString(toDate(block_timestamp)) AS date,
    max(block_number)                 AS last_block
FROM execution.blocks
WHERE toDate(block_timestamp) IN ({sql_list(dates)})
GROUP BY date
ORDER BY date
"""


def sql_top_users(anchor_date: str, top_n: int) -> str:
    """
    Top-N holders per (protocol, reserve) at anchor_date, joined with
    lending_market_mapping to get the atoken address.
    Ranking by descending string-length then value catches large Int256 correctly.
    """
    return f"""
SELECT
    b.protocol,
    b.symbol,
    b.reserve_address,
    m.supply_token_address AS atoken_address,
    m.supply_token_symbol  AS atoken_symbol,
    b.user_address
FROM (
    SELECT
        protocol, reserve_address, symbol, user_address,
        ROW_NUMBER() OVER (
            PARTITION BY protocol, reserve_address
            ORDER BY
                length(toString(scaled_balance)) DESC,
                toString(scaled_balance)          DESC
        ) AS rn
    FROM dbt.int_execution_lending_aave_user_balances_daily
    WHERE date = '{anchor_date}'
      AND scaled_balance > 0
    GROUP BY protocol, reserve_address, symbol, user_address, scaled_balance
) b
JOIN dbt.lending_market_mapping m
    ON b.protocol              = m.protocol
   AND lower(b.reserve_address) = lower(m.reserve_address)
WHERE b.rn <= {top_n}
ORDER BY b.protocol, b.symbol, b.rn
"""


def sql_db_balances(dates: list[str], users: list[str]) -> str:
    return f"""
SELECT
    toString(b.date)           AS date,
    b.protocol,
    b.symbol,
    b.reserve_address,
    b.user_address,
    m.supply_token_address     AS atoken_address,
    m.supply_token_symbol      AS atoken_symbol,
    toString(b.scaled_balance) AS db_scaled_balance
FROM dbt.int_execution_lending_aave_user_balances_daily b
JOIN dbt.lending_market_mapping m
    ON b.protocol              = m.protocol
   AND lower(b.reserve_address) = lower(m.reserve_address)
WHERE b.date IN ({sql_list(dates)})
  AND b.scaled_balance > 0
  AND b.user_address   IN ({sql_list(users)})
ORDER BY b.protocol, b.symbol, b.user_address, b.date
"""


# ── Main ─────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Compare DB scaled_balance vs on-chain scaledBalanceOf"
    )
    p.add_argument("--rpc",         default=os.getenv("GNOSIS_ARCHIVE_RPC_URL"),
                   help="Gnosis archive RPC URL  [env: GNOSIS_ARCHIVE_RPC_URL]")
    p.add_argument("--ch-host",     default=os.getenv("CLICKHOUSE_HOST", "localhost"))
    p.add_argument("--ch-port",     type=int, default=int(os.getenv("CLICKHOUSE_PORT", "8443")))
    p.add_argument("--ch-user",     default=os.getenv("CLICKHOUSE_USER", "default"))
    p.add_argument("--ch-password", default=os.getenv("CLICKHOUSE_PASSWORD", ""))
    p.add_argument("--ch-secure",   action="store_true",
                   default=os.getenv("CLICKHOUSE_SECURE", "true").lower() == "true")
    p.add_argument("--top-n",  type=int, default=20,
                   help="Top-N holders per (protocol, reserve)  [default: 20]")
    p.add_argument("--dates",  default=",".join(DEFAULT_DATES),
                   help="Comma-separated dates  [default: 7 monthly snapshots]")
    p.add_argument("--output", default="aave_scaled_balance_comparison.csv")
    p.add_argument("--workers", type=int, default=8,
                   help="Parallel HTTP workers  [default: 8]")
    return p.parse_args()


def main():
    args = parse_args()

    if not args.rpc:
        sys.exit("ERROR: --rpc or GNOSIS_ARCHIVE_RPC_URL is required")

    dates = [d.strip() for d in args.dates.split(",") if d.strip()]
    anchor_date = max(dates)

    # 1. Connect ────────────────────────────────────────────────────────────
    print("Connecting to ClickHouse…")
    import clickhouse_connect
    client = clickhouse_connect.get_client(
        host=args.ch_host,
        port=args.ch_port,
        user=args.ch_user,
        password=args.ch_password,
        secure=args.ch_secure,
    )

    # 2. Last block per date ────────────────────────────────────────────────
    print(f"Querying last block for {len(dates)} dates…")
    block_rows = ch_rows(client, sql_last_blocks(dates))
    date_to_block: dict[str, int] = {r["date"]: int(r["last_block"]) for r in block_rows}
    missing = [d for d in dates if d not in date_to_block]
    if missing:
        print(f"  WARNING: no block data for: {missing}; skipping those dates")
    dates = [d for d in dates if d in date_to_block]
    for d in sorted(date_to_block):
        print(f"  {d}  →  block {date_to_block[d]:,}")

    # 3. Top-N users per market ─────────────────────────────────────────────
    print(f"\nQuerying top-{args.top_n} users per market (anchor: {anchor_date})…")
    user_rows = ch_rows(client, sql_top_users(anchor_date, args.top_n))
    sampled_users = sorted({r["user_address"] for r in user_rows})
    print(f"  {len(user_rows)} (protocol, market, user) combos → {len(sampled_users)} unique addresses")

    # 4. DB balances ────────────────────────────────────────────────────────
    print(f"\nFetching DB balances for {len(sampled_users)} users × {len(dates)} dates…")
    db_rows = ch_rows(client, sql_db_balances(dates, sampled_users))
    for row in db_rows:
        row["block"] = date_to_block[row["date"]]
    print(f"  {len(db_rows)} rows")

    # 5. Deduplicate RPC calls ──────────────────────────────────────────────
    print("\nDeduplicating RPC calls…")
    key_to_idx: dict[tuple, int] = {}
    specs: list[dict] = []
    for row in db_rows:
        key = (row["atoken_address"].lower(), row["user_address"].lower(), row["block"])
        if key not in key_to_idx:
            key_to_idx[key] = len(specs)
            specs.append({
                "to":    row["atoken_address"],
                "data":  encode_call(row["user_address"]),
                "block": row["block"],
            })
    print(f"  {len(db_rows)} DB rows → {len(specs)} unique RPC calls")

    # 6. Execute RPC ────────────────────────────────────────────────────────
    print(f"\nExecuting {len(specs)} eth_call batches against {args.rpc}…")
    rpc_values = call_all(args.rpc, specs, workers=args.workers)
    ok = sum(1 for v in rpc_values if v is not None)
    print(f"  {ok}/{len(specs)} calls returned a value")

    # 7. Write CSV ──────────────────────────────────────────────────────────
    print(f"\nWriting {args.output}…")
    fieldnames = [
        "date", "block", "protocol", "symbol",
        "reserve_address", "atoken_address", "atoken_symbol",
        "user_address",
        "db_scaled_balance", "rpc_scaled_balance",
        "diff_wei", "diff_abs_wei",
    ]

    counts = {"exact": 0, "nonzero_diff": 0, "no_rpc": 0}

    with open(args.output, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in db_rows:
            key = (row["atoken_address"].lower(), row["user_address"].lower(), row["block"])
            rpc_val = rpc_values[key_to_idx[key]]
            try:
                db_int = int(row["db_scaled_balance"])
            except (ValueError, TypeError):
                db_int = None

            if rpc_val is not None and db_int is not None:
                diff = rpc_val - db_int
                counts["exact" if diff == 0 else "nonzero_diff"] += 1
            else:
                diff = None
                counts["no_rpc"] += 1

            w.writerow({
                "date":               row["date"],
                "block":              row["block"],
                "protocol":           row["protocol"],
                "symbol":             row["symbol"],
                "reserve_address":    row["reserve_address"],
                "atoken_address":     row["atoken_address"],
                "atoken_symbol":      row["atoken_symbol"],
                "user_address":       row["user_address"],
                "db_scaled_balance":  row["db_scaled_balance"],
                "rpc_scaled_balance": str(rpc_val) if rpc_val is not None else "",
                "diff_wei":           str(diff)     if diff    is not None else "",
                "diff_abs_wei":       str(abs(diff)) if diff   is not None else "",
            })

    print(f"\n{'─' * 48}")
    print(f"  Total rows     : {len(db_rows)}")
    print(f"  Exact match    : {counts['exact']}")
    print(f"  Non-zero diff  : {counts['nonzero_diff']}")
    print(f"  No RPC value   : {counts['no_rpc']}")
    print(f"  Output         : {args.output}")
    print(f"{'─' * 48}")


if __name__ == "__main__":
    main()