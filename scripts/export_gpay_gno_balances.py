#!/usr/bin/env python3
"""Export per-user GNO balances for Gnosis Pay wallets to CSV.

Two sources (pick with --source):

  dbt      (default) - instant, no RPC. Reads the already-built dbt balances
           layer `int_execution_gpay_balances_daily` as of its latest day.
           Event-derived (cumulative ERC-20 transfers), refreshed by the
           pipeline; effectively equal to on-chain for GNO.

  onchain  - ground truth, ~minutes. Reads balanceOf(GNO) live from a Gnosis
           RPC node for every Gnosis Pay wallet, via batched JSON-RPC
           eth_call (pure stdlib, no web3). A non-"latest" --block needs an
           ARCHIVE node.

Both apply identical June-2026 Safe-migration handling and write the same
schema, keyed by the canonical (new) Safe:

  pay_wallet,amount,balance_source

  balance_source:
    single         - non-migrated wallet (or new Safe holding only its own GNO)
    old_safe_only  - funds still entirely on the old Safe, shown under the new
                     Safe (the new Safe itself holds 0 - an on-chain balanceOf
                     of pay_wallet will NOT match amount)
    combined       - old Safe + new Safe balances merged under the new Safe

Migration rule (matches int_execution_gpay_balances_user_daily): exploited/
refunded ("lost") old Safes are dropped from first_refund_at onward (their
residual is recovery-entitled, not user funds); every other old Safe is
remapped onto its migration new Safe and summed, so no old-Safe address
appears in the output.

Connection comes from the repo .env (CLICKHOUSE_*). On-chain mode also needs
GNOSIS_RPC_URL (in .env or --rpc-url). See README_gpay_gno_balances.md.

Examples:
  python scripts/export_gpay_gno_balances.py
  python scripts/export_gpay_gno_balances.py --source onchain --block latest
  python scripts/export_gpay_gno_balances.py --source onchain --out /tmp/gno.csv
"""
import argparse
import csv
import json
import os
import ssl
import sys
import time
import urllib.request
from datetime import date
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENV = ROOT / ".env"

# GNO ERC-20 on Gnosis Chain.
GNO_TOKEN = "0x9c58bacc331c9aa871afd802db6379a98e80cedb"
# balanceOf(address) selector.
BALANCE_OF = "0x70a08231"
# Public fallback; rate-limited and NOT an archive node. Set GNOSIS_RPC_URL to
# a dedicated (and, for historical blocks, archive) endpoint for real runs.
DEFAULT_RPC = "https://rpc.gnosischain.com"


def load_env(path: Path) -> dict:
    env = {}
    if not path.exists():
        return env
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def fmt_amount(wei: int) -> str:
    """Exact GNO decimal from integer wei (18 decimals), trailing zeros trimmed."""
    whole, frac = divmod(wei, 10**18)
    if frac == 0:
        return str(whole)
    return f"{whole}.{frac:018d}".rstrip("0")


def ch_post(env: dict, sql: str) -> str:
    host = env["CLICKHOUSE_URL"]
    port = env.get("CLICKHOUSE_PORT", "8443")
    user = env["CLICKHOUSE_USER"]
    pwd = env["CLICKHOUSE_PASSWORD"]
    scheme = "https" if env.get("CLICKHOUSE_SECURE", "True").lower() == "true" else "http"
    req = urllib.request.Request(f"{scheme}://{host}:{port}/", data=sql.encode())
    req.add_header("X-ClickHouse-User", user)
    req.add_header("X-ClickHouse-Key", pwd)
    with urllib.request.urlopen(req, context=ssl.create_default_context(), timeout=300) as resp:
        return resp.read().decode()


def ch_rows(env: dict, sql: str) -> list:
    body = ch_post(env, sql + " FORMAT TabSeparated")
    return [line.split("\t") for line in body.splitlines() if line]


def load_switchover(env: dict) -> list:
    """[old_safe, new_safe, is_lost, first_refund_at] per migrated pair."""
    return ch_rows(
        env,
        "SELECT lower(old_safe), lower(new_safe), toString(is_lost), "
        "ifNull(toString(first_refund_at), '') "
        "FROM dbt.int_execution_gpay_safe_switchover",
    )


def remap(balances: dict, sw_rows: list) -> list:
    """Apply the migration rule. balances: {addr(lower): wei int}.

    Returns [(canonical_addr, wei int, balance_source)] sorted by amount desc.
    """
    today = date.today()
    lost = set()
    old2new = {}
    for old, new, is_lost, refund in sw_rows:
        old2new[old] = new
        if is_lost == "1" and refund:
            try:
                if date.fromisoformat(refund) <= today:
                    lost.add(old)
            except ValueError:
                pass

    agg = {}  # canonical -> [old_part_wei, new_part_wei]
    for addr, wei in balances.items():
        if wei <= 0 or addr in lost:
            continue
        if addr in old2new:
            canon, is_old = old2new[addr], True
        else:
            canon, is_old = addr, False
        slot = agg.setdefault(canon, [0, 0])
        slot[0 if is_old else 1] += wei

    out = []
    for canon, (old_part, new_part) in agg.items():
        total = old_part + new_part
        if total <= 0:
            continue
        if old_part > 0 and new_part > 0:
            src = "combined"
        elif old_part > 0:
            src = "old_safe_only"
        else:
            src = "single"
        out.append((canon, total, src))
    out.sort(key=lambda r: (-r[1], r[0]))
    return out


def balances_from_dbt(env: dict):
    asof = ch_rows(
        env,
        "SELECT toString(max(date)) FROM dbt.int_execution_gpay_balances_daily "
        "WHERE symbol = 'GNO' AND date < today()",
    )[0][0]
    rows = ch_rows(
        env,
        "SELECT lower(address), toString(balance) "
        "FROM dbt.int_execution_gpay_balances_daily "
        f"WHERE symbol = 'GNO' AND date = toDate('{asof}') AND balance > 0",
    )
    balances = {addr: int(Decimal(bal) * (10**18)) for addr, bal in rows}
    return balances, asof


def _rpc_post(rpc_url: str, payload: list, ctx, retries: int = 4):
    data = json.dumps(payload).encode()
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                rpc_url, data=data, headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, context=ctx, timeout=120) as resp:
                return json.loads(resp.read().decode())
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(1.5 * (attempt + 1))


def balances_onchain(env: dict, rpc_url: str, block: str, batch_size: int):
    wallets = [r[0] for r in ch_rows(
        env, "SELECT DISTINCT lower(address) FROM dbt.int_execution_gpay_wallets"
    )]
    ctx = ssl.create_default_context()
    blk = block if block == "latest" else hex(int(block))
    balances = {}

    def sweep(chunk):
        payload = [
            {"jsonrpc": "2.0", "id": j, "method": "eth_call",
             "params": [{"to": GNO_TOKEN, "data": BALANCE_OF + a[2:].rjust(64, "0")}, blk]}
            for j, a in enumerate(chunk)
        ]
        try:
            resp = _rpc_post(rpc_url, payload, ctx)
        except Exception:
            if len(chunk) > 1:  # batch too large / rate-limited: split and retry
                mid = len(chunk) // 2
                sweep(chunk[:mid])
                sweep(chunk[mid:])
                return
            balances[chunk[0]] = 0
            return
        by_id = {item.get("id"): item for item in resp}
        for j, a in enumerate(chunk):
            res = by_id.get(j, {}).get("result")
            balances[a] = int(res, 16) if isinstance(res, str) and res not in ("", "0x") else 0

    total = len(wallets)
    for i in range(0, total, batch_size):
        sweep(wallets[i:i + batch_size])
        print(f"  ...{min(i + batch_size, total)}/{total} wallets read", file=sys.stderr)

    asof = date.today().isoformat() if block == "latest" else f"block-{block}"
    return balances, asof


def main() -> int:
    ap = argparse.ArgumentParser(description="Export Gnosis Pay per-user GNO balances to CSV.")
    ap.add_argument("--source", choices=["dbt", "onchain"], default="dbt",
                    help="dbt (default, instant) or onchain (live balanceOf, ~minutes)")
    ap.add_argument("--out", default=None,
                    help="output CSV path (default gpay_gno_balances_<as-of>.csv in repo root)")
    ap.add_argument("--block", default="latest",
                    help="onchain only: 'latest' or a block number (archive node needed)")
    ap.add_argument("--rpc-url", default=None,
                    help="onchain only: overrides GNOSIS_RPC_URL / the public default")
    ap.add_argument("--batch-size", type=int, default=500,
                    help="onchain only: eth_call requests per JSON-RPC batch")
    args = ap.parse_args()

    env = load_env(ENV)

    if args.source == "dbt":
        balances, asof = balances_from_dbt(env)
    else:
        rpc_url = args.rpc_url or env.get("GNOSIS_RPC_URL") or DEFAULT_RPC
        if rpc_url == DEFAULT_RPC:
            print("WARNING: using public RPC (rate-limited, not archive). "
                  "Set GNOSIS_RPC_URL for real runs.", file=sys.stderr)
        print(f"Sweeping balanceOf(GNO) via {rpc_url} at block {args.block} ...", file=sys.stderr)
        balances, asof = balances_onchain(env, rpc_url, args.block, args.batch_size)

    rows = remap(balances, load_switchover(env))

    out = Path(args.out) if args.out else ROOT / f"gpay_gno_balances_{asof}.csv"
    total_wei = 0
    counts = {}
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["pay_wallet", "amount", "balance_source"])
        for addr, wei, src in rows:
            w.writerow([addr, fmt_amount(wei), src])
            total_wei += wei
            counts[src] = counts.get(src, 0) + 1

    print(f"Wrote {len(rows)} wallets to {out}  (source={args.source}, as-of {asof})")
    print(f"Total GNO: {fmt_amount(total_wei)}")
    print("Breakdown: " + ", ".join(f"{k}={counts[k]}" for k in sorted(counts)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
