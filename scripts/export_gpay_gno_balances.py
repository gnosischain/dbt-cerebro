#!/usr/bin/env python3
"""Export per-wallet GNO balances for all Gnosis Pay wallets to CSV.

Source: live on-chain balanceOf(GNO) reads captured in the ClickHouse
scratch table scratch.rpc_calls_620582fa (block 46,779,442, 2026-06-19).
Each wallet was read directly from the GNO ERC-20 contract
(0x9c58bacc331c9aa871afd802db6379a98e80cedb), so the output IS the on-chain
state, already cross-checked against int_execution_gpay_balances_daily.

Output is keyed by the canonical (new) Safe: surviving old-Safe balances are
remapped onto their migration new Safe and summed, so no old-Safe address
appears. balance_source flags each row's origin: 'single' (no migration),
'old_safe_only' (funds still entirely on the old Safe, shown under the new
Safe which holds none), 'combined' (old + new merged).

Reads the ClickHouse connection from the repo .env. Writes:
  gpay_gno_balances_onchain.csv   (columns: pay_wallet,amount,balance_source)

Run:  python scripts/export_gpay_gno_balances.py
"""
import csv
import os
import ssl
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENV = ROOT / ".env"
# Output path and source snapshot table are overridable per run so the export
# can target a fresh rpc_batch_call balanceOf(GNO) sweep (each lands in a new
# scratch.rpc_calls_<id> table that expires ~7 days after capture).
OUT = ROOT / os.environ.get("GPAY_GNO_OUT", "gpay_gno_balances_onchain.csv")
SCRATCH = os.environ.get("GPAY_GNO_SCRATCH", "scratch.rpc_calls_620582fa")


def load_env(path: Path) -> dict:
    env = {}
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


def main() -> int:
    env = load_env(ENV)
    host = env["CLICKHOUSE_URL"]
    port = env.get("CLICKHOUSE_PORT", "8443")
    user = env["CLICKHOUSE_USER"]
    pwd = env["CLICKHOUSE_PASSWORD"]
    scheme = "https" if env.get("CLICKHOUSE_SECURE", "True").lower() == "true" else "http"

    # June 2026 Safe migration handling, keyed by the canonical (new) Safe and
    # matching int_execution_gpay_balances_user_daily semantics:
    #  - drop exploited/refunded ("lost") old-Safe residuals from first_refund_at
    #    onward - they are recovery-entitled, not user funds.
    #  - remap every surviving old Safe onto its migration new Safe and sum, so
    #    no old-Safe address remains (the GNO was never moved off the old Safe -
    #    new Safes hold only dust - so this folds the real balance onto the new
    #    Safe instead of dropping it, which would understate by ~1.6k GNO).
    # balance_source tags each row: 'combined' = old+new both contributed,
    # 'old_safe_only' = balance is entirely on the old Safe (new Safe holds 0),
    # 'single' = non-migrated wallet (or new Safe with only its own balance).
    query = (
        f"WITH sw AS ("
        f"  SELECT old_safe, new_safe, is_lost, first_refund_at"
        f"  FROM dbt.int_execution_gpay_safe_switchover"
        f"), "
        f"kept AS ("
        f"  SELECT lower(address) AS addr, gno_out_0 AS gno"
        f"  FROM {SCRATCH} FINAL"
        f"  WHERE gno_out_0 > 0"
        f"    AND lower(address) NOT IN (SELECT old_safe FROM sw WHERE is_lost = 1 AND first_refund_at <= today())"
        f"), "
        f"remapped AS ("
        f"  SELECT if(m.old_safe != '', m.new_safe, k.addr) AS canonical_addr,"
        f"         (m.old_safe != '') AS is_old_src, k.gno AS gno"
        f"  FROM kept k LEFT JOIN sw m ON m.old_safe = k.addr"
        f") "
        f"SELECT canonical_addr, toString(sum(gno)), "
        f"  multiIf(sumIf(gno, is_old_src) > 0 AND sumIf(gno, NOT is_old_src) > 0, 'combined',"
        f"          sumIf(gno, is_old_src) > 0, 'old_safe_only', 'single') "
        f"FROM remapped GROUP BY canonical_addr "
        f"ORDER BY sum(gno) DESC, canonical_addr FORMAT TabSeparated"
    )
    url = f"{scheme}://{host}:{port}/"
    req = urllib.request.Request(url, data=query.encode())
    req.add_header("X-ClickHouse-User", user)
    req.add_header("X-ClickHouse-Key", pwd)
    ctx = ssl.create_default_context()

    with urllib.request.urlopen(req, context=ctx, timeout=120) as resp:
        body = resp.read().decode()

    rows = 0
    total_wei = 0
    with OUT.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["pay_wallet", "amount", "balance_source"])
        for line in body.splitlines():
            if not line:
                continue
            addr, wei_s, source = line.split("\t")
            wei = int(wei_s)
            total_wei += wei
            w.writerow([addr, fmt_amount(wei), source])
            rows += 1

    print(f"Wrote {rows} wallets to {OUT}")
    print(f"Total GNO: {fmt_amount(total_wei)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
