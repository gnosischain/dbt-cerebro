#!/usr/bin/env python3
"""Export the active users for one month to CSV, keyed by fees paid.

"Active" here means the user paid net-positive total fees in the target
calendar month, summed across every fee stream (holdings, sDAI, Gnosis Pay,
Gnosis App). Source is the revenue rollup model
`int_revenue_fees_monthly_per_user` (dbt schema), which stores one row per
(month, stream_type, symbol, user) with a `month_fees` amount.

Equivalent SQL (flattened form of the request):

  SELECT user, round(sum(month_fees), 4) AS fees
  FROM dbt.int_revenue_fees_monthly_per_user FINAL
  WHERE user IS NOT NULL AND month = toDate('<month>')
  GROUP BY user HAVING fees >= <min_fee>
  ORDER BY fees DESC

`--min-fee` defaults to 0.01 (drop sub-cent dust users, matching the Dune
`gnosis_month_active_users` definition). FINAL dedupes the ReplacingMergeTree
so unmerged duplicate rows are not double-counted; the month filter lives in
the WHERE so ClickHouse prunes to the single monthly partition (the model is
partitioned by month).

Reads the ClickHouse connection from the repo .env. Writes:
  active_users_fees_<YYYY-MM>.csv   (columns: user,fees)

Run:  python scripts/export_active_users_by_fees.py --month 2025-06-01
      python scripts/export_active_users_by_fees.py --month 2026-06 --min-fee 0.01
"""
import argparse
import csv
import os
import ssl
import sys
import urllib.request
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ENV = ROOT / ".env"
# Schema the dbt models land in (matches the hardcoded `dbt.` prefix used by
# the sibling export scripts); overridable via the same env var dbt reads.
DB = os.environ.get("CLICKHOUSE_DATABASE", "dbt")
TABLE = f"{DB}.int_revenue_fees_monthly_per_user"


def load_env(path: Path) -> dict:
    env = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def parse_month(value: str) -> date:
    """Accept a YYYY-MM-DD (or YYYY-MM) date and normalize to the 1st of month."""
    for fmt in ("%Y-%m-%d", "%Y-%m"):
        try:
            return datetime.strptime(value, fmt).date().replace(day=1)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f"invalid month {value!r}; expected YYYY-MM-DD or YYYY-MM"
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "--month",
        required=True,
        type=parse_month,
        help="target month, YYYY-MM-DD (normalized to the 1st) or YYYY-MM",
    )
    ap.add_argument(
        "--output",
        help="output CSV path (default: active_users_fees_<YYYY-MM>.csv in repo root)",
    )
    ap.add_argument(
        "--min-fee",
        type=float,
        default=0.01,
        help="minimum total monthly fee per user to include (default: 0.01)",
    )
    args = ap.parse_args()

    month = args.month
    out = Path(args.output) if args.output else ROOT / f"active_users_fees_{month:%Y-%m}.csv"

    env = load_env(ENV)
    host = env["CLICKHOUSE_URL"]
    port = env.get("CLICKHOUSE_PORT", "8443")
    user = env["CLICKHOUSE_USER"]
    pwd = env["CLICKHOUSE_PASSWORD"]
    scheme = "https" if env.get("CLICKHOUSE_SECURE", "True").lower() == "true" else "http"

    query = (
        f"SELECT user, round(sum(month_fees), 4) AS fees "
        f"FROM {TABLE} FINAL "
        f"WHERE user IS NOT NULL AND month = toDate('{month:%Y-%m-%d}') "
        f"GROUP BY user HAVING fees >= {args.min_fee} "
        f"ORDER BY fees DESC, user "
        f"FORMAT TabSeparated"
    )
    url = f"{scheme}://{host}:{port}/"
    req = urllib.request.Request(url, data=query.encode())
    req.add_header("X-ClickHouse-User", user)
    req.add_header("X-ClickHouse-Key", pwd)
    ctx = ssl.create_default_context()

    with urllib.request.urlopen(req, context=ctx, timeout=120) as resp:
        body = resp.read().decode()

    rows = 0
    total_fees = 0.0
    with out.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["user", "fees"])
        for line in body.splitlines():
            if not line:
                continue
            addr, fees_s = line.split("\t")
            total_fees += float(fees_s)
            w.writerow([addr, fees_s])
            rows += 1

    print(f"Wrote {rows} active users (fees >= {args.min_fee}) to {out}")
    print(f"Total fees ({month:%Y-%m}): {round(total_fees, 4)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
