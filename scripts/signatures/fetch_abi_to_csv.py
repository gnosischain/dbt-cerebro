#!/usr/bin/env python3
"""
Fetch a contract ABI from Blockscout and append it to seeds/contracts_abi.csv.

Unlike `dbt run-operation fetch_and_insert_abi`, this writes to the CSV
(the canonical source of truth) rather than directly to the ClickHouse
contracts_abi table. Running this then `dbt seed --select contracts_abi`
pushes the ABI to ClickHouse in a way that survives future seed runs,
which the dbt operation does NOT: `dbt seed` replaces the table with the
CSV's contents on every run, silently wiping anything inserted via the
macro.

Usage:
    python scripts/signatures/fetch_abi_to_csv.py 0xADDRESS
    python scripts/signatures/fetch_abi_to_csv.py 0xADDRESS --name CustomName
    python scripts/signatures/fetch_abi_to_csv.py 0xADDRESS --force   # overwrite existing row
    python scripts/signatures/fetch_abi_to_csv.py 0xADDRESS --chain celo   # non-default chain

Egress-less fallback (reads from the ClickHouse contracts_abi table
instead of the Blockscout HTTP API — requires that
`dbt run-operation fetch_and_insert_abi` has already run for the
address):
    dbt run-operation fetch_and_insert_abi --args '{"address": "0xADDRESS"}'
    python scripts/signatures/fetch_abi_to_csv.py 0xADDRESS --from-ch

Normal follow-up after running this:
    dbt seed --select contracts_abi
    python scripts/signatures/signature_generator.py
    dbt seed --select event_signatures function_signatures

One-shot convenience (all four steps chained, only on success):
    python scripts/signatures/fetch_abi_to_csv.py 0xADDRESS --regen

Only the append-to-CSV step is in this script. The --regen flag shells
out to dbt and the signature generator for the three follow-up commands
so a single invocation leaves the warehouse fully in sync.
"""
import argparse
import csv
import json
import re
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

# Per-chain Blockscout instances. Adding a chain here (plus rows in the
# seed CSVs) is all the decoding pipeline needs to support it.
BLOCKSCOUT_HOSTS = {
    "gnosis": "https://gnosis.blockscout.com",
    "celo": "https://celo.blockscout.com",
}
DEFAULT_CHAIN = "gnosis"
# Set by main() from --chain; module-level so the fetch helpers stay simple.
BLOCKSCOUT_BASE = f"{BLOCKSCOUT_HOSTS[DEFAULT_CHAIN]}/api/v2/smart-contracts"
BLOCKSCOUT_V1 = f"{BLOCKSCOUT_HOSTS[DEFAULT_CHAIN]}/api"
REPO_ROOT = Path(__file__).resolve().parents[2]
CSV_PATH = REPO_ROOT / "seeds" / "contracts_abi.csv"
ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")

_BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def _get_json(url: str) -> dict:
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": _BROWSER_UA},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def _fetch_blockscout_v1(address: str) -> dict:
    """Fallback to Blockscout v1 API (etherscan-compatible).

    Used when the v2 endpoint returns a 5xx error (common for Vyper contracts).
    Calls getabi + getsourcecode to recover ABI and contract name.
    """
    import urllib.parse

    abi_url = (
        f"{BLOCKSCOUT_V1}?module=contract&action=getabi"
        f"&address={address}"
    )
    body = _get_json(abi_url)
    if body.get("status") != "1":
        raise SystemExit(
            f"Blockscout v1 getabi failed for {address}: {body.get('result') or body}"
        )
    abi = json.loads(body["result"])

    name = ""
    try:
        src_url = (
            f"{BLOCKSCOUT_V1}?module=contract&action=getsourcecode"
            f"&address={address}"
        )
        src_body = _get_json(src_url)
        if src_body.get("status") == "1":
            results = src_body.get("result") or []
            if results:
                name = results[0].get("ContractName") or ""
    except Exception:
        pass

    return {
        "abi_json": json.dumps(abi, separators=(",", ":")),
        "contract_name": name,
        "implementations": [],
    }


def fetch_blockscout(address: str) -> dict:
    """Fetch smart-contract metadata from Blockscout.

    Returns a dict with keys:
        abi_json       : str (JSON array as string)
        contract_name  : str
        implementations: list[dict] (may be empty)

    Tries the v2 REST API first. If it returns a 5xx error (which can
    happen for Vyper contracts), falls back to the v1 etherscan-compatible
    API (module=contract&action=getabi).

    Blockscout's public API blocks the default Python-urllib user-agent
    with 403 Forbidden, so we send a browser-like UA explicitly.
    """
    url = f"{BLOCKSCOUT_BASE}/{address.lower()}"
    try:
        body = _get_json(url)
    except urllib.error.HTTPError as e:
        if e.code >= 500:
            print(f"  v2 API returned HTTP {e.code}, falling back to v1 API …")
            return _fetch_blockscout_v1(address)
        raise SystemExit(
            f"Blockscout HTTP {e.code} for {address}: {e.reason}\n"
            "If this is a 403 or 429, retry in a minute or use --from-ch "
            "to read the ABI from the ClickHouse contracts_abi table "
            "(requires that `dbt run-operation fetch_and_insert_abi` "
            "has already run for this address)."
        )
    except (urllib.error.URLError, OSError, TimeoutError) as e:
        reason = getattr(e, "reason", str(e))
        print(f"  v2 API error ({reason}), falling back to v1 API …")
        return _fetch_blockscout_v1(address)

    abi = body.get("abi")
    if abi is None:
        raise SystemExit(f"Blockscout returned no `abi` for {address} (is the source code verified?)")

    return {
        "abi_json": json.dumps(abi, separators=(",", ":")),
        "contract_name": body.get("name") or "",
        "implementations": body.get("implementations") or [],
    }


def fetch_from_clickhouse(address: str) -> dict:
    """Read a previously-inserted ABI row from ClickHouse contracts_abi.

    Shells out to `dbt run-operation` because that reuses the project's
    existing credentials and profile config — no need to duplicate CH
    connection setup here. The operation prints the row on stdout as JSON.
    """
    # Use dbt show / show-table via an inline Jinja macro passed as --args.
    # Simpler approach: dbt compile a tiny query via show.
    query = (
        "SELECT contract_address, implementation_address, abi_json, "
        "contract_name, source, toString(updated_at) AS updated_at, chain "
        "FROM contracts_abi WHERE lower(replaceAll(contract_address, '0x', '')) = '"
        f"{address.lower().replace('0x', '')}"
        "' AND implementation_address = '' LIMIT 1"
    )
    cmd = ["dbt", "show", "--inline", query, "--output", "json"]
    try:
        result = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        raise SystemExit(
            f"dbt show failed: {e.stderr or e.stdout}\n"
            "Make sure `dbt run-operation fetch_and_insert_abi --args "
            f"'{{\"address\": \"{address}\"}}'` has been run first."
        )

    # dbt show --output json prints the rows in a structured block. Find
    # the first JSON line that parses as a dict-with-'rows' wrapper.
    rows = None
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict) and "rows" in parsed:
            rows = parsed["rows"]
            break
    if not rows:
        raise SystemExit(
            f"Couldn't parse any row from dbt show output. Raw:\n{result.stdout}"
        )
    row = rows[0]
    # dbt show returns a dict keyed by column name
    abi_json = row.get("abi_json") or ""
    if not abi_json or abi_json in ("[]", "{}"):
        raise SystemExit(f"ClickHouse row for {address} has no ABI")
    return {
        "abi_json": abi_json,
        "contract_name": row.get("contract_name") or "",
        "implementations": [],  # follow-up fetches aren't supported in --from-ch mode
    }


def read_csv(path: Path) -> tuple[list[str], list[list[str]]]:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)
    if header != ["contract_address", "implementation_address", "abi_json",
                  "contract_name", "source", "updated_at", "chain"]:
        raise SystemExit(f"Unexpected contracts_abi.csv header: {header}")
    # every row must have 7 fields
    bad = [(i, len(r)) for i, r in enumerate(rows, start=2) if len(r) != 7]
    if bad:
        raise SystemExit(f"Malformed rows in contracts_abi.csv (line, field count): {bad[:5]}")
    return header, rows


def write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow(header)
        w.writerows(rows)


def upsert_row(rows: list[list[str]], new_row: list[str], *, force: bool) -> str:
    """Insert or replace new_row in rows, keyed on
    (chain, contract_address, implementation_address).
    Returns one of: 'inserted', 'replaced', 'skipped_exists'.
    """
    key_addr = new_row[0].lower()
    key_impl = new_row[1].lower()
    key_chain = new_row[6].lower()
    for i, r in enumerate(rows):
        if r[0].lower() == key_addr and r[1].lower() == key_impl and r[6].lower() == key_chain:
            if not force:
                return "skipped_exists"
            rows[i] = new_row
            return "replaced"
    rows.append(new_row)
    return "inserted"


def run_regen(regen: bool) -> None:
    if not regen:
        return
    print("\n─── --regen: pushing CSV and regenerating signatures ───")
    # Every step is fail-fast; if one dies the remaining steps are skipped.
    steps = [
        ["dbt", "seed", "--select", "contracts_abi"],
        [sys.executable, str(REPO_ROOT / "scripts" / "signatures" / "signature_generator.py")],
        ["dbt", "seed", "--select", "event_signatures", "function_signatures"],
    ]
    for cmd in steps:
        print(f"\n$ {' '.join(cmd)}")
        subprocess.run(cmd, cwd=REPO_ROOT, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("address", help="0x-prefixed 40-hex contract address (case preserved)")
    parser.add_argument("--name", help="Override the contract_name field (default: from Blockscout)")
    parser.add_argument("--source", default="blockscout",
                        help="Value for the source column (default: blockscout)")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite the row if one already exists for this (address, impl) pair")
    parser.add_argument("--regen", action="store_true",
                        help="After writing the CSV, run `dbt seed contracts_abi`, "
                             "`signature_generator.py`, and `dbt seed event_signatures function_signatures`")
    parser.add_argument("--from-ch", dest="from_ch", action="store_true",
                        help="Read the ABI from the ClickHouse contracts_abi table instead "
                             "of fetching from Blockscout. Useful when the container has no "
                             "outbound HTTP access or Blockscout returns 403/429. Requires "
                             "that `dbt run-operation fetch_and_insert_abi` has already "
                             "run for this address so the row exists in ClickHouse.")
    parser.add_argument("--chain", default=DEFAULT_CHAIN, choices=sorted(BLOCKSCOUT_HOSTS),
                        help=f"Chain the contract lives on (default: {DEFAULT_CHAIN}). "
                             "Selects the Blockscout instance and is written to the "
                             "chain column of contracts_abi.csv.")
    args = parser.parse_args()

    global BLOCKSCOUT_BASE, BLOCKSCOUT_V1
    BLOCKSCOUT_BASE = f"{BLOCKSCOUT_HOSTS[args.chain]}/api/v2/smart-contracts"
    BLOCKSCOUT_V1 = f"{BLOCKSCOUT_HOSTS[args.chain]}/api"

    if not ADDRESS_RE.match(args.address):
        raise SystemExit(f"Invalid address: {args.address!r} (expected 0x + 40 hex chars)")

    if not CSV_PATH.exists():
        raise SystemExit(f"CSV not found: {CSV_PATH}")

    if args.from_ch:
        print(f"Reading {args.address} from ClickHouse contracts_abi table …")
        meta = fetch_from_clickhouse(args.address)
    else:
        print(f"Fetching {args.address} from Blockscout …")
        meta = fetch_blockscout(args.address)
    name = args.name or meta["contract_name"]
    if not name:
        raise SystemExit(
            "No contract name found — pass --name to override "
            "(Blockscout returned empty, or the ClickHouse row had no name)"
        )
    print(f"  name            : {name}")
    print(f"  abi size        : {len(meta['abi_json'])} bytes")
    print(f"  implementations : {len(meta['implementations'])} "
          + (f"(first: {meta['implementations'][0].get('address_hash')})"
             if meta["implementations"] else "(none)"))

    header, rows = read_csv(CSV_PATH)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # 1. The contract itself
    new_row = [args.address, "", meta["abi_json"], name, args.source, now, args.chain]
    status = upsert_row(rows, new_row, force=args.force)
    if status == "skipped_exists":
        print(f"\n{args.address} already exists in CSV and --force was not passed. "
              "No changes. Pass --force to overwrite.")
        return
    print(f"\n{status:9s}  {args.address:44s}  {name}")

    # 2. The implementation (if the contract is a proxy). We only
    #    follow the first implementation, matching the dbt macro.
    if meta["implementations"]:
        impl = meta["implementations"][0]
        impl_addr = impl.get("address_hash")
        impl_name = impl.get("name") or ""
        if impl_addr and ADDRESS_RE.match(impl_addr):
            print(f"Fetching implementation {impl_addr} from Blockscout …")
            impl_meta = fetch_blockscout(impl_addr)
            impl_new = [
                args.address,        # proxy address (as the contract_address)
                impl_addr,           # implementation_address
                impl_meta["abi_json"],
                impl_name or impl_meta["contract_name"],
                args.source,
                now,
                args.chain,
            ]
            impl_status = upsert_row(rows, impl_new, force=args.force)
            print(f"{impl_status:9s}  {args.address:44s}  (impl {impl_addr}  {impl_new[3]})")

    write_csv(CSV_PATH, header, rows)
    print(f"\nCSV now has {len(rows)} data rows. Wrote {CSV_PATH}")

    if not args.regen:
        print("\nNext steps:")
        print("  dbt seed --select contracts_abi")
        print("  python scripts/signatures/signature_generator.py")
        print("  dbt seed --select event_signatures function_signatures")
        print("\nOr re-run this script with --regen to chain all three.")
    else:
        run_regen(True)


if __name__ == "__main__":
    main()
