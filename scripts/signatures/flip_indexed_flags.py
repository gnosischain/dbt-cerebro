#!/usr/bin/env python3
"""One-off: flip specific `indexed` flags in seeds/contracts_abi.csv where
the bundled ABI disagrees with the on-chain Solidity source.

Currently scoped to Gnosis Pay unblock — the two RolesMod_v2 events that
were wrongly declaring `module` as indexed. See the investigation at
/Users/hugser/.claude/plans/enumerated-meandering-boot.md for the full
evidence (Zodiac Roles v2 source + 654/654 NULL topic1 in raw data).

Idempotent: running twice is a no-op. Reports the number of flags flipped.

Usage:
    python scripts/signatures/flip_indexed_flags.py
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

CSV_PATH = Path(__file__).resolve().parents[2] / "seeds" / "contracts_abi.csv"

# (contract_name, event_name, param_name, target_indexed_value)
CHANGES: list[tuple[str, str, str, bool]] = [
    ("RolesMod_v2", "AssignRoles", "module", False),
    ("RolesMod_v2", "SetDefaultRole", "module", False),
]


def main() -> int:
    if not CSV_PATH.exists():
        print(f"error: {CSV_PATH} not found", file=sys.stderr)
        return 1

    with CSV_PATH.open() as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        print("error: empty CSV", file=sys.stderr)
        return 1

    header, body = rows[0], rows[1:]
    try:
        name_col = header.index("contract_name")
        abi_col = header.index("abi_json")
    except ValueError as e:
        print(f"error: missing expected column: {e}", file=sys.stderr)
        return 1

    flipped = 0
    touched_rows = 0
    for row in body:
        targets = [c for c in CHANGES if c[0] == row[name_col]]
        if not targets:
            continue
        try:
            abi = json.loads(row[abi_col])
        except json.JSONDecodeError as e:
            print(
                f"warning: could not parse ABI for {row[name_col]}: {e}",
                file=sys.stderr,
            )
            continue

        row_flipped = 0
        for _, event_name, param_name, target in targets:
            for entry in abi:
                if entry.get("type") != "event" or entry.get("name") != event_name:
                    continue
                for inp in entry.get("inputs", []):
                    if inp.get("name") != param_name:
                        continue
                    if inp.get("indexed") != target:
                        inp["indexed"] = target
                        row_flipped += 1

        if row_flipped:
            row[abi_col] = json.dumps(abi, separators=(",", ":"))
            touched_rows += 1
            flipped += row_flipped

    with CSV_PATH.open("w", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(header)
        writer.writerows(body)

    expected = len(CHANGES)
    print(f"flipped {flipped} indexed flags across {touched_rows} row(s) (expected {expected})")
    if flipped == 0:
        print("note: nothing was flipped — either already applied or targets not found")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
