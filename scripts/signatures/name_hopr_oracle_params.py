#!/usr/bin/env python3
"""Name the unnamed HOPR oracle event params in seeds/contracts_abi.csv.

Why this patch is needed
------------------------
`decode_logs` builds its output with `mapFromArrays(param_names, param_values)`.
When an event's ABI params have empty names, every key collapses to '' and the
resulting Map has duplicate empty keys -- the values are all still there, in
order, but nothing downstream can address them by name. Two HOPR oracle events
hit this:

  HoprTicketPriceOracle.TicketPriceUpdated(uint256, uint256)
      Genuinely unnamed in the Solidity source
      (hoprnet/contracts ethereum/contracts/src/TicketPriceOracle.sol:8).

  HoprWinningProbabilityOracle.WinProbUpdated(uint56, uint56)
      Named in the source as `WinProbUpdated(WinProb oldWinProb, WinProb
      newWinProb)` (WinningProbabilityOracle.sol:16) and named in jura's
      verified ABI (0x69081e1a...), but the names are missing from dufour's
      verified ABI (0x7Eb8d762...). We restore the source's own names.

Evidence for the (old, new) ordering -- not an assumption
--------------------------------------------------------
Both contracts guard with `...MustNotBeSame()`, and in the decoded dufour
TicketPriceUpdated series each event's first value equals the previous event's
second value (0->100, 100->1e18, 1e18->1e17, 1e17->3e16, ...). That chain only
holds if the params are (old, new).

Scope: exactly the four (contract, event) pairs listed in CHANGES. Idempotent --
running twice is a no-op. After running:

    python scripts/signatures/signature_generator.py   # SIGNATURE_GEN_SOURCE=csv
    dbt seed --select contracts_abi event_signatures function_signatures
    dbt run --select contracts_hopr_TicketPriceOracle_events \
                     contracts_hopr_WinningProbabilityOracle_events --full-refresh
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

CSV_PATH = Path(__file__).resolve().parents[2] / "seeds" / "contracts_abi.csv"

# (contract_address_lower, event_name, [param names in ABI order])
CHANGES: list[tuple[str, str, list[str]]] = [
    ("0xca5656fe6f2d847aca32cf5f38e51d2054ca1273", "TicketPriceUpdated",
     ["oldTicketPrice", "newTicketPrice"]),          # dufour
    ("0xcd638cd10913971d6a6c058c4a4bbcf3029d1df3", "TicketPriceUpdated",
     ["oldTicketPrice", "newTicketPrice"]),          # jura
    ("0x7eb8d762fe794a108e568ad2097562cc5d3a1359", "WinProbUpdated",
     ["oldWinProb", "newWinProb"]),                  # dufour (jura already named)
    ("0x69081e1a1419c5265b66ee89081919b9fb56ae0e", "WinProbUpdated",
     ["oldWinProb", "newWinProb"]),                  # jura (no-op, kept for symmetry)
]


def main() -> int:
    csv.field_size_limit(10**9)
    with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        rows = list(reader)
    if not headers:
        print(f"error: no header in {CSV_PATH}", file=sys.stderr)
        return 2

    renamed = 0
    for addr, event_name, names in CHANGES:
        for row in rows:
            if row["contract_address"].lower() != addr:
                continue
            abi = json.loads(row["abi_json"])
            changed = False
            for entry in abi:
                if entry.get("type") != "event" or entry.get("name") != event_name:
                    continue
                inputs = entry.get("inputs", [])
                if len(inputs) != len(names):
                    print(f"error: {addr} {event_name} has {len(inputs)} params, "
                          f"expected {len(names)} -- refusing to guess", file=sys.stderr)
                    return 1
                for inp, new_name in zip(inputs, names):
                    if not inp.get("name"):
                        inp["name"] = new_name
                        changed = True
                        renamed += 1
            if changed:
                row["abi_json"] = json.dumps(abi, separators=(",", ":"))

    if renamed == 0:
        print("No unnamed params found -- already patched, nothing to do.")
        return 0

    with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Named {renamed} previously-unnamed param(s). Wrote {CSV_PATH}")
    print("Next: regenerate signatures, reseed, then --full-refresh the two oracle models.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
