#!/usr/bin/env python3
"""Classify failed dbt nodes from stashed per-batch run_results.json files.

Reads every *.json under --stash-dir, inspects each node with status == "error",
and partitions the unique_ids into TRANSIENT (retry-worthy ClickHouse errors)
vs PERMANENT (logic/SQL bugs). Emits two lines on stdout consumable by bash:

  TRANSIENT=<space-separated unique_ids>
  PERMANENT=<space-separated unique_ids>

Always exits 0 so the caller can read both lists; an empty stash-dir yields two
empty assignments.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


TRANSIENT_RE = re.compile(
    r"Code:\s*(?:241|159|209|210)\b"
    r"|MEMORY_LIMIT_EXCEEDED"
    r"|TIMEOUT_EXCEEDED"
    r"|SOCKET_TIMEOUT"
    r"|NETWORK_ERROR",
    re.IGNORECASE,
)


def classify_run_results(path: Path) -> tuple[set[str], set[str]]:
    transient: set[str] = set()
    permanent: set[str] = set()
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[classify] skipping unreadable {path}: {exc}", file=sys.stderr)
        return transient, permanent

    for result in data.get("results", []):
        if result.get("status") != "error":
            continue
        unique_id = result.get("unique_id")
        if not unique_id:
            continue
        message = result.get("message") or ""
        if TRANSIENT_RE.search(message):
            transient.add(unique_id)
        else:
            permanent.add(unique_id)
    return transient, permanent


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stash-dir",
        type=Path,
        required=True,
        help="Directory containing per-batch run_results.json copies.",
    )
    args = parser.parse_args()

    all_transient: set[str] = set()
    all_permanent: set[str] = set()

    if args.stash_dir.is_dir():
        for path in sorted(args.stash_dir.glob("*.json")):
            t, p = classify_run_results(path)
            all_transient |= t
            all_permanent |= p

    # A node that failed permanently in one batch and transiently in another
    # (edge case: same node appears twice) is treated as permanent — a real
    # bug shouldn't be masked by a concurrent transient flake.
    all_transient -= all_permanent

    print(f"TRANSIENT={' '.join(sorted(all_transient))}")
    print(f"PERMANENT={' '.join(sorted(all_permanent))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
