#!/usr/bin/env python3
"""CI guard: enforce the zero-duplicate, mutation-free incremental policy.

Walks target/manifest.json and fails if any incremental model:

  1. resolves to incremental_strategy='delete+insert'
     -> banned: emits ALTER ... DELETE mutations on ClickHouse.
  2. uses incremental_strategy='insert_overwrite' without a partition_by
     -> insert_overwrite needs a partition to REPLACE; without one it is a no-op
        or replaces the whole table.
  3. uses incremental_strategy='append' without the 'microbatch' tag
     -> append is only duplicate-safe behind a strict no-overlap watermark; the
        microbatch tag is how a model opts into that path. A bare append model
        risks overlap-append duplicates.

A transient allowlist (scripts/checks/no_delete_insert.allow, one unique_id per
line, '#' comments allowed) suppresses not-yet-migrated models during rollout.
Shrink it per wave; delete it when empty.

Usage:
    dbt parse            # refresh target/manifest.json first
    python scripts/checks/no_delete_insert.py [--manifest target/manifest.json]
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST = REPO_ROOT / "target" / "manifest.json"
ALLOWLIST = pathlib.Path(__file__).resolve().parent / "no_delete_insert.allow"


def load_allowlist() -> set[str]:
    if not ALLOWLIST.exists():
        return set()
    out = set()
    for line in ALLOWLIST.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            out.add(line)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    args = ap.parse_args()

    manifest_path = pathlib.Path(args.manifest)
    if not manifest_path.exists():
        print(f"ERROR: manifest not found at {manifest_path}. Run `dbt parse` first.")
        return 2

    manifest = json.loads(manifest_path.read_text())
    allow = load_allowlist()

    violations: list[tuple[str, str]] = []
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        config = node.get("config", {}) or {}
        if config.get("materialized") != "incremental":
            continue

        uid = node["unique_id"]
        if uid in allow:
            continue

        strategy = config.get("incremental_strategy")
        partition_by = config.get("partition_by")
        tags = config.get("tags") or []

        if strategy == "delete+insert":
            violations.append((uid, "delete+insert is banned (emits ALTER DELETE mutations)"))
        elif strategy == "insert_overwrite" and not partition_by:
            violations.append((uid, "insert_overwrite requires a partition_by"))
        elif strategy == "append" and "microbatch" not in tags:
            violations.append((uid, "append requires the 'microbatch' tag (no-overlap watermark)"))

    if violations:
        print("Incremental policy violations:\n")
        for uid, why in sorted(violations):
            print(f"  {uid}: {why}")
        print(f"\n{len(violations)} violation(s). See scripts/checks/no_delete_insert.py for the policy.")
        return 1

    print("Incremental policy OK: no delete+insert, all insert_overwrite partitioned, all append tagged microbatch.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
