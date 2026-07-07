#!/usr/bin/env python3
"""CI ratchet: per-module documentation/ownership coverage floors.

Reads target/manifest.json (run `dbt parse` first), computes per-module
coverage for first-party models (package_name == gnosis_dbt):

  - model_description_pct : models with a non-empty description
  - column_description_pct: columns with a non-empty description
  - owner_pct             : models with meta.owner

and compares against the committed floors in
scripts/checks/doc_coverage_floors.json. Any metric that drops below its
floor fails the build (exit 1) — coverage can only ratchet up.

After a docs wave lands, re-baseline with:
    python scripts/checks/check_doc_coverage.py --update-floors

Usage: dbt parse && python scripts/checks/check_doc_coverage.py
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
from collections import defaultdict

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST = REPO_ROOT / "target" / "manifest.json"
FLOORS = pathlib.Path(__file__).resolve().parent / "doc_coverage_floors.json"
PROJECT_PACKAGE = "gnosis_dbt"
METRICS = ("model_description_pct", "column_description_pct", "owner_pct")


def compute_coverage(manifest: dict) -> dict[str, dict[str, float]]:
    stats: dict[str, dict[str, int]] = defaultdict(
        lambda: {"models": 0, "described": 0, "cols": 0, "col_desc": 0, "owned": 0}
    )
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        if node.get("package_name") != PROJECT_PACKAGE:
            continue
        module = (node.get("fqn") or ["", "unknown"])[1]
        s = stats[module]
        s["models"] += 1
        if (node.get("description") or "").strip():
            s["described"] += 1
        meta = node.get("meta") or (node.get("config") or {}).get("meta") or {}
        if meta.get("owner"):
            s["owned"] += 1
        for col in (node.get("columns") or {}).values():
            s["cols"] += 1
            if (col.get("description") or "").strip():
                s["col_desc"] += 1

    def pct(n: int, d: int) -> float:
        return round(100.0 * n / d, 1) if d else 100.0

    return {
        module: {
            "model_description_pct": pct(s["described"], s["models"]),
            "column_description_pct": pct(s["col_desc"], s["cols"]),
            "owner_pct": pct(s["owned"], s["models"]),
        }
        for module, s in sorted(stats.items())
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    ap.add_argument("--update-floors", action="store_true",
                    help="Re-baseline floors to current coverage (run after a docs wave lands)")
    args = ap.parse_args()

    manifest_path = pathlib.Path(args.manifest)
    if not manifest_path.exists():
        print(f"ERROR: manifest not found at {manifest_path}. Run `dbt parse` first.")
        return 2

    coverage = compute_coverage(json.loads(manifest_path.read_text()))

    if args.update_floors:
        FLOORS.write_text(json.dumps(coverage, indent=2) + "\n")
        print(f"floors re-baselined to current coverage for {len(coverage)} modules -> {FLOORS.name}")
        return 0

    if not FLOORS.exists():
        print(f"ERROR: no floors file at {FLOORS}. Run with --update-floors to baseline.")
        return 2

    floors = json.loads(FLOORS.read_text())
    failures = []
    for module, metric_floors in floors.items():
        current = coverage.get(module)
        if current is None:
            # Module removed/renamed — surface it so the floors file gets pruned.
            failures.append(f"{module}: present in floors but absent from manifest — prune the entry")
            continue
        for metric in METRICS:
            floor = metric_floors.get(metric)
            if floor is None:
                continue
            if current[metric] < floor:
                failures.append(
                    f"{module}.{metric}: {current[metric]}% < floor {floor}%"
                )

    if failures:
        print(f"Doc-coverage ratchet: {len(failures)} regression(s):\n")
        for f in sorted(failures):
            print("  " + f)
        print("\nCoverage can only ratchet up. Restore the docs, or (after a deliberate "
              "removal) re-baseline with --update-floors.")
        return 1

    print(f"Doc-coverage ratchet OK: {len(floors)} modules at or above their floors.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
