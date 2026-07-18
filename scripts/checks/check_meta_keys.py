#!/usr/bin/env python3
"""CI guard: reject schema-generator noise in model meta.

DENYLIST gate, deliberately NOT a whitelist: model meta carries real runtime
and privacy contracts (owner, authoritative, full_refresh, inference_notes,
agent, api.exclude_from_api, privacy_tier, expose_to_mcp, ...) that a naive
whitelist would reject wholesale. The only keys banned here are the schema
generator's bookkeeping noise, which carries no meaning for any consumer and
churns diffs:

    generated_by, _generated_at, _generated_fields

(the same set scripts/cleanup_schema_meta.py migrated away once already).

For visibility — not enforcement — the gate also enumerates every top-level
meta key in use with its model count, so contract drift is at least seen.

Reads target/manifest.json (run `dbt parse` first).
Usage: python scripts/checks/check_meta_keys.py [--manifest target/manifest.json]
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys
from collections import Counter

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST = REPO_ROOT / "target" / "manifest.json"

PROJECT_PACKAGE = "gnosis_dbt"

# Keep in sync with scripts/cleanup_schema_meta.py KEYS_TO_REMOVE.
NOISE_KEYS = {"generated_by", "_generated_at", "_generated_fields"}


def _merged_meta(node: dict) -> dict:
    cfg_meta = (node.get("config") or {}).get("meta") or {}
    top_meta = node.get("meta") or {}
    return {**cfg_meta, **top_meta}


def find_violations(manifest: dict):
    """Returns (violations, key_counts): violations as sorted (model_name, key)
    pairs; key_counts a Counter of every top-level meta key in use."""
    violations = []
    key_counts: Counter = Counter()
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        if node.get("package_name") != PROJECT_PACKAGE:
            continue
        meta = _merged_meta(node)
        for key in meta:
            key_counts[key] += 1
            if key in NOISE_KEYS:
                violations.append((node["name"], key))
    return sorted(violations), key_counts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    args = ap.parse_args()

    manifest_path = pathlib.Path(args.manifest)
    if not manifest_path.exists():
        print(f"ERROR: manifest not found at {manifest_path}. Run `dbt parse` first.")
        return 2

    manifest = json.loads(manifest_path.read_text())
    violations, key_counts = find_violations(manifest)

    if key_counts:
        keys = ", ".join(f"{k}({n})" for k, n in sorted(key_counts.items()))
        print(f"meta keys in use: {keys}")

    if violations:
        print(f"\nMeta-key policy: {len(violations)} generator-noise key(s):\n")
        for name, key in violations:
            print(f"  {name}: {key}")
        print("\nStrip them (see scripts/cleanup_schema_meta.py) — they are schema-gen "
              "bookkeeping, not contracts.")
        return 1

    print("Meta-key policy OK: no generator-noise keys in model meta.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
