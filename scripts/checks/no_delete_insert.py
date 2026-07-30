#!/usr/bin/env python3
"""CI guard: enforce the zero-duplicate, mutation-free incremental policy.

Scope: FIRST-PARTY models only (package_name == 'gnosis_dbt'). Vendored dbt
packages (e.g. Elementary) ship their own materialization strategy that we do
not control and must not edit — `dbt deps` would clobber any change — so they are
exempt from this policy. Without this scoping, enabling a package like Elementary
would fail the check on its internal append/incremental models (e.g.
elementary.test_result_rows).

Walks target/manifest.json and fails if any FIRST-PARTY incremental model:

  1. [delete_insert] resolves to incremental_strategy='delete+insert'
     -> banned: emits ALTER ... DELETE mutations on ClickHouse.
  2. [overwrite_no_partition] uses insert_overwrite without a partition_by
     -> insert_overwrite needs a partition to REPLACE; without one it is a no-op
        or replaces the whole table.
  3. [append_no_microbatch] uses append without the 'microbatch' tag
     -> append is only duplicate-safe behind a strict no-overlap watermark; the
        microbatch tag is how a model opts into that path. A bare append model
        risks overlap-append duplicates.
  4. [staged_literal_overwrite / staged_scoped_branch] has meta.full_refresh
     stages AND its RAW code strategy is a literal insert_overwrite (or an
     expression whose scoped start_month branch is not 'append')
     -> staged batches on insert_overwrite REPLACE whole partitions and leave
        only the last stage (docs/lessons/staged-insert-overwrite-wipe.md).
        The safe pattern is
            incremental_strategy=('append' if start_month else 'insert_overwrite')
        NOTE: raw code is authoritative here — the manifest's resolved value
        collapses that expression to its default branch, so resolved config
        CANNOT distinguish the safe pattern from a dangerous literal.

Allowlist (scripts/checks/no_delete_insert.allow): one entry per line,
'#' comments; either a bare unique_id (exempts the model from ALL rules) or
unique_id::rule (exempts one rule). SHRINK-ONLY: an entry that suppressed
nothing this run means the violation is fixed — the stale line fails the build
until deleted.

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
sys.path.insert(0, str(REPO_ROOT / "scripts" / "agent_context"))
try:
    from scripts.agent_context.strategy import analyze_strategy  # package import (pytest)
except ImportError:
    from strategy import analyze_strategy  # script import (path inserted above)

DEFAULT_MANIFEST = REPO_ROOT / "target" / "manifest.json"
ALLOWLIST = pathlib.Path(__file__).resolve().parent / "no_delete_insert.allow"

# The policy governs only this project's own models. Vendored packages (e.g.
# Elementary) manage their own materialization and are not ours to migrate.
PROJECT_PACKAGE = "gnosis_dbt"


def load_allowlist() -> set:
    if not ALLOWLIST.exists():
        return set()
    out = set()
    for line in ALLOWLIST.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            out.add(line)
    return out


def _merged_meta(node: dict) -> dict:
    cfg_meta = (node.get("config") or {}).get("meta") or {}
    top_meta = node.get("meta") or {}
    return {**cfg_meta, **top_meta}


def find_violations(manifest: dict, allow: set):
    """Returns (violations, used_allow): violations as (unique_id, rule, msg);
    used_allow = the allow entries that suppressed something this run."""
    violations = []
    used_allow = set()

    def check(uid: str, rule: str, msg: str) -> None:
        for key in (uid, f"{uid}::{rule}"):
            if key in allow:
                used_allow.add(key)
                return
        violations.append((uid, rule, msg))

    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        if node.get("package_name") != PROJECT_PACKAGE:
            continue
        config = node.get("config", {}) or {}
        if config.get("materialized") != "incremental":
            continue

        uid = node["unique_id"]
        strategy = config.get("incremental_strategy")
        partition_by = config.get("partition_by")
        tags = config.get("tags") or []

        if strategy == "delete+insert":
            check(uid, "delete_insert",
                  "delete+insert is banned (emits ALTER DELETE mutations)")
        elif strategy == "insert_overwrite" and not partition_by:
            check(uid, "overwrite_no_partition",
                  "insert_overwrite requires a partition_by")
        elif strategy == "append" and "microbatch" not in tags:
            check(uid, "append_no_microbatch",
                  "append requires the 'microbatch' tag (no-overlap watermark)")

        # Staged-strategy rule: raw code is authoritative (resolved config
        # collapsed any expression to its default branch at parse time).
        if _merged_meta(node).get("full_refresh"):
            info = analyze_strategy(node.get("raw_code") or "")
            if info["literal"] == "insert_overwrite" or (
                not info["assigned"] and strategy == "insert_overwrite"
            ):
                check(uid, "staged_literal_overwrite",
                      "meta.full_refresh stages + literal/inherited insert_overwrite: "
                      "staged batches REPLACE partitions and keep only the last stage. "
                      "Use ('append' if start_month else 'insert_overwrite').")
            elif info["expression"] and info["scoped_append"] is False:
                check(uid, "staged_scoped_branch",
                      f"staged model's scoped (start_month) branch resolves to "
                      f"'{info['scoped_branch']}', not 'append' — scoped batches must "
                      "append, never overwrite/mutate.")

    return violations, used_allow


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
    violations, used_allow = find_violations(manifest, allow)

    if violations:
        print("Incremental policy violations:\n")
        for uid, rule, why in sorted(violations):
            print(f"  {uid}  [{rule}]  {why}")
        print(f"\n{len(violations)} violation(s). See scripts/checks/no_delete_insert.py "
              "for the policy (allowlist entries may be rule-scoped: unique_id::rule).")
        return 1

    # Shrink-only ratchet: an allow entry that suppressed nothing is FIXED —
    # force its removal so the exemption backlog can only go down.
    stale = allow - used_allow
    if stale:
        print(f"Incremental policy: {len(stale)} STALE allowlist entr(ies) — the "
              "violation is fixed; delete these lines from "
              f"{ALLOWLIST.relative_to(REPO_ROOT)}:\n")
        for entry in sorted(stale):
            print("  " + entry)
        return 1

    if used_allow:
        print(f"Incremental policy OK — but {len(used_allow)} grandfathered "
              f"exemption(s) remain in {ALLOWLIST.name} (these models still "
              "violate the policy; the allowlist is shrink-only).")
    else:
        print("Incremental policy OK: no delete+insert, all insert_overwrite "
              "partitioned, all append tagged microbatch, staged strategies safe.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
