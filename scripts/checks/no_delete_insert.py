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
  5. [stage_var_not_read] has a meta.full_refresh stage passing a dimension
     INCLUDE filter (symbol/slice) that the model body never reads via var()
     -> the filter is inert: dbt ignores unknown vars, so the run silently
        covers ALL members. On an append strategy that is an exact second copy
        of every existing member in the window.
  6. [staged_scoped_include_overwrite] has such a stage AND a literal/inherited
     insert_overwrite strategy
     -> the filtered result set REPLACEs whole partitions, wiping every other
        member for those partitions.
     Rules 5-6 are the two halves of docs/lessons/stage-vars-scope-illusion.md;
     only their intersection (body reads the var AND scoped branch is append) is
     safe. Both are narrower than rule 4 on purpose: rule 4's grandfathered
     allowlist entries carry symbol_exclude, which is not an include filter.

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

# Stage vars that NARROW a run to a subset of dimension members. Exclude-style
# vars (symbol_exclude) are deliberately absent: they widen rather than narrow,
# so they neither leave other members unbuilt nor shrink an overwrite window.
INCLUDE_FILTER_VARS = {"symbol", "slice"}


def load_allowlist() -> set:
    if not ALLOWLIST.exists():
        return set()
    out = set()
    for line in ALLOWLIST.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            out.add(line)
    return out


def _stages(meta_full_refresh) -> list:
    """The stage dicts of a meta.full_refresh block, tolerant of odd shapes."""
    if not isinstance(meta_full_refresh, dict):
        return []
    stages = meta_full_refresh.get("stages") or []
    if not isinstance(stages, list):
        return []
    return [s for s in stages if isinstance(s, dict)]


def _reads_var(raw_code: str, var_name: str) -> bool:
    """True if the model body reads var('<name>') / var("<name>")."""
    return f"var('{var_name}'" in raw_code or f'var("{var_name}"' in raw_code


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
        meta_fr = _merged_meta(node).get("full_refresh")
        if meta_fr:
            raw_code = node.get("raw_code") or ""
            info = analyze_strategy(raw_code)
            inherited_overwrite = (
                info["literal"] == "insert_overwrite"
                or (not info["assigned"] and strategy == "insert_overwrite")
            )
            if inherited_overwrite:
                check(uid, "staged_literal_overwrite",
                      "meta.full_refresh stages + literal/inherited insert_overwrite: "
                      "staged batches REPLACE partitions and keep only the last stage. "
                      "Use ('append' if start_month else 'insert_overwrite').")
            elif info["expression"] and info["scoped_append"] is False:
                check(uid, "staged_scoped_branch",
                      f"staged model's scoped (start_month) branch resolves to "
                      f"'{info['scoped_branch']}', not 'append' — scoped batches must "
                      "append, never overwrite/mutate.")

            # Dimension-scoping rules. A stage's vars are inert metadata until the
            # model body reads them, and honouring them on insert_overwrite wipes
            # every other dimension member.
            # See docs/lessons/stage-vars-scope-illusion.md.
            scoped_vars = set()
            for stage in _stages(meta_fr):
                for var_name in (stage.get("vars") or {}):
                    if var_name in INCLUDE_FILTER_VARS:
                        scoped_vars.add(var_name)

            if scoped_vars:
                unread = sorted(v for v in scoped_vars if not _reads_var(raw_code, v))
                if unread:
                    check(uid, "stage_var_not_read",
                          f"meta.full_refresh stage passes include filter(s) "
                          f"{', '.join(unread)} that the model body never reads via "
                          f"var(). dbt ignores unknown vars, so the run silently covers "
                          f"ALL members — on an append strategy that duplicates every "
                          f"existing member in the window. Wire up "
                          f"macros/db/symbol_filter.sql, or drop the stage var.")
                if inherited_overwrite:
                    check(uid, "staged_scoped_include_overwrite",
                          f"meta.full_refresh stage carries include filter(s) "
                          f"{', '.join(sorted(scoped_vars))} on a literal/inherited "
                          f"insert_overwrite model: the filtered result set REPLACEs "
                          f"whole partitions, wiping every other member for those "
                          f"partitions. Re-run such months unfiltered instead.")

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
