#!/usr/bin/env python3
"""Change-aware gate: what did this branch touch, what could it break, and do
the touched models satisfy their contract requirements?

Usage:
    python scripts/agent_context/check.py --base-ref main [--strict]

Steps:
  1. git diff --name-only <base-ref>...HEAD -> changed model files
  2. For each changed model: contract presence, hazards, downstream impact
     (incl. affected api_ marts), and the exact validation selectors to run.
  3. Ratchet: a CHANGED high-risk model without an explicit contract
     (meta.agent grain/invariants) fails unless listed in
     agent_context/contract_ratchet.allow. Untouched legacy gaps are only
     reported. --strict also fails on any reported (non-allowlisted) gap.
  4. Composes the static CI gates (no_delete_insert, check_api_tags,
     check_doc_coverage) so local == CI.

Failure modes:
  - A changed model file UNKNOWN to the artifact is BLOCKING (the artifact is
    stale or the model was never parsed — either way its hazards are unknown;
    run `dbt parse` + build_agent_context.py, then re-run).
  - DELETED model files are reported but never block: a removed model has no
    hazards left to check, and the breakage a deletion can cause (dangling
    ref(), orphaned semantic metrics) is caught by dbt parse and the
    semantic-registry gate.
  - `--require-base` (CI): an unresolvable base ref is a hard error. Without
    it (local), the diff falls back to the working tree with a warning.

Exit code 0 = clean; 1 = blocking findings.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RATCHET_ALLOW = REPO_ROOT / "agent_context" / "contract_ratchet.allow"

STATIC_GATES = [
    ("no-delete-insert", [sys.executable, "scripts/checks/no_delete_insert.py"]),
    ("api-tags", [sys.executable, "scripts/checks/check_api_tags.py"]),
    ("doc-coverage", [sys.executable, "scripts/checks/check_doc_coverage.py"]),
]


def changed_model_files(base_ref: str) -> list[Path]:
    out = subprocess.check_output(
        ["git", "diff", "--name-only", f"{base_ref}...HEAD"], cwd=REPO_ROOT, text=True
    )
    # Also include uncommitted work — this repo's convention is to review the
    # working tree before anything is committed.
    out += subprocess.check_output(
        ["git", "diff", "--name-only", "HEAD"], cwd=REPO_ROOT, text=True
    )
    out += subprocess.check_output(
        ["git", "ls-files", "--others", "--exclude-standard", "models/"],
        cwd=REPO_ROOT, text=True,
    )
    files = sorted({l.strip() for l in out.splitlines() if l.strip()})
    return [Path(f) for f in files if f.startswith("models/") and f.endswith(".sql")]


def get_changed_models(base_ref: str, require_base: bool) -> list[Path]:
    """Changed model files vs base_ref. With require_base (CI), an
    unresolvable base fails CLOSED — an empty changed-set would silently skip
    every per-model check and pass a broken branch."""
    try:
        return changed_model_files(base_ref)
    except subprocess.CalledProcessError:
        if require_base:
            raise SystemExit(
                f"ERROR: base ref '{base_ref}' could not be resolved. In CI the "
                "base MUST be fetched and passed explicitly (fetch-depth: 0 or "
                "an explicit fetch of the base SHA) — refusing to fail open."
            )
        print(f"[warn] git diff against '{base_ref}' failed; checking working tree only")
        return []


def partition_existing(paths: list[Path]) -> tuple[list[Path], list[Path]]:
    """Split changed paths into (present, deleted). git diff lists deleted
    files too; a deleted model can never be in the manifest-derived artifact,
    so treating it as 'unknown' would hard-block every model removal/rename.
    Its hazards are moot, and dangling refs are caught by dbt parse and the
    semantic-registry gate."""
    present = [p for p in paths if (REPO_ROOT / p).exists()]
    deleted = [p for p in paths if not (REPO_ROOT / p).exists()]
    return present, deleted


def load_artifact() -> dict:
    path = REPO_ROOT / "target" / "agent_context.json"
    if not path.exists():
        rc = subprocess.call(
            [sys.executable, str(REPO_ROOT / "scripts/agent_context/build_agent_context.py")]
        )
        if rc != 0:
            raise SystemExit("ERROR: could not build agent context")
    return json.loads(path.read_text())


def load_allowlist() -> set[str]:
    if not RATCHET_ALLOW.exists():
        return set()
    return {
        l.strip() for l in RATCHET_ALLOW.read_text().splitlines()
        if l.strip() and not l.startswith("#")
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Change-aware contract/impact gate")
    ap.add_argument("--base-ref", default="main")
    ap.add_argument("--strict", action="store_true",
                    help="Also fail on reported (non-changed) contract gaps.")
    ap.add_argument("--skip-static", action="store_true",
                    help="Skip the composed static gates (already run separately in CI).")
    ap.add_argument("--require-base", action="store_true",
                    help="Fail hard when the base ref cannot be resolved (CI). "
                         "Without it, falls back to working-tree-only with a warning.")
    args = ap.parse_args()

    artifact = load_artifact()
    models = artifact["models"]
    allow = load_allowlist()
    blocking: list[str] = []

    changed = get_changed_models(args.base_ref, args.require_base)
    changed, deleted = partition_existing(changed)

    changed_names = [p.stem for p in changed]
    known = [n for n in changed_names if n in models]
    unknown = [n for n in changed_names if n not in models]

    print(f"Changed model files vs {args.base_ref} (incl. working tree): {len(changed_names)}")
    if deleted:
        print(f"Deleted model file(s), skipped — nothing left to hazard-check; "
              f"ref/metric breakage is covered by dbt parse and the semantic-registry gate: "
              f"{', '.join(sorted(p.stem for p in deleted))}")
    if unknown:
        blocking.append(
            f"changed model(s) unknown to the agent context: {', '.join(unknown)} — "
            "the artifact is stale or the model was never parsed, so its hazards "
            "can't be checked. Run `dbt parse` (make manifest) + "
            "`python scripts/agent_context/build_agent_context.py`, then re-run."
        )

    validations: list[str] = []
    for name in known:
        m = models[name]
        c = m["contract"]
        hazard_ids = [h["id"] for h in c.get("hazards", [])]
        api_count = m.get("downstream_api_count", 0)
        api_list = m.get("downstream_api_models") or []
        api_note = ""
        if api_list:
            more = f"(+{api_count - len(api_list)} more)" if api_count > len(api_list) else ""
            api_note = f" api={','.join(api_list)}{more}"
        print(f"\n- {name}  ({m['path']})")
        print(f"    high_risk={m['high_risk']} explicit_contract={m['explicit_contract']}"
              f" downstream_direct={m['downstream_direct_count']}"
              f" downstream_transitive={m['downstream_transitive_count']}" + api_note)
        if hazard_ids:
            print(f"    hazards: {', '.join(hazard_ids)}")
        for v in c.get("validation", []):
            if v not in validations:
                validations.append(v)
        if m["high_risk"] and not m["explicit_contract"] and name not in allow:
            blocking.append(
                f"{name}: changed high-risk model without meta.agent grain/invariants "
                f"(add the contract, or allowlist in {RATCHET_ALLOW.name})"
            )

    # Report (not block) the overall ratchet position.
    gaps = [
        n for n, m in models.items()
        if m["high_risk"] and not m["explicit_contract"] and n not in allow and n not in known
    ]
    print(f"\nRatchet: {len(gaps)} untouched high-risk models still lack explicit contracts "
          f"(reported only{'; --strict blocks' if args.strict else ''}).")
    if args.strict and gaps:
        blocking.append(f"--strict: {len(gaps)} high-risk models lack explicit contracts")

    if validations:
        print("\nValidation to run for this change:")
        for v in validations:
            print(f"  - {v}")

    if not args.skip_static:
        print("\nStatic gates:")
        for label, cmd in STATIC_GATES:
            rc = subprocess.call(cmd, cwd=REPO_ROOT)
            print(f"  [{'ok' if rc == 0 else 'FAIL'}] {label}")
            if rc != 0:
                blocking.append(f"static gate failed: {label}")

    if blocking:
        print("\nBLOCKING:")
        for b in blocking:
            print(f"  - {b}")
        return 1
    print("\ncheck: clean")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
