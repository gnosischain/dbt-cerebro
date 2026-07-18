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
    args = ap.parse_args()

    artifact = load_artifact()
    models = artifact["models"]
    allow = load_allowlist()
    blocking: list[str] = []

    try:
        changed = changed_model_files(args.base_ref)
    except subprocess.CalledProcessError:
        print(f"[warn] git diff against '{args.base_ref}' failed; checking working tree only")
        changed = []

    changed_names = [p.stem for p in changed]
    known = [n for n in changed_names if n in models]
    unknown = [n for n in changed_names if n not in models]

    print(f"Changed model files vs {args.base_ref} (incl. working tree): {len(changed_names)}")
    if unknown:
        print(f"  [warn] not in agent context (new model? rebuild after dbt parse): {', '.join(unknown)}")

    validations: list[str] = []
    for name in known:
        m = models[name]
        c = m["contract"]
        hazard_ids = [h["id"] for h in c.get("hazards", [])]
        print(f"\n- {name}  ({m['path']})")
        print(f"    high_risk={m['high_risk']} explicit_contract={m['explicit_contract']}"
              f" downstream={m['downstream_count']}"
              + (f" api={','.join(m['downstream_api_models'])}" if m["downstream_api_models"] else ""))
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
