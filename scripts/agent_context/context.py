#!/usr/bin/env python3
"""Print a bounded change packet for a model — contract, hazards, lineage,
validation — so any agent (or human) sees a model's failure modes BEFORE
touching it.

Usage:
    python scripts/agent_context/context.py --select <model> [--task build|fix|backfill|review]

Reads target/agent_context.json (auto-builds it from the manifest if missing
or stale). The packet is intentionally short: it links lessons rather than
inlining them.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

TASK_GUIDANCE = {
    "build": [
        "Build inputs first, then seed with `dbt run --full-refresh -s <model>` — a green",
        "run on an empty incremental stays empty forever (never-seeded-incremental).",
        "Schema/tags/meta per AGENTS.md; author semantic/ if it's a mart.",
    ],
    "fix": [
        "Check docs/lessons/INDEX.md for a matching mistake class before diagnosing.",
        "Verify against ground truth (chain/raw source), not the model itself.",
    ],
    "backfill": [
        "Classify downstreams FIRST: cumulative (reads {{ this }}) need history",
        "backfilled chronologically BEFORE they advance; stateless can wait.",
        "Pick the lever from the AGENTS.md decision table — gap_window_refresh.py for",
        "backfilled months in decode chains; staged refresh.py for full history.",
        "Check for pending refresh state (target/refresh_state/) before starting.",
    ],
    "review": [
        "Check the diff against this model's hazards and the non-negotiable rules in",
        "AGENTS.md (strategy/partition grain, hooks pairing, meta/tag contract).",
    ],
}


def ensure_artifact() -> dict:
    path = REPO_ROOT / "target" / "agent_context.json"
    if not path.exists():
        print("[info] agent_context.json missing — building from manifest...", file=sys.stderr)
        rc = subprocess.call(
            [sys.executable, str(REPO_ROOT / "scripts/agent_context/build_agent_context.py")]
        )
        if rc != 0:
            raise SystemExit("ERROR: could not build agent context (is target/manifest.json present?)")
    return json.loads(path.read_text())


def main() -> int:
    ap = argparse.ArgumentParser(description="Print a model's change packet")
    ap.add_argument("--select", "-s", required=True, help="model name")
    ap.add_argument("--task", choices=sorted(TASK_GUIDANCE), default="fix")
    args = ap.parse_args()

    artifact = ensure_artifact()
    model = artifact["models"].get(args.select)
    if model is None:
        candidates = [n for n in artifact["models"] if args.select in n][:8]
        print(f"ERROR: model '{args.select}' not found in agent context.")
        if candidates:
            print("  Did you mean: " + ", ".join(candidates))
        return 1

    c = model["contract"]
    print(f"=== {model['name']}  [{args.task}] ===")
    print(f"path: {model['path']}")
    line = f"materialized: {model['materialized']}"
    if model["incremental_strategy"]:
        line += f" | strategy: {model['incremental_strategy']}"
        if model["strategy_expression"]:
            line += " (EXPRESSION — branch depends on run vars; check before running)"
    if model["partition_by"]:
        line += f" | partition_by: {model['partition_by']}"
    print(line)
    if model["has_meta_full_refresh"]:
        stages = model.get("full_refresh_stages")
        print(f"staged full_refresh: yes ({len(stages)} stages)" if stages else "staged full_refresh: yes")
    flags = []
    if model["reads_this"]:
        flags.append("CUMULATIVE (reads {{ this }} — backfill order matters)")
    if model["high_risk"]:
        flags.append("high-risk class")
    if flags:
        print("flags: " + "; ".join(flags))

    if c.get("grain"):
        print(f"grain: {c['grain']}")
    if c.get("semantics"):
        print(f"semantics: {c['semantics']}")
    if c.get("ground_truth"):
        print(f"ground truth: {c['ground_truth']}")

    if c.get("hazards"):
        print("\nKNOWN HAZARDS (docs/lessons/<id>.md):")
        for h in c["hazards"]:
            print(f"  - [{h['status']}] {h['id']}: {h['title']}")

    if c.get("invariants"):
        print("\nINVARIANTS:")
        for inv in c["invariants"]:
            if isinstance(inv, dict):
                print(f"  - {inv.get('id', '')}: {inv.get('text', '')}")
            else:
                print(f"  - {inv}")

    if c.get("rules"):
        print("\nRULES:")
        for r in c["rules"]:
            text = r["text"] if isinstance(r, dict) else str(r)
            lesson = f"  [{r['lesson']}]" if isinstance(r, dict) and r.get("lesson") else ""
            print(f"  - {' '.join(text.split())}{lesson}")

    print(f"\nLINEAGE: {model['downstream_count']} downstream models"
          + (f"; api marts affected: {', '.join(model['downstream_api_models'])}"
             if model["downstream_api_models"] else ""))

    if c.get("reprocess_runbook"):
        print(f"\nREPROCESS: {c['reprocess_runbook']}")

    print("\nVALIDATION:")
    for v in c.get("validation", []) or ["make check-fast"]:
        print(f"  - {v}")
    print(f"  - dbt build -s {model['name']} (tests included)")

    print(f"\nTASK GUIDANCE ({args.task}):")
    for g in TASK_GUIDANCE[args.task]:
        print(f"  {g}")

    if c.get("agents_md"):
        print(f"\nScoped guide: {c['agents_md']}")
    print(f"Resolved from profiles: {', '.join(c.get('profiles') or ['(global only)'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
