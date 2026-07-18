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

try:
    from scripts.agent_context import build_agent_context as builder
except ImportError:  # run as a script: this file's dir is sys.path[0]
    import build_agent_context as builder

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


def artifact_is_stale(artifact_path: Path, input_paths) -> bool:
    """True when any existing input file is newer than the artifact (mtime)."""
    art_mtime = artifact_path.stat().st_mtime
    return any(p.exists() and p.stat().st_mtime > art_mtime for p in input_paths)


def ensure_artifact(force_rebuild: bool = False) -> dict:
    path = REPO_ROOT / "target" / "agent_context.json"
    inputs = [REPO_ROOT / "target" / "manifest.json"]
    inputs.extend(builder._fingerprint_paths(REPO_ROOT))

    reason = None
    if force_rebuild:
        reason = "forced rebuild"
    elif not path.exists():
        reason = "missing"
    elif artifact_is_stale(path, inputs):
        reason = "stale (an input changed since the artifact was built)"
    else:
        data = json.loads(path.read_text())
        if data.get("schema_version") != builder.SCHEMA_VERSION:
            reason = (
                f"schema_version {data.get('schema_version')} != {builder.SCHEMA_VERSION}"
            )
        else:
            return data

    print(f"[info] agent_context.json {reason} — building from manifest...", file=sys.stderr)
    rc = subprocess.call(
        [sys.executable, str(REPO_ROOT / "scripts/agent_context/build_agent_context.py")]
    )
    if rc != 0:
        raise SystemExit("ERROR: could not build agent context (is target/manifest.json present?)")
    return json.loads(path.read_text())


def degraded_packet(name: str, task: str, artifact: dict) -> int:
    """Best-effort guidance for a model the artifact doesn't know (usually a
    brand-new file that hasn't been through dbt parse yet). Never a hard error:
    a new model is exactly when an agent needs the guardrails most."""
    print(f"=== {name}  [{task}] — DEGRADED PACKET ===")
    print("This model is not in the agent context (new model? not parsed yet).")
    print("For the full packet: run `dbt parse` (make manifest), then re-run this command.\n")

    candidates = [n for n in artifact.get("models", {}) if name in n][:8]
    if candidates:
        print("Similarly named existing models: " + ", ".join(candidates) + "\n")

    matches = sorted((REPO_ROOT / "models").rglob(f"{name}.sql"))
    guides: list[str] = []
    if matches:
        print(f"file: {matches[0].relative_to(REPO_ROOT)}")
        current = matches[0].parent
        while current != REPO_ROOT:
            guide = current / "AGENTS.md"
            if guide.exists():
                guides.append(str(guide.relative_to(REPO_ROOT)))
            current = current.parent
    if (REPO_ROOT / "AGENTS.md").exists():
        guides.append("AGENTS.md")
    if guides:
        print("Scoped guides (read before changing anything):")
        for g in guides:
            print(f"  - {g}")

    try:
        import yaml
        spec = yaml.safe_load((REPO_ROOT / "agent_context" / "profiles.yml").read_text())
        rules = (spec.get("global") or {}).get("rules") or []
        if rules:
            print("\nGLOBAL RULES:")
            for r in rules:
                text = r["text"] if isinstance(r, dict) else str(r)
                print(f"  - {' '.join(text.split())}")
    except Exception:
        pass

    print(f"\nTASK GUIDANCE ({task}):")
    for g in TASK_GUIDANCE[task]:
        print(f"  {g}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Print a model's change packet")
    ap.add_argument("--select", "-s", required=True, help="model name")
    ap.add_argument("--task", choices=sorted(TASK_GUIDANCE), default="fix")
    args = ap.parse_args()

    artifact = ensure_artifact()
    model = artifact["models"].get(args.select)
    if model is None:
        # A fresh manifest may contain it — force one rebuild before degrading.
        artifact = ensure_artifact(force_rebuild=True)
        model = artifact["models"].get(args.select)
    if model is None:
        return degraded_packet(args.select, args.task, artifact)

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

    api_models = model.get("downstream_api_models") or []
    api_count = model.get("downstream_api_count", len(api_models))
    lineage = (f"\nLINEAGE: {model['downstream_direct_count']} direct children, "
               f"{model['downstream_transitive_count']} transitive downstream models")
    if api_models:
        more = f" (+{api_count - len(api_models)} more)" if api_count > len(api_models) else ""
        lineage += f"; api marts affected (transitive): {', '.join(api_models)}{more}"
    print(lineage)

    if c.get("reprocess_runbook"):
        print(f"\nREPROCESS: {c['reprocess_runbook']}")

    print("\nVALIDATION:")
    for v in c.get("validation", []) or ["make check-fast"]:
        print(f"  - {v}")
    print(f"  - dbt build -s {model['name']} (tests included)")

    print(f"\nTASK GUIDANCE ({args.task}):")
    for g in TASK_GUIDANCE[args.task]:
        print(f"  {g}")

    guides = c.get("agents_md") or []
    if isinstance(guides, str):
        guides = [guides]
    if guides:
        print("\nScoped guides: " + ", ".join(guides))
    print(f"Resolved from profiles: {', '.join(c.get('profiles') or ['(global only)'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
