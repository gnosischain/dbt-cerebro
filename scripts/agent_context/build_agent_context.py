#!/usr/bin/env python3
"""Build target/agent_context.json — resolved agent contracts for every
first-party model.

Inputs (all derived, never hand-enumerated):
  - target/manifest.json           model configs, lineage, checksums, raw SQL
  - agent_context/profiles.yml     scope profiles (class rules; see file header)
  - docs/lessons/*.md              lesson records (frontmatter: id/status/title/...)

Resolution per model: global -> matching profiles (file order) -> meta.agent.
List fields (rules, hazards, validation, invariants) merge with de-dup;
agents_md accumulates guides across all matching layers (ordered, deduped);
scalar fields (semantics, grain, ground_truth, reprocess_runbook) are
overridden by later layers, model meta.agent winning.

Schema v2 (consumers must accept v1 AND v2 — MCP normalizes on load):
  - incremental_strategy is null for non-incremental models (the project-wide
    +incremental_strategy default otherwise leaks partition-overwrite hazards
    onto every view/table);
  - strategy_expression is detected from RAW model code (resolved and
    unrendered config both collapse config() expressions at parse time);
  - contract.agents_md is a LIST of guides;
  - lineage: downstream_direct_count + downstream_transitive_count, and
    downstream_api_models is the TRANSITIVE api_ descendant set (list capped,
    downstream_api_count carries the true total);
  - inputs_fingerprint: digest over models_hash + profiles.yml + lessons +
    AGENTS.md guides, so consumers can detect staleness from ANY input.

Outputs:
  - target/agent_context.json          full artifact (local agents)
  - target/agent_context.public.json   privacy-filtered variant for gh-pages/MCP
    (drops models with privacy/internal tags or meta expose_to_mcp=false —
    direct or nested under meta.semantic; never raw SQL/credentials/endpoints)

Determinism: output is sort_keys JSON; `models_hash` is a digest of the sorted
model checksums, so identical model code -> identical hash across re-parses
(dbt's invocation metadata is excluded). CI runs --check to verify the artifact
is reproducible and all references resolve.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

try:
    from scripts.agent_context.strategy import analyze_strategy
except ImportError:  # run as a script: this file's dir is sys.path[0]
    from strategy import analyze_strategy

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_VERSION = 2

LIST_FIELDS = ("rules", "hazards", "validation", "invariants")
SCALAR_FIELDS = ("semantics", "grain", "ground_truth", "reprocess_runbook")

# Transitive api_ descendants stored per model (the count field carries the
# true total when the list is capped).
DOWNSTREAM_API_CAP = 20

# meta.agent keys accepted from schema.yml (anything else is a validation error)
META_AGENT_KEYS = {
    "grain", "semantics", "invariants", "hazards", "ground_truth",
    "validation", "reprocess_runbook", "notes",
}

PRIVACY_TAG_MARKERS = ("privacy",)  # any tag containing these substrings
PRIVACY_TAGS_EXACT = {"internal_only"}


# ---------------------------------------------------------------------------
# Lessons
# ---------------------------------------------------------------------------

def load_lessons(lessons_dir: Path) -> Dict[str, dict]:
    lessons: Dict[str, dict] = {}
    for path in sorted(lessons_dir.glob("*.md")):
        if path.name == "INDEX.md":
            continue
        text = path.read_text()
        if not text.startswith("---"):
            continue
        try:
            _, fm, body = text.split("---", 2)
            meta = yaml.safe_load(fm)
        except Exception as exc:
            raise SystemExit(f"ERROR: unparseable lesson frontmatter in {path}: {exc}")
        lid = meta.get("id")
        if not lid:
            raise SystemExit(f"ERROR: lesson {path} has no id")
        if lid != path.stem:
            raise SystemExit(f"ERROR: lesson id '{lid}' != filename '{path.stem}'")
        lessons[lid] = {
            "id": lid,
            "title": meta.get("title", ""),
            "status": meta.get("status", "observed"),
            "scope": meta.get("scope", ""),
            "symptom": meta.get("symptom", ""),
            "last_verified": str(meta.get("last_verified", "")),
            "evidence": meta.get("evidence", []),
            "path": str(path.relative_to(REPO_ROOT)),
            # Full curated body (Symptom/Root cause/Remediation/...) — lessons
            # are public-safe by construction, and remote agents need the
            # remediation text, not just the headline.
            "body": body.strip(),
        }
    return lessons


# ---------------------------------------------------------------------------
# Profile matching / resolution
# ---------------------------------------------------------------------------

def merged_meta(node: dict) -> dict:
    """Model meta from both carriers: project-level +meta lands in config.meta,
    schema.yml meta in node.meta (node-level wins on key collision)."""
    cfg_meta = (node.get("config") or {}).get("meta") or {}
    top_meta = node.get("meta") or {}
    return {**cfg_meta, **top_meta}


def model_facts(node: dict) -> dict:
    """Derived, never-hand-written facts about a model node."""
    cfg = node.get("config") or {}
    raw = node.get("raw_code") or ""
    meta = merged_meta(node)
    materialized = cfg.get("materialized")
    # dbt_project.yml sets +incremental_strategy project-wide, so the RESOLVED
    # config carries a strategy on every node — only incremental models
    # actually have one. Expression detection must use raw code: config()
    # expressions are already collapsed in both config and unrendered_config.
    is_incremental = materialized == "incremental"
    strategy_info = analyze_strategy(raw) if is_incremental else None
    return {
        "name": node["name"],
        "path": node.get("original_file_path", ""),
        "materialized": materialized,
        "incremental_strategy": cfg.get("incremental_strategy") if is_incremental else None,
        "strategy_expression": bool(strategy_info and strategy_info["expression"]),
        "strategy_scoped_append": strategy_info["scoped_append"] if strategy_info else None,
        "partition_by": cfg.get("partition_by"),
        "unique_key": cfg.get("unique_key"),
        "tags": sorted(node.get("tags") or []),
        "has_meta_full_refresh": bool(meta.get("full_refresh")),
        "full_refresh_stages": [
            s.get("name") for s in (meta.get("full_refresh") or {}).get("stages", [])
        ] or None,
        "reads_this": "{{ this }}" in raw or "{{this}}" in raw,
        "owner": meta.get("owner"),
        "checksum": (node.get("checksum") or {}).get("checksum", ""),
        "description_present": bool(node.get("description")),
    }


def profile_matches(match: dict, facts: dict, raw_code: str) -> bool:
    for key, want in (match or {}).items():
        if key == "path_prefix":
            if not facts["path"].startswith(want):
                return False
        elif key == "name_prefix":
            if not facts["name"].startswith(want):
                return False
        elif key == "materialized":
            if facts["materialized"] != want:
                return False
        elif key == "incremental_strategy":
            if facts["incremental_strategy"] != want:
                return False
        elif key == "strategy_expression":
            if facts["strategy_expression"] != bool(want):
                return False
        elif key == "has_meta_full_refresh":
            if facts["has_meta_full_refresh"] != bool(want):
                return False
        elif key == "reads_this":
            if facts["reads_this"] != bool(want):
                return False
        elif key == "sql_contains":
            if want not in raw_code:
                return False
        elif key == "tag":
            if want not in facts["tags"]:
                return False
        else:
            raise SystemExit(f"ERROR: unknown profile match key '{key}'")
    return True


def merge_layer(contract: dict, layer: dict) -> None:
    for f in LIST_FIELDS:
        vals = layer.get(f)
        if vals:
            existing = contract.setdefault(f, [])
            for v in vals:
                if v not in existing:
                    existing.append(v)
    for f in SCALAR_FIELDS:
        if layer.get(f) is not None:
            contract[f] = layer[f]
    # Guides ACCUMULATE (a decode model with staged full_refresh needs both the
    # contracts guide and the full-refresh guide, not whichever profile matched
    # last). Profile values may be a scalar or a list; normalize either way.
    guides = layer.get("agents_md")
    if guides:
        if isinstance(guides, str):
            guides = [guides]
        existing = contract.setdefault("agents_md", [])
        for g in guides:
            if g not in existing:
                existing.append(g)
    if layer.get("notes"):
        contract.setdefault("notes", []).append(layer["notes"])


def resolve_contract(
    facts: dict, raw_code: str, meta_agent: Optional[dict],
    global_layer: dict, profiles: List[dict],
) -> dict:
    contract: dict = {}
    merge_layer(contract, global_layer)
    matched = []
    for prof in profiles:
        if profile_matches(prof.get("match") or {}, facts, raw_code):
            merge_layer(contract, prof)
            matched.append(prof["name"])
    if meta_agent:
        merge_layer(contract, meta_agent)
    contract["profiles"] = matched
    return contract


def is_high_risk(facts: dict) -> bool:
    return bool(
        facts["materialized"] == "incremental"
        or facts["path"].startswith("models/contracts")
        or facts["reads_this"]
        or facts["has_meta_full_refresh"]
    )


def has_explicit_contract(meta_agent: Optional[dict]) -> bool:
    if not meta_agent:
        return False
    return bool(meta_agent.get("grain") or meta_agent.get("invariants"))


def is_public(facts: dict, node: dict) -> bool:
    for tag in facts["tags"]:
        if tag in PRIVACY_TAGS_EXACT or any(m in tag for m in PRIVACY_TAG_MARKERS):
            return False
    # The live opt-out contract is DIRECT meta.expose_to_mcp (per-model in
    # schema.yml — see dbt_project.yml's mixpanel privacy notes); the nested
    # meta.semantic.expose_to_mcp form is also honored.
    meta = merged_meta(node)
    if meta.get("expose_to_mcp") is False:
        return False
    if (meta.get("semantic") or {}).get("expose_to_mcp") is False:
        return False
    return True


def transitive_descendants(uid: str, child_map: dict, cache: dict) -> set:
    """All transitive MODEL descendants of uid (iterative, cycle-safe, memoized)."""
    if uid in cache:
        return cache[uid]
    result: set = set()
    stack = [c for c in child_map.get(uid, []) if c.startswith("model.")]
    while stack:
        cur = stack.pop()
        if cur in result:
            continue
        result.add(cur)
        cached = cache.get(cur)
        if cached is not None:
            result |= cached
            continue
        stack.extend(c for c in child_map.get(cur, []) if c.startswith("model."))
    cache[uid] = result
    return result


# Bounded, deterministic input set for the artifact fingerprint. rglob is
# deliberately avoided (it would walk node_modules/worktrees); these patterns
# cover the root guide, domain guides (incl. one nested level), script guides.
_GUIDE_GLOBS = ("AGENTS.md", "models/*/AGENTS.md", "models/*/*/AGENTS.md",
                "scripts/*/AGENTS.md")


def _fingerprint_paths(repo_root: Path) -> List[Path]:
    paths = [repo_root / "agent_context" / "profiles.yml"]
    lessons_dir = repo_root / "docs" / "lessons"
    if lessons_dir.exists():
        paths.extend(sorted(lessons_dir.glob("*.md")))
    for pattern in _GUIDE_GLOBS:
        paths.extend(sorted(repo_root.glob(pattern)))
    return [p for p in paths if p.exists()]


def compute_inputs_fingerprint(models_hash: str, repo_root: Path = REPO_ROOT) -> str:
    """Digest over every artifact input: model checksums + profiles + lessons +
    guides. A consumer (or context.py) comparing this against a recompute knows
    the artifact is stale regardless of WHICH input moved."""
    h = hashlib.sha256()
    h.update(models_hash.encode())
    for p in _fingerprint_paths(repo_root):
        h.update(str(p.relative_to(repo_root)).encode())
        h.update(hashlib.sha256(p.read_bytes()).hexdigest().encode())
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build(target_dir: Path) -> dict:
    manifest_path = target_dir / "manifest.json"
    if not manifest_path.exists():
        raise SystemExit(f"ERROR: {manifest_path} not found — run `dbt parse` first")
    manifest = json.loads(manifest_path.read_text())
    project = manifest["metadata"]["project_name"]

    profiles_path = REPO_ROOT / "agent_context" / "profiles.yml"
    spec = yaml.safe_load(profiles_path.read_text())
    if spec.get("version") != 1:
        raise SystemExit("ERROR: profiles.yml version must be 1")
    global_layer = spec.get("global") or {}
    profiles = spec.get("profiles") or []

    lessons = load_lessons(REPO_ROOT / "docs" / "lessons")

    errors: List[str] = []

    # Validate profile hazard/lesson refs up front.
    for layer in [global_layer] + profiles:
        for h in layer.get("hazards") or []:
            if h not in lessons:
                errors.append(f"profile '{layer.get('name', 'global')}': unknown hazard '{h}'")
        for r in layer.get("rules") or []:
            if isinstance(r, dict) and r.get("lesson") and r["lesson"] not in lessons:
                errors.append(f"profile '{layer.get('name', 'global')}': rule references unknown lesson '{r['lesson']}'")

    models: Dict[str, dict] = {}
    checksums: List[str] = []
    child_map = manifest.get("child_map") or {}
    descendants_cache: Dict[str, set] = {}

    for uid, node in manifest["nodes"].items():
        if node.get("resource_type") != "model" or node.get("package_name") != project:
            continue
        facts = model_facts(node)
        raw_code = node.get("raw_code") or ""
        checksums.append(f"{node['name']}:{facts['checksum']}")

        meta_agent = merged_meta(node).get("agent")
        if meta_agent:
            unknown = set(meta_agent) - META_AGENT_KEYS
            if unknown:
                errors.append(f"{node['name']}: unknown meta.agent keys {sorted(unknown)}")
            for h in meta_agent.get("hazards") or []:
                if h not in lessons:
                    errors.append(f"{node['name']}: meta.agent references unknown lesson '{h}'")

        contract = resolve_contract(facts, raw_code, meta_agent, global_layer, profiles)
        # Attach lesson status inline so a consumer needn't join.
        contract["hazards"] = [
            {"id": h, "status": lessons[h]["status"], "title": lessons[h]["title"]}
            for h in contract.get("hazards", []) if h in lessons
        ]
        direct_children = [
            c for c in child_map.get(uid, []) if c.startswith("model.")
        ]
        descendants = transitive_descendants(uid, child_map, descendants_cache)
        api_descendants = sorted(
            {d.split(".")[-1] for d in descendants if d.split(".")[-1].startswith("api_")}
        )
        entry = {
            **{k: v for k, v in facts.items() if k != "checksum"},
            "contract": contract,
            "high_risk": is_high_risk(facts),
            "explicit_contract": has_explicit_contract(meta_agent),
            "downstream_direct_count": len(direct_children),
            "downstream_transitive_count": len(descendants),
            "downstream_api_models": api_descendants[:DOWNSTREAM_API_CAP],
            "downstream_api_count": len(api_descendants),
            "public": is_public(facts, node),
        }
        models[node["name"]] = entry

    if errors:
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        raise SystemExit(f"ERROR: {len(errors)} agent-context validation error(s)")

    models_hash = hashlib.sha256("\n".join(sorted(checksums)).encode()).hexdigest()

    return {
        "schema_version": SCHEMA_VERSION,
        "project": project,
        "models_hash": models_hash,
        "inputs_fingerprint": compute_inputs_fingerprint(models_hash, REPO_ROOT),
        "counts": {
            "models": len(models),
            "high_risk": sum(1 for m in models.values() if m["high_risk"]),
            "explicit_contracts": sum(1 for m in models.values() if m["explicit_contract"]),
        },
        "lessons": lessons,
        "models": models,
    }


def public_variant(artifact: dict) -> dict:
    pub = dict(artifact)
    pub["models"] = {
        name: {k: v for k, v in m.items() if k != "public"}
        for name, m in artifact["models"].items()
        if m["public"]
    }
    pub["counts"] = dict(artifact["counts"], models=len(pub["models"]))
    return pub


def dump(obj: dict) -> str:
    return json.dumps(obj, indent=2, sort_keys=True, default=str) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--target-dir", default=str(REPO_ROOT / "target"))
    ap.add_argument(
        "--check", action="store_true",
        help="Rebuild and verify the on-disk artifacts match (determinism/CI gate).",
    )
    args = ap.parse_args()

    target_dir = Path(args.target_dir)
    artifact = build(target_dir)
    full = dump(artifact)
    pub = dump(public_variant(artifact))

    full_path = target_dir / "agent_context.json"
    pub_path = target_dir / "agent_context.public.json"

    if args.check:
        ok = True
        for path, want in ((full_path, full), (pub_path, pub)):
            if not path.exists():
                print(f"CHECK FAIL: {path} missing", file=sys.stderr)
                ok = False
            elif path.read_text() != want:
                print(f"CHECK FAIL: {path} is stale — rerun build_agent_context.py", file=sys.stderr)
                ok = False
        if ok:
            print("agent-context check OK (deterministic, references resolve)")
        return 0 if ok else 1

    full_path.write_text(full)
    pub_path.write_text(pub)
    c = artifact["counts"]
    print(
        f"Wrote {full_path} ({c['models']} models, {c['high_risk']} high-risk, "
        f"{c['explicit_contracts']} explicit contracts, {len(artifact['lessons'])} lessons)"
    )
    print(f"Wrote {pub_path} ({len(json.loads(pub)['models'])} public models)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
