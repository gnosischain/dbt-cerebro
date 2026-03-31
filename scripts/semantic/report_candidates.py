#!/usr/bin/env python3
"""Report candidate semantic coverage gaps for first-party dbt models."""

from __future__ import annotations

import argparse
import copy
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.semantic.build_registry import (
    PROJECT_NAME,
    get_cerebro_meta,
    iter_semantic_authoring,
    load_json,
    load_semantic_authoring,
    canonical_status,
    semantic_authoring_roots,
)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-dir", default="target")
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    manifest_path = Path(args.target_dir) / "manifest.json"
    if not manifest_path.exists():
        print(f"Missing manifest: {manifest_path}", file=sys.stderr)
        return 2

    manifest, _ = load_json(manifest_path)
    authoring_roots = semantic_authoring_roots(REPO_ROOT)
    semantic_models, _metrics = load_semantic_authoring(*authoring_roots)
    coverage_items = []
    state_counts = Counter()
    module_state_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for unique_id, node in sorted(manifest.get("nodes", {}).items()):
        if node.get("resource_type") != "model" or node.get("package_name") != PROJECT_NAME:
            continue
        name = node["name"]
        if not name.startswith(("api_", "fct_", "int_")):
            continue
        module = node.get("fqn", ["", "unknown"])[1]
        authored = copy.deepcopy(semantic_models.get(name, {}))
        semantic_meta = get_cerebro_meta(authored)
        quality_tier = canonical_status(semantic_meta.get("quality_tier"), default="")
        if not authored:
            coverage_state = "missing"
        elif quality_tier == "approved":
            coverage_state = "approved"
        else:
            coverage_state = "candidate"
        item = {
            "name": name,
            "module": module,
            "path": node.get("original_file_path") or node.get("path", ""),
            "description": node.get("description", ""),
            "coverage_state": coverage_state,
            "semantic_source_file": authored.get("source_file", ""),
            "quality_tier": quality_tier or "missing",
        }
        coverage_items.append(item)
        state_counts[coverage_state] += 1
        module_state_counts[module][coverage_state] += 1

    if args.format == "json":
        print(
            json.dumps(
                {
                    "summary": {
                        "total_models": len(coverage_items),
                        "state_counts": dict(sorted(state_counts.items())),
                        "module_state_counts": {
                            module: dict(sorted(counts.items()))
                            for module, counts in sorted(module_state_counts.items())
                        },
                    },
                    "models": coverage_items,
                },
                indent=2,
                ensure_ascii=True,
            )
        )
        return 0

    missing_items = [item for item in coverage_items if item["coverage_state"] == "missing"]
    lines = [
        "# Semantic Candidate Report",
        "",
        "## Current authoring files",
        "\n".join(
            f"- {path.relative_to(REPO_ROOT)}"
            for path in iter_semantic_authoring(*authoring_roots)
        ) or "- none",
        "",
        "## Coverage summary",
        f"- Total tracked models: {len(coverage_items)}",
        f"- Approved: {state_counts.get('approved', 0)}",
        f"- Candidate scaffolded: {state_counts.get('candidate', 0)}",
        f"- Missing scaffold: {state_counts.get('missing', 0)}",
        "",
        "## Coverage by module",
    ]
    for module, counts in sorted(module_state_counts.items()):
        lines.append(
            "- `%s`: approved=%s, candidate=%s, missing=%s"
            % (
                module,
                counts.get("approved", 0),
                counts.get("candidate", 0),
                counts.get("missing", 0),
            )
        )
    lines.extend(
        [
            "",
            "## Missing semantic authoring candidates",
            f"Total missing: {len(missing_items)}",
            "",
        ]
    )
    for candidate in missing_items[:200]:
        lines.append(
            f"- `{candidate['name']}` ({candidate['module']}) :: {candidate['path']}"
        )
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
