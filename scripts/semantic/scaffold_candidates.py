#!/usr/bin/env python3
"""Generate candidate semantic authoring scaffolds for dbt models."""

from __future__ import annotations

import argparse
import copy
import json
import re
import sys
from pathlib import Path
from typing import Any, Optional

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.semantic.build_registry import (  # noqa: E402
    PROJECT_NAME,
    canonical_status,
    load_json,
    load_semantic_authoring,
    load_yaml_file,
    semantic_authoring_roots,
)

TIME_NAMES = (
    "date",
    "day",
    "week",
    "month",
    "hour",
    "timestamp",
    "block_date",
)
TIME_TOKENS = ("date", "day", "week", "month", "hour", "timestamp", "time")
STRING_MARKERS = ("string", "lowcardinality", "enum", "fixedstring")
NUMERIC_MARKERS = ("int", "float", "decimal")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-dir", default="target")
    parser.add_argument(
        "--modules",
        default="",
        help="Comma-separated module filter. Default: all modules.",
    )
    parser.add_argument("--write", action="store_true", help="Write scaffold files in place.")
    return parser.parse_args(argv)


def _humanize_model_name(model_name: str) -> str:
    name = re.sub(r"^(api_|fct_|int_)", "", model_name)
    return name.replace("_", " ")


def _semantic_name(model_name: str) -> str:
    return re.sub(r"^(api_|fct_|int_)", "", model_name)


def _unique_semantic_name(model_name: str, used_names: set[str]) -> str:
    base_name = _semantic_name(model_name)
    if base_name not in used_names:
        used_names.add(base_name)
        return base_name

    candidate_name = model_name
    if candidate_name not in used_names:
        used_names.add(candidate_name)
        return candidate_name

    suffix = 2
    while True:
        candidate_name = f"{model_name}_{suffix}"
        if candidate_name not in used_names:
            used_names.add(candidate_name)
            return candidate_name
        suffix += 1


def _column_type(column: dict[str, Any]) -> str:
    return str(
        column.get("data_type")
        or column.get("type")
        or ""
    ).lower()


def _guess_time_granularity(model_name: str, column_name: str) -> str:
    lowered_model = model_name.lower()
    lowered_column = column_name.lower()
    for token in ("hourly", "hour"):
        if token in lowered_model or token == lowered_column:
            return "hour"
    for token in ("daily", "day", "date"):
        if token in lowered_model or token == lowered_column:
            return "day"
    for token in ("weekly", "week"):
        if token in lowered_model or token == lowered_column:
            return "week"
    for token in ("monthly", "month"):
        if token in lowered_model or token == lowered_column:
            return "month"
    return ""


def _is_time_column(column_name: str, column: dict[str, Any]) -> bool:
    lowered_name = column_name.lower()
    lowered_type = _column_type(column)
    if lowered_name in TIME_NAMES:
        return True
    if any(token in lowered_name for token in TIME_TOKENS) and "date" in lowered_type:
        return True
    return "datetime" in lowered_type or lowered_type == "date"


def _is_numeric_column(column: dict[str, Any]) -> bool:
    lowered_type = _column_type(column)
    return any(marker in lowered_type for marker in NUMERIC_MARKERS)


def _is_string_column(column: dict[str, Any]) -> bool:
    lowered_type = _column_type(column)
    return any(marker in lowered_type for marker in STRING_MARKERS)


def _is_id_like(name: str) -> bool:
    lowered_name = name.lower()
    return lowered_name in {
        "id",
        "block_number",
        "chain_id",
        "epoch",
        "slot",
        "validator_index",
    } or lowered_name.endswith(("_id", "_address", "_hash"))


def _agg_for_measure(name: str) -> str:
    lowered_name = name.lower()
    if any(token in lowered_name for token in ("pct", "percent", "ratio", "apy", "price", "avg", "mean")):
        return "average"
    return "sum"


def _candidate_dimensions(model_name: str, columns: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    dimensions: list[dict[str, Any]] = []
    agg_time_dimension = ""
    for column_name, column in columns.items():
        if _is_time_column(column_name, column):
            granularity = _guess_time_granularity(model_name, column_name) or "day"
            dimensions.append(
                {
                    "name": column_name,
                    "type": "time",
                    "expr": column_name,
                    "type_params": {"time_granularity": granularity},
                }
            )
            if not agg_time_dimension:
                agg_time_dimension = column_name
            continue

        if _is_string_column(column) or _is_id_like(column_name):
            dimensions.append(
                {
                    "name": column_name,
                    "type": "categorical",
                    "expr": column_name,
                }
            )
            continue

        lowered_name = column_name.lower()
        if lowered_name in {"label", "name", "category", "type", "status"}:
            dimensions.append(
                {
                    "name": column_name,
                    "type": "categorical",
                    "expr": column_name,
                }
            )

    return dimensions, agg_time_dimension


def _candidate_measures(columns: dict[str, Any]) -> list[dict[str, Any]]:
    measures: list[dict[str, Any]] = []
    for column_name, column in columns.items():
        if not _is_numeric_column(column):
            continue
        if _is_time_column(column_name, column) or _is_id_like(column_name):
            continue
        measures.append(
            {
                "name": f"{column_name}_value" if not column_name.endswith("_value") else column_name,
                "agg": _agg_for_measure(column_name),
                "expr": column_name,
            }
        )
    return measures


def _output_file(node: dict[str, Any]) -> Path:
    original_path = Path(node.get("original_file_path") or node.get("path") or "")
    parts = original_path.parts
    if len(parts) >= 3 and parts[0] == "models" and parts[1] == "execution":
        return REPO_ROOT / "semantic" / "authoring" / "execution" / parts[2] / "semantic_models.yml"
    module = node.get("fqn", ["", "unknown"])[1]
    return REPO_ROOT / "semantic" / "authoring" / module / "semantic_models.yml"


def _candidate_model(node: dict[str, Any], semantic_name: Optional[str] = None) -> dict[str, Any]:
    model_name = node["name"]
    columns = node.get("columns", {}) or {}
    dimensions, agg_time_dimension = _candidate_dimensions(model_name, columns)
    measures = _candidate_measures(columns)
    semantic_model: dict[str, Any] = {
        "name": semantic_name or _semantic_name(model_name),
        "model": "ref('%s')" % model_name,
        "config": {
            "meta": {
                "cerebro": {
                    "owner": (
                        node.get("config", {}).get("meta", {}).get("owner")
                        or node.get("meta", {}).get("owner")
                        or "analytics_team"
                    ),
                    "quality_tier": "candidate",
                    "question_synonyms": [_humanize_model_name(model_name)],
                }
            }
        },
    }
    if agg_time_dimension:
        semantic_model["defaults"] = {"agg_time_dimension": agg_time_dimension}
        grain = _guess_time_granularity(model_name, agg_time_dimension)
        if grain:
            semantic_model["config"]["meta"]["cerebro"]["grain"] = grain
    if dimensions:
        semantic_model["dimensions"] = dimensions
    if measures:
        semantic_model["measures"] = measures
    return semantic_model


def _load_existing_authoring(path: Path) -> dict[str, Any]:
    if path.exists():
        payload = load_yaml_file(path)
    else:
        payload = {}
    return {
        "semantic_models": list(payload.get("semantic_models", []) or []),
        "metrics": list(payload.get("metrics", []) or []),
    }


def _write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = yaml.safe_dump(
        payload,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=False,
        width=120,
    )
    path.write_text(body, encoding="utf-8")


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    manifest_path = Path(args.target_dir) / "manifest.json"
    if not manifest_path.exists():
        print(f"Missing manifest: {manifest_path}", file=sys.stderr)
        return 2

    modules_filter = {
        module.strip()
        for module in args.modules.split(",")
        if module.strip()
    }
    manifest, _ = load_json(manifest_path)
    authored_models, _metrics = load_semantic_authoring(*semantic_authoring_roots(REPO_ROOT))
    used_semantic_names = {
        authored.get("name")
        for authored in authored_models.values()
        if isinstance(authored, dict) and authored.get("name")
    }
    grouped_updates: dict[Path, list[dict[str, Any]]] = {}

    for unique_id, node in sorted(manifest.get("nodes", {}).items()):
        if node.get("resource_type") != "model" or node.get("package_name") != PROJECT_NAME:
            continue
        model_name = node["name"]
        module = node.get("fqn", ["", "unknown"])[1]
        if modules_filter and module not in modules_filter:
            continue
        if not model_name.startswith(("api_", "fct_", "int_")):
            continue
        if model_name in authored_models:
            continue
        semantic_name = _unique_semantic_name(model_name, used_semantic_names)
        grouped_updates.setdefault(_output_file(node), []).append(
            _candidate_model(node, semantic_name=semantic_name)
        )

    if not grouped_updates:
        print("No missing candidate scaffolds found.")
        return 0

    for path, additions in sorted(grouped_updates.items()):
        existing = _load_existing_authoring(path)
        semantic_models = copy.deepcopy(existing["semantic_models"])
        semantic_models.extend(sorted(additions, key=lambda item: item["name"]))
        payload = {
            "semantic_models": semantic_models,
            "metrics": existing["metrics"],
        }
        if args.write:
            _write_yaml(path, payload)
        print(
            "%s %s (+%s scaffolds)"
            % (
                "UPDATED" if args.write else "WOULD UPDATE",
                path.relative_to(REPO_ROOT),
                len(additions),
            )
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
