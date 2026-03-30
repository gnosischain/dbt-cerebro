#!/usr/bin/env python3
"""Build the Cerebro semantic registry from dbt artifacts and semantic authoring."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.semantic.build_reporting import (
    update_summary_section,
    utc_now,
    write_metrics,
)

PROJECT_NAME = "gnosis_dbt"
APPROVED_STATUSES = {"approved"}
BLOCKED_STATUSES = {"blocked"}
SOURCE_PREFIX = "source."
MODEL_PREFIX = "model."
REQUIRED_APPROVED_META = ("grain", "owner", "quality_tier", "question_synonyms")
MODEL_NAME_RE = re.compile(r"ref\((?:'|\")(?P<name>[^'\"]+)(?:'|\")\)")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def load_json(path: Path) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    return json.loads(raw), sha256_bytes(raw)


def dump_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )


def load_yaml_file(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a mapping at the top level")
    return data


def semantic_authoring_roots(repo_root: Path) -> list[Path]:
    return [
        repo_root / "semantic" / "authoring",
        repo_root / "models",
    ]


def iter_semantic_authoring(*roots: Path) -> list[Path]:
    paths: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        if not root.exists():
            continue
        for path in sorted(root.rglob("semantic_models.yml")):
            if not path.is_file() or path in seen:
                continue
            seen.add(path)
            paths.append(path)
    return paths


def resolve_model_ref(raw_ref: Optional[str]) -> Optional[str]:
    if not raw_ref:
        return None
    match = MODEL_NAME_RE.search(raw_ref)
    if match:
        return match.group("name")
    return raw_ref


def canonical_status(value: Optional[str], *, default: str = "candidate") -> str:
    if not value:
        return default
    return str(value).strip().lower()


def canonical_node_name(unique_id: str) -> str:
    parts = unique_id.split(".")
    return parts[-1] if parts else unique_id


def get_cerebro_meta(payload: dict[str, Any]) -> dict[str, Any]:
    config_meta = payload.get("config", {}).get("meta", {}).get("cerebro", {})
    if isinstance(config_meta, dict) and config_meta:
        return config_meta
    legacy_meta = payload.get("meta", {}).get("cerebro", {})
    return legacy_meta if isinstance(legacy_meta, dict) else {}


def merge_column_info(
    manifest_columns: dict[str, Any],
    catalog_columns: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    column_names = sorted(set(manifest_columns) | set(catalog_columns))
    for column_name in column_names:
        manifest_column = manifest_columns.get(column_name, {}) or {}
        catalog_column = catalog_columns.get(column_name, {}) or {}
        description = (
            manifest_column.get("description")
            or catalog_column.get("comment")
            or catalog_column.get("description")
            or ""
        )
        merged[column_name] = {
            "name": column_name,
            "description": description,
            "data_type": (
                catalog_column.get("type")
                or manifest_column.get("data_type")
                or catalog_column.get("data_type")
                or ""
            ),
        }
    return merged


def build_lineage(
    manifest: dict[str, Any],
    unique_id: str,
    project_model_ids: set[str],
    project_source_ids: set[str],
) -> dict[str, list[str]]:
    parent_map = manifest.get("parent_map", {})
    child_map = manifest.get("child_map", {})

    def simplify(nodes: list[str]) -> list[str]:
        output: list[str] = []
        for node_id in nodes:
            if node_id in project_model_ids or node_id in project_source_ids:
                output.append(canonical_node_name(node_id))
        return output

    return {
        "upstream": simplify(parent_map.get(unique_id, [])),
        "downstream": simplify(child_map.get(unique_id, [])),
    }


def load_semantic_authoring(*roots: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    semantic_models: dict[str, Any] = {}
    metrics: dict[str, Any] = {}
    for path in iter_semantic_authoring(*roots):
        payload = load_yaml_file(path)
        source_root = next(
            (root for root in roots if root.exists() and root in path.parents),
            path.parent,
        )
        for semantic_model in payload.get("semantic_models", []) or []:
            if not isinstance(semantic_model, dict):
                continue
            resolved_model = resolve_model_ref(semantic_model.get("model"))
            if not resolved_model:
                continue
            semantic_copy = copy.deepcopy(semantic_model)
            semantic_copy["resolved_model"] = resolved_model
            semantic_copy["source_file"] = str(path.relative_to(source_root.parent))
            semantic_models[resolved_model] = semantic_copy

        for metric in payload.get("metrics", []) or []:
            if not isinstance(metric, dict):
                continue
            metric_copy = copy.deepcopy(metric)
            metric_copy["source_file"] = str(path.relative_to(source_root.parent))
            metrics[metric_copy["name"]] = metric_copy
    return semantic_models, metrics


def load_relationships(relationships_root: Path) -> list[dict[str, Any]]:
    relationships: list[dict[str, Any]] = []
    if not relationships_root.exists():
        return relationships
    for path in sorted(relationships_root.glob("*.yml")):
        payload = load_yaml_file(path)
        for relationship in payload.get("relationships", []) or []:
            if not isinstance(relationship, dict):
                continue
            relationship_copy = copy.deepcopy(relationship)
            relationship_copy["source_file"] = str(path)
            relationship_copy["quality_tier"] = canonical_status(
                relationship_copy.get("quality_tier"),
                default="candidate",
            )
            relationships.append(relationship_copy)
    return relationships


def load_overrides(overrides_root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    overrides: list[dict[str, Any]] = []
    duplicate_warnings: list[dict[str, Any]] = []
    seen_keys: dict[tuple[str, str], str] = {}
    if not overrides_root.exists():
        return overrides, duplicate_warnings

    for path in sorted(overrides_root.glob("*.yml")):
        payload = load_yaml_file(path)
        for override in payload.get("overrides", []) or []:
            if not isinstance(override, dict):
                continue
            override_copy = copy.deepcopy(override)
            override_copy["source_file"] = str(path)
            override_type = str(override_copy.get("type", "generic"))
            target = str(override_copy.get("target", ""))
            key = (override_type, target)
            if key in seen_keys:
                duplicate_warnings.append(
                    {
                        "code": "duplicate_override",
                        "severity": "warning",
                        "message": (
                            f"Override {override_type}:{target} appears in both "
                            f"{seen_keys[key]} and {path}"
                        ),
                    }
                )
            else:
                seen_keys[key] = str(path)
            overrides.append(override_copy)
    return overrides, duplicate_warnings


def build_namespaces(registry_models: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    namespaces: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"providers": [], "metrics": []}
    )
    for model_name, model in registry_models.items():
        for dimension in model.get("dimensions", []):
            namespace = namespaces[dimension["name"]]
            namespace["type"] = dimension.get("type", "")
            namespace["providers"].append(
                {
                    "model": model_name,
                    "module": model["module"],
                    "status": model["semantic_status"],
                }
            )
        for entity in model.get("entities", []):
            namespace = namespaces[entity["name"]]
            namespace["type"] = entity.get("type", "")
            namespace["providers"].append(
                {
                    "model": model_name,
                    "module": model["module"],
                    "status": model["semantic_status"],
                }
            )
    for namespace in namespaces.values():
        namespace["providers"].sort(
            key=lambda provider: (
                provider["module"],
                provider["model"],
            )
        )
    return dict(sorted(namespaces.items()))


def build_metrics(
    authored_metrics: dict[str, Any],
    registry_models: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    metrics: dict[str, dict[str, Any]] = {}
    measure_to_model: dict[str, str] = {}
    for model_name, model in registry_models.items():
        for measure in model.get("measures", []):
            measure_to_model[measure["name"]] = model_name

    for metric_name, authored_metric in authored_metrics.items():
        meta = get_cerebro_meta(authored_metric)
        measure_name = authored_metric.get("type_params", {}).get("measure", "")
        root_model = measure_to_model.get(measure_name, "")
        status = canonical_status(meta.get("quality_tier"), default="candidate")
        metrics[metric_name] = {
            "name": metric_name,
            "label": authored_metric.get("label", metric_name),
            "description": authored_metric.get("description", ""),
            "type": authored_metric.get("type", ""),
            "measure": measure_name,
            "root_model": root_model,
            "module": registry_models.get(root_model, {}).get("module", ""),
            "quality_tier": status,
            "semantic_status": "approved" if status in APPROVED_STATUSES else "candidate",
            "allowed_dimensions": meta.get("allowed_dimensions", []),
            "supported_time_grains": meta.get("supported_time_grains", []),
            "default_filters": meta.get("default_filters", []),
            "question_synonyms": meta.get("question_synonyms", []),
            "source_file": authored_metric.get("source_file", ""),
        }
        if root_model:
            registry_models[root_model].setdefault("metric_names", []).append(metric_name)
    return metrics


def build_module_summary(registry_models: dict[str, dict[str, Any]]) -> dict[str, dict[str, int]]:
    summary: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for model in registry_models.values():
        module = model["module"]
        summary[module]["total_nodes"] += 1
        summary[module][model["resource_type"]] += 1
        summary[module][model["semantic_status"]] += 1
        name = model["name"]
        for prefix in ("api_", "fct_", "int_", "stg_"):
            if name.startswith(prefix):
                summary[module][prefix.rstrip("_")] += 1
                break
    return {key: dict(value) for key, value in sorted(summary.items())}


def build_registry(
    *,
    manifest: dict[str, Any],
    manifest_hash: str,
    catalog: dict[str, Any],
    catalog_hash: str,
    semantic_manifest_hash: str,
    semantic_models: dict[str, Any],
    authored_metrics: dict[str, Any],
    relationships: list[dict[str, Any]],
    overrides: list[dict[str, Any]],
) -> dict[str, Any]:
    manifest_nodes = manifest.get("nodes", {})
    manifest_sources = manifest.get("sources", {})
    catalog_nodes = catalog.get("nodes", {})
    catalog_sources = catalog.get("sources", {})

    project_model_ids = {
        unique_id
        for unique_id, node in manifest_nodes.items()
        if node.get("resource_type") == "model"
        and node.get("package_name") == PROJECT_NAME
    }
    project_source_ids = {
        unique_id
        for unique_id, node in manifest_sources.items()
        if node.get("package_name") == PROJECT_NAME
    }

    registry_models: dict[str, dict[str, Any]] = {}

    for unique_id in sorted(project_model_ids):
        node = manifest_nodes[unique_id]
        model_name = node["name"]
        authored_semantic = semantic_models.get(model_name, {})
        semantic_meta = get_cerebro_meta(authored_semantic)
        quality_tier = canonical_status(semantic_meta.get("quality_tier"))
        semantic_status = (
            "approved"
            if quality_tier in APPROVED_STATUSES
            else "candidate" if authored_semantic else "docs_only"
        )
        lineage = build_lineage(manifest, unique_id, project_model_ids, project_source_ids)
        manifest_columns = node.get("columns", {})
        catalog_columns = catalog_nodes.get(unique_id, {}).get("columns", {})
        columns = merge_column_info(manifest_columns, catalog_columns)
        registry_models[model_name] = {
            "name": model_name,
            "resource_type": "model",
            "package_name": node.get("package_name", ""),
            "module": node.get("fqn", ["", "unknown"])[1],
            "path": node.get("original_file_path") or node.get("path", ""),
            "fqn": node.get("fqn", []),
            "materialized": node.get("config", {}).get("materialized", ""),
            "description": node.get("description", ""),
            "owner": (
                node.get("config", {}).get("meta", {}).get("owner")
                or node.get("meta", {}).get("owner")
                or ""
            ),
            "tags": node.get("tags", []),
            "relation_name": node.get("relation_name", ""),
            "semantic_status": semantic_status,
            "quality_tier": quality_tier if authored_semantic else "",
            "semantic_source_file": authored_semantic.get("source_file", ""),
            "semantic": {
                "defaults": authored_semantic.get("defaults", {}),
                "meta": semantic_meta,
            },
            "entities": authored_semantic.get("entities", []),
            "dimensions": authored_semantic.get("dimensions", []),
            "measures": authored_semantic.get("measures", []),
            "metric_names": [],
            "columns": columns,
            "lineage": lineage,
        }

    for unique_id in sorted(project_source_ids):
        node = manifest_sources[unique_id]
        source_name = f"{node.get('source_name', node.get('schema', 'source'))}.{node['name']}"
        catalog_columns = catalog_sources.get(unique_id, {}).get("columns", {})
        columns = merge_column_info(node.get("columns", {}), catalog_columns)
        registry_models[source_name] = {
            "name": source_name,
            "resource_type": "source",
            "package_name": node.get("package_name", ""),
            "module": node.get("fqn", ["", "unknown"])[1],
            "path": node.get("original_file_path") or node.get("path", ""),
            "fqn": node.get("fqn", []),
            "materialized": "source",
            "description": node.get("description", ""),
            "owner": node.get("meta", {}).get("owner", ""),
            "tags": node.get("tags", []),
            "relation_name": node.get("relation_name", ""),
            "semantic_status": "docs_only",
            "quality_tier": "",
            "semantic_source_file": "",
            "semantic": {},
            "entities": [],
            "dimensions": [],
            "measures": [],
            "metric_names": [],
            "columns": columns,
            "lineage": build_lineage(
                manifest,
                unique_id,
                project_model_ids,
                project_source_ids,
            ),
        }

    metrics = build_metrics(authored_metrics, registry_models)
    namespaces = build_namespaces(registry_models)
    modules = build_module_summary(registry_models)

    return {
        "metadata": {
            "generated_at": utc_now(),
            "project_name": manifest.get("metadata", {}).get("project_name", PROJECT_NAME),
            "manifest_hash": manifest_hash,
            "catalog_hash": catalog_hash,
            "semantic_manifest_hash": semantic_manifest_hash,
            "model_count": sum(1 for model in registry_models.values() if model["resource_type"] == "model"),
            "source_count": sum(1 for model in registry_models.values() if model["resource_type"] == "source"),
        },
        "models": dict(sorted(registry_models.items())),
        "metrics": dict(sorted(metrics.items())),
        "relationships": relationships,
        "overrides": overrides,
        "namespaces": namespaces,
        "modules": modules,
        "coverage_summary": {
            "modules": modules,
            "semantic_status_counts": {
                status: sum(1 for model in registry_models.values() if model["semantic_status"] == status)
                for status in ("approved", "candidate", "docs_only")
            },
            "metric_count": len(metrics),
            "relationship_count": len(relationships),
        },
    }


def validate_registry(
    registry: dict[str, Any],
    *,
    override_warnings: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = list(override_warnings or [])
    models = registry["models"]
    metrics = registry["metrics"]
    relationships = registry["relationships"]

    for model_name, model in models.items():
        if model["resource_type"] != "model":
            continue
        if model["semantic_status"] == "approved":
            semantic_meta = model.get("semantic", {}).get("meta", {})
            for field in REQUIRED_APPROVED_META:
                value = semantic_meta.get(field)
                if value in ("", None, []):
                    errors.append(
                        {
                            "code": "missing_required_approved_meta",
                            "severity": "error",
                            "model": model_name,
                            "message": f"Approved semantic model {model_name} is missing config.meta.cerebro.{field}",
                        }
                    )
            if not model.get("measures"):
                warnings.append(
                    {
                        "code": "approved_model_missing_measures",
                        "severity": "warning",
                        "model": model_name,
                        "message": f"Approved semantic model {model_name} has no measures",
                    }
                )

        if not model.get("description"):
            warnings.append(
                {
                    "code": "missing_description",
                    "severity": "warning",
                    "model": model_name,
                    "message": f"Model {model_name} is missing a description",
                }
            )
        if not model.get("owner"):
            warnings.append(
                {
                    "code": "missing_owner",
                    "severity": "warning",
                    "model": model_name,
                    "message": f"Model {model_name} is missing an owner",
                }
            )

    dimension_providers: dict[str, set[str]] = defaultdict(set)
    for model_name, model in models.items():
        for dimension in model.get("dimensions", []):
            dimension_providers[dimension["name"]].add(model_name)

    approved_relationship_names = {
        relationship["name"]
        for relationship in relationships
        if relationship.get("quality_tier") in APPROVED_STATUSES
    }

    for relationship in relationships:
        left_model = relationship.get("left_model", "")
        right_model = relationship.get("right_model", "")
        if left_model not in models:
            errors.append(
                {
                    "code": "unknown_left_model",
                    "severity": "error",
                    "relationship": relationship.get("name", ""),
                    "message": f"Relationship references unknown left model {left_model}",
                }
            )
        if right_model not in models:
            errors.append(
                {
                    "code": "unknown_right_model",
                    "severity": "error",
                    "relationship": relationship.get("name", ""),
                    "message": f"Relationship references unknown right model {right_model}",
                }
            )
        if (
            relationship.get("allow_any_join")
            and relationship.get("name") not in approved_relationship_names
        ):
            warnings.append(
                {
                    "code": "allow_any_join_not_approved",
                    "severity": "warning",
                    "relationship": relationship.get("name", ""),
                    "message": (
                        f"Relationship {relationship.get('name', '')} allows ANY JOIN "
                        "but is not approved"
                    ),
                }
            )

    for metric_name, metric in metrics.items():
        root_model = metric.get("root_model", "")
        if not root_model or root_model not in models:
            errors.append(
                {
                    "code": "metric_missing_root_model",
                    "severity": "error",
                    "metric": metric_name,
                    "message": f"Metric {metric_name} does not resolve to a known root model",
                }
            )
            continue

        allowed_dimensions = metric.get("allowed_dimensions", [])
        root_dimension_names = {
            dimension["name"]
            for dimension in models[root_model].get("dimensions", [])
        }
        for dimension_name in allowed_dimensions:
            if dimension_name not in root_dimension_names and dimension_name not in dimension_providers:
                errors.append(
                    {
                        "code": "metric_dimension_unreachable",
                        "severity": "error",
                        "metric": metric_name,
                        "message": (
                            f"Metric {metric_name} declares allowed dimension "
                            f"{dimension_name} but no provider exists in the registry"
                        ),
                    }
                )

    return {
        "generated_at": utc_now(),
        "error_count": len(errors),
        "warning_count": len(warnings),
        "errors": errors,
        "warnings": warnings,
        "summary": {
            "approved_models": sum(
                1 for model in models.values() if model["semantic_status"] == "approved"
            ),
            "candidate_models": sum(
                1 for model in models.values() if model["semantic_status"] == "candidate"
            ),
            "docs_only_models": sum(
                1 for model in models.values() if model["semantic_status"] == "docs_only"
            ),
            "approved_metrics": sum(
                1 for metric in metrics.values()
                if metric["semantic_status"] == "approved"
            ),
        },
    }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-dir", default="target")
    parser.add_argument("--validate", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    target_dir = Path(args.target_dir)
    started_at = time.perf_counter()
    repo_root = Path(__file__).resolve().parents[2]
    manifest_path = target_dir / "manifest.json"
    catalog_path = target_dir / "catalog.json"
    semantic_manifest_path = target_dir / "semantic_manifest.json"
    required_paths = (manifest_path, catalog_path, semantic_manifest_path)
    missing = [str(path) for path in required_paths if not path.exists()]
    if missing:
        summary = update_summary_section(
            target_dir,
            "registry",
            {
                "status": "error",
                "elapsed_seconds": round(time.perf_counter() - started_at, 6),
                "missing_inputs": missing,
                "validation": {"error_count": 0, "warning_count": 0},
                "coverage": {
                    "semantic_status_counts": {},
                    "metric_quality_counts": {},
                    "relationship_quality_counts": {},
                },
            },
        )
        write_metrics(target_dir, summary)
        print(f"Missing required inputs: {', '.join(missing)}", file=sys.stderr)
        return 2

    try:
        manifest, manifest_hash = load_json(manifest_path)
        catalog, catalog_hash = load_json(catalog_path)
        _semantic_manifest, semantic_manifest_hash = load_json(semantic_manifest_path)
        semantic_models, authored_metrics = load_semantic_authoring(
            *semantic_authoring_roots(repo_root)
        )
        relationships = load_relationships(repo_root / "semantic" / "relationships")
        overrides, override_warnings = load_overrides(repo_root / "semantic" / "overrides")
        registry = build_registry(
            manifest=manifest,
            manifest_hash=manifest_hash,
            catalog=catalog,
            catalog_hash=catalog_hash,
            semantic_manifest_hash=semantic_manifest_hash,
            semantic_models=semantic_models,
            authored_metrics=authored_metrics,
            relationships=relationships,
            overrides=overrides,
        )
        validation = validate_registry(registry, override_warnings=override_warnings)
        dump_json(target_dir / "semantic_registry.json", registry)
        dump_json(target_dir / "semantic_validation_report.json", validation)
    except Exception as exc:  # pragma: no cover - fatal path
        summary = update_summary_section(
            target_dir,
            "registry",
            {
                "status": "error",
                "elapsed_seconds": round(time.perf_counter() - started_at, 6),
                "error": str(exc),
                "validation": {"error_count": 0, "warning_count": 0},
                "coverage": {
                    "semantic_status_counts": {},
                    "metric_quality_counts": {},
                    "relationship_quality_counts": {},
                },
            },
        )
        write_metrics(target_dir, summary)
        print(f"Fatal semantic registry build error: {exc}", file=sys.stderr)
        return 2

    status = "success"
    if args.validate and validation["error_count"] > 0:
        status = "validation_failed"
    metric_quality_counts: dict[str, int] = defaultdict(int)
    for metric in registry["metrics"].values():
        metric_quality_counts[metric.get("quality_tier", "candidate") or "candidate"] += 1
    relationship_quality_counts: dict[str, int] = defaultdict(int)
    for relationship in registry["relationships"]:
        relationship_quality_counts[
            relationship.get("quality_tier", "candidate") or "candidate"
        ] += 1
    summary = update_summary_section(
        target_dir,
        "registry",
        {
            "status": status,
            "elapsed_seconds": round(time.perf_counter() - started_at, 6),
            "validation": {
                "error_count": validation["error_count"],
                "warning_count": validation["warning_count"],
            },
            "coverage": {
                "semantic_status_counts": registry["coverage_summary"]["semantic_status_counts"],
                "metric_quality_counts": dict(sorted(metric_quality_counts.items())),
                "relationship_quality_counts": dict(sorted(relationship_quality_counts.items())),
            },
            "model_count": registry["metadata"]["model_count"],
            "source_count": registry["metadata"]["source_count"],
            "metric_count": registry["coverage_summary"]["metric_count"],
            "relationship_count": registry["coverage_summary"]["relationship_count"],
        },
    )
    write_metrics(target_dir, summary)
    if status == "validation_failed":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
