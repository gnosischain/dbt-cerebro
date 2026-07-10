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
GRAPH_REQUIRED = (
    "enabled",
    "profile",
    "source_column",
    "target_column",
    "source_kind",
    "target_kind",
)
GRAPH_OPTIONAL = (
    "directed",
    "time_column",
    "weight_column",
    "node_enrichment_model",
    "node_enrichment_key",
    "evidence_model",
    "evidence_source_column",
    "evidence_target_column",
    "default_filters",
    "notes",
)
GRAPH_ALLOWED = set(GRAPH_REQUIRED) | set(GRAPH_OPTIONAL)
MODEL_NAME_RE = re.compile(r"ref\((?:'|\")(?P<name>[^'\"]+)(?:'|\")\)")
# A bare or backtick-quoted SQL identifier. Graph column names are interpolated
# verbatim into generated ClickHouse SQL, so non-expression columns MUST match
# this (defense-in-depth against an injected identifier in authoring).
_SAFE_COLUMN_RE = re.compile(r"^`?[A-Za-z_][A-Za-z0-9_]*`?$")
_SAFE_PROFILE_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_NUMERIC_TYPE_TOKENS = ("int", "float", "decimal")
_TEMPORAL_TYPE_TOKENS = ("date", "time")
# Tokens that must never appear in a graph column value — even an "expression"
# column (substring(...)) has no legitimate use for a statement terminator or a
# SQL comment. Catches injection attempts the expression bypass would miss.
_DANGEROUS_SQL_TOKENS = (";", "--", "/*", "*/")


def _is_sql_expression(col: str) -> bool:
    return "(" in col or " " in col


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


def load_generated_entities(repo_root: Path) -> dict[str, list[dict[str, Any]]]:
    """Load the generated entity overlay (entity annotations per model).

    Produced by ``scripts/semantic/generate_entities.py`` from the entity
    dictionary. Deliberately NOT a semantic_models.yml authoring file: the
    authoring loader is last-file-wins per model, which cannot express the
    precedence contract. The merge is entities-only and happens in
    ``build_registry`` — a hand-authored semantic_model that declares its own
    entities wins WHOLESALE; everything else about the model is untouched.
    A missing file yields an empty overlay (build works without generation).
    """
    path = repo_root / "semantic" / "authoring" / "generated" / "entities_generated.yml"
    if not path.exists():
        return {}
    payload = load_yaml_file(path)
    overlay: dict[str, list[dict[str, Any]]] = {}
    for row in payload.get("generated_entities", []) or []:
        if not isinstance(row, dict):
            continue
        model = row.get("model")
        entities = row.get("entities") or []
        if model and entities:
            overlay[str(model)] = copy.deepcopy(entities)
    return overlay


def load_entity_dictionary(repo_root: Path) -> dict[str, dict[str, Any]]:
    """Load the human-curated entity dictionary for registry publication.

    Published as ``registry["entity_dictionary"]`` so the runtime
    (cerebro-mcp entity_index) can resolve hubs and sensitivity without
    reaching back into this repo. Missing file => empty dict.
    """
    path = repo_root / "semantic" / "entity_dictionary.yml"
    if not path.exists():
        return {}
    payload = load_yaml_file(path)
    dictionary: dict[str, dict[str, Any]] = {}
    for entry in payload.get("entities", []) or []:
        name = entry.get("entity")
        if not name:
            continue
        dictionary[str(name)] = {
            "hub_model": entry.get("hub_model"),
            "sensitivity": entry.get("sensitivity", "open"),
            "columns": list(entry.get("columns", []) or []),
        }
    return dictionary


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
        seen_entity_providers: set[tuple[str, str]] = set()
        for entity in model.get("entities", []):
            namespace = namespaces[entity["name"]]
            namespace["type"] = entity.get("type", "")
            # Multi-binding: a model can declare the same entity on several
            # columns (different `expr`). The namespace is a provider list for
            # the docs renderer, so collapse to one provider per (entity, model).
            provider_key = (entity["name"], model_name)
            if provider_key in seen_entity_providers:
                continue
            seen_entity_providers.add(provider_key)
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


# Metric types whose value is computed FROM other metrics (post-aggregation)
# rather than from a measure. MVP scope: same-root only — every input metric
# must resolve to the SAME root_model (validate_registry enforces this).
DERIVED_METRIC_TYPES = ("ratio", "derived")


def derived_metric_input_names(metric_type: str, type_params: dict[str, Any]) -> list[str]:
    """Input metric names a ratio/derived metric depends on.

    Accepts both the bare-string and the MetricFlow ``{name: ...}`` mapping
    forms for ratio numerator/denominator and derived ``metrics`` entries.
    Order is preserved (numerator before denominator). Entries with no
    resolvable name are dropped — the validator flags structural gaps
    (missing numerator/denominator, empty metrics list) separately.
    """

    def _input_name(value: Any) -> str:
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            return str(value.get("name", "") or "")
        return ""

    type_params = type_params or {}
    names: list[str] = []
    if metric_type == "ratio":
        for key in ("numerator", "denominator"):
            name = _input_name(type_params.get(key))
            if name:
                names.append(name)
    elif metric_type == "derived":
        for item in type_params.get("metrics") or []:
            name = _input_name(item)
            if name:
                names.append(name)
    return names


def build_metrics(
    authored_metrics: dict[str, Any],
    registry_models: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    metrics: dict[str, dict[str, Any]] = {}

    # Build a `measure_name -> [model_names...]` map. Multiple semantic_models
    # can declare measures with the same name (this happens commonly with
    # generic names like `value_value`, `users_cnt_value`). Previously this
    # was a flat last-write-wins dict, which silently picked the wrong root
    # model when authors hit a collision. We now keep all candidates so
    # `validate_registry` can flag ambiguous bindings as errors. For the
    # `root_model` field below we deterministically pick `sorted(models)[0]`
    # — still a single source but at least stable across runs.
    measure_to_models: dict[str, list[str]] = defaultdict(list)
    for model_name, model in registry_models.items():
        for measure in model.get("measures", []):
            measure_to_models[measure["name"]].append(model_name)

    for metric_name, authored_metric in authored_metrics.items():
        meta = get_cerebro_meta(authored_metric)
        type_params = authored_metric.get("type_params", {}) or {}
        measure_name = type_params.get("measure", "")
        candidate_models = sorted(measure_to_models.get(measure_name, []))
        root_model = candidate_models[0] if candidate_models else ""
        status = canonical_status(meta.get("quality_tier"), default="candidate")
        metric_entry = {
            "name": metric_name,
            "label": authored_metric.get("label", metric_name),
            "description": authored_metric.get("description", ""),
            "type": authored_metric.get("type", ""),
            # `measure` is kept as a flat back-compat key (consumers key off
            # it); `type_params` is the full authored dict so ratio/derived
            # inputs (`numerator`, `denominator`, `metrics`, `expr`) reach
            # the registry and the MCP planner.
            "measure": measure_name,
            "type_params": copy.deepcopy(type_params),
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
        # Preserve the full candidate list so the validator can detect
        # ambiguous bindings. Not part of the public schema — consumers
        # should keep reading `root_model` (which is deterministic).
        if len(candidate_models) > 1:
            metric_entry["_ambiguous_measure_models"] = candidate_models
        metrics[metric_name] = metric_entry
        if root_model:
            registry_models[root_model].setdefault("metric_names", []).append(metric_name)

    # Second pass: ratio/derived metrics have no measure of their own, so the
    # first pass left root_model empty. Their root is inherited from their
    # input metrics — which must all exist and share ONE root_model (the
    # same-root MVP contract; validate_registry flags violations with
    # derived_metric_unknown_input / derived_metric_cross_root). When the
    # inputs are unknown or cross-root, root_model stays "" so the validator
    # has something concrete to point at.
    for metric_name, metric_entry in metrics.items():
        if metric_entry.get("type") not in DERIVED_METRIC_TYPES:
            continue
        input_names = derived_metric_input_names(
            metric_entry["type"], metric_entry.get("type_params", {})
        )
        if not input_names or any(name not in metrics for name in input_names):
            continue
        input_roots = {metrics[name].get("root_model", "") for name in input_names}
        if len(input_roots) != 1:
            continue
        root_model = next(iter(input_roots))
        if not root_model:
            continue
        metric_entry["root_model"] = root_model
        metric_entry["module"] = registry_models.get(root_model, {}).get("module", "")
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
    generated_entities: Optional[dict[str, list[dict[str, Any]]]] = None,
    entity_dictionary: Optional[dict[str, dict[str, Any]]] = None,
) -> dict[str, Any]:
    generated_entities = generated_entities or {}
    entity_dictionary = entity_dictionary or {}
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
        # Privacy boundary at the source: never publish internal-only /
        # identity-bridge models into the registry. Mirrors the cerebro-mcp
        # manifest deny list (INTERNAL_ONLY_TAGS + meta.expose_to_mcp=false);
        # the MCP loader also filters defensively, so an old artifact is safe.
        _tags = node.get("tags", []) or []
        _meta = node.get("config", {}).get("meta") or node.get("meta") or {}
        if any(t in ("internal_only", "privacy:tier_internal") for t in _tags) or (
            isinstance(_meta, dict) and _meta.get("expose_to_mcp") is False
        ):
            continue
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
            # Hand-authored entities win WHOLESALE; the generated overlay only
            # fills models that declare none (contract 0.4 — entities-only
            # merge, everything else about the model is untouched).
            "entities": (
                authored_semantic.get("entities")
                or generated_entities.get(model_name, [])
            ),
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
        # Human-curated entity dictionary (hub + sensitivity per entity) —
        # the runtime entity_index resolves hubs/privacy from here. Empty
        # when semantic/entity_dictionary.yml is absent.
        "entity_dictionary": entity_dictionary,
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


def load_graph_kinds(path: Path) -> dict[str, dict[str, Any]]:
    """Load the node-kind taxonomy registry (semantic/graph_kinds.yml).

    Returns a mapping of kind name -> metadata. Missing file -> empty mapping
    (the kind check then degrades to a no-op rather than failing the build).
    """
    if not path.exists():
        return {}
    data = load_yaml_file(path)
    kinds = data.get("node_kinds") or {}
    if not isinstance(kinds, dict):
        raise ValueError(f"{path}: 'node_kinds' must be a mapping")
    return kinds


def validate_graph_meta(
    model_name: str,
    model: dict[str, Any],
    models: dict[str, Any],
    *,
    allowed_kinds: Optional[set[str]] = None,
) -> list[dict[str, Any]]:
    """Validate one model's graph block; return a list of error/warning dicts.

    This is the single dbt-side graph validator (the cerebro-mcp side mirrors it
    in ``cerebro_mcp.semantic.graph_extraction``; the committed catalog JSON
    Schema is the shared contract). Each issue carries a ``code`` and
    ``severity`` (``error``/``warning``).
    """
    issues: list[dict[str, Any]] = []

    def _add(code: str, message: str, severity: str = "error") -> None:
        issues.append({"code": code, "severity": severity, "model": model_name, "message": message})

    graph = (model.get("semantic", {}).get("meta", {}) or {}).get("graph")
    if not graph:
        return issues
    if not isinstance(graph, dict):
        _add("graph_meta_not_mapping", f"{model_name}: config.meta.cerebro.graph must be a mapping")
        return issues
    unknown = set(graph) - GRAPH_ALLOWED
    if unknown:
        _add("graph_meta_unknown_keys", f"{model_name}: unknown cerebro.graph keys: {sorted(unknown)}")
    if not graph.get("enabled"):
        return issues
    missing = [key for key in GRAPH_REQUIRED if key not in graph]
    if missing:
        _add("graph_meta_missing_required", f"{model_name}: cerebro.graph missing required keys {missing}")
    profile_id = graph.get("profile")
    if profile_id and not _SAFE_PROFILE_RE.match(str(profile_id)):
        _add(
            "graph_meta_unsafe_identifier",
            f"{model_name}: cerebro.graph.profile='{profile_id}' must match [a-z][a-z0-9_]*",
        )
    model_columns = model.get("columns", {}) or {}
    columns = set(model_columns.keys())
    for key in ("source_column", "target_column", "time_column", "weight_column"):
        col = graph.get(key)
        if not col:
            continue
        # Forbidden tokens are rejected for ANY column value (even expressions).
        if any(tok in col for tok in _DANGEROUS_SQL_TOKENS):
            _add(
                "graph_meta_unsafe_identifier",
                f"{model_name}: cerebro.graph.{key}='{col}' contains a forbidden SQL token",
            )
            continue
        # Expression form (e.g. substring(...)) — author-trusted SQL; skip the
        # identifier/existence checks (the column-type checks below also skip it).
        if _is_sql_expression(col):
            continue
        # Defense-in-depth: a non-expression column is interpolated verbatim into
        # SQL, so it must be a plain (optionally backtick-quoted) identifier.
        if not _SAFE_COLUMN_RE.match(col):
            _add(
                "graph_meta_unsafe_identifier",
                f"{model_name}: cerebro.graph.{key}='{col}' is not a safe SQL identifier",
            )
            continue
        # Graph meta keeps ClickHouse identifier quoting (e.g. `from`, `to`)
        # because the column name is interpolated verbatim into generated SQL and
        # those are reserved words. The catalog stores the bare name, so strip
        # backticks before checking membership.
        bare = col.strip("`")
        if bare not in columns:
            _add(
                "graph_meta_unknown_column",
                f"{model_name}: cerebro.graph.{key}='{col}' not in model columns",
            )
            continue
        # Q3 — type checks. weight_column drives sum(), so a non-numeric type is
        # an ERROR (it fails at query time); time_column non-temporal is advisory
        # (some sources legitimately store dates as String).
        data_type = str((model_columns.get(bare, {}) or {}).get("data_type", "")).lower()
        if not data_type:
            continue
        if key == "weight_column" and not any(t in data_type for t in _NUMERIC_TYPE_TOKENS):
            _add(
                "graph_meta_weight_not_numeric",
                f"{model_name}: cerebro.graph.weight_column='{col}' has non-numeric type "
                f"'{data_type}'; sum() will fail at query time",
            )
        elif key == "time_column" and not any(t in data_type for t in _TEMPORAL_TYPE_TOKENS):
            _add(
                "graph_meta_time_not_temporal",
                f"{model_name}: cerebro.graph.time_column='{col}' has non-temporal type '{data_type}'",
                severity="warning",
            )
    for key in ("node_enrichment_model", "evidence_model"):
        ref = graph.get(key)
        if ref and ref not in models:
            _add(
                "graph_meta_unknown_model_ref",
                f"{model_name}: cerebro.graph.{key}='{ref}' not found in registry",
            )
    # Q2 — node kinds must be members of the taxonomy registry (graph_kinds.yml).
    if allowed_kinds is not None:
        for side in ("source_kind", "target_kind"):
            kind = graph.get(side)
            if kind and kind not in allowed_kinds:
                _add(
                    "graph_meta_unknown_kind",
                    f"{model_name}: cerebro.graph.{side}='{kind}' is not a registered node kind "
                    f"(add it to semantic/graph_kinds.yml)",
                )
    # Advisory: a kind with no matching entity won't auto-resolve enrichment joins.
    entity_names = {entity.get("name") for entity in model.get("entities", [])}
    for side in ("source_kind", "target_kind"):
        kind = graph.get(side)
        if kind and entity_names and kind not in entity_names:
            _add(
                "graph_meta_kind_without_entity",
                f"{model_name}: cerebro.graph.{side}='{kind}' has no matching entity; "
                "cross-model enrichment joins will not resolve automatically",
                severity="warning",
            )
    return issues


# ---------------------------------------------------------------------------
# Graph catalog (semantic_graph_catalog.json) — WS4
#
# The published, versioned graph contract. `profiles` is a strict 1:1 with the
# cerebro-mcp `GraphProfile` dataclass (contract-only sharing: the committed JSON
# Schema is the shared source of truth; mcp reconstructs GraphProfile from these
# rows). Deterministic output (no timestamps; sorted) so the artifact is
# byte-stable across rebuilds of the same registry.
# ---------------------------------------------------------------------------

GRAPH_CATALOG_SCHEMA_VERSION = 1

# Control / pagination keys that are NOT column filters (kept in sync with
# cerebro_mcp.semantic.graph_extraction._CONTROL_KEYS). Stripped from
# default_filters so the catalog only carries real column predicates.
_CONTROL_KEYS = frozenset(
    {
        "limit",
        "max_neighbors",
        "hops",
        "window_days",
        "transfer_window_days",
        "direction",
        "relation_types",
        "offset",
        "seed_ids",
    }
)

# Canonical GraphProfile field order — must match cerebro_mcp GraphProfile.
GRAPH_PROFILE_FIELDS = (
    "profile",
    "model_name",
    "relation_name",
    "source_column",
    "target_column",
    "source_kind",
    "target_kind",
    "directed",
    "time_column",
    "weight_column",
    "evidence_model",
    "evidence_source_column",
    "evidence_target_column",
    "node_enrichment_model",
    "node_enrichment_key",
    "default_filters",
    "module",
    "description",
    "semantic_status",
    "quality_tier",
    "question_synonyms",
    "semantic_source_file",
)


def _opt_str(value: Any) -> Optional[str]:
    return value if isinstance(value, str) and value else None


def extract_graph_profile_dict(name: str, model: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Extract a GraphProfile-shaped dict for the catalog (1:1 with mcp).

    Returns None when there is no enabled+complete graph block. Mirrors
    cerebro_mcp.semantic.graph_extraction.extract_graph_profile exactly (empty
    optional strings coerced to None, control keys stripped, evidence columns
    defaulting to source/target).
    """
    graph = (model.get("semantic", {}).get("meta", {}) or {}).get("graph")
    if not isinstance(graph, dict) or not graph.get("enabled"):
        return None
    if any(k not in graph for k in ("profile", "source_column", "target_column", "source_kind", "target_kind")):
        return None
    meta = (model.get("semantic", {}) or {}).get("meta") or {}
    src = graph["source_column"]
    tgt = graph["target_column"]
    return {
        "profile": graph["profile"],
        "model_name": name,
        "relation_name": model.get("relation_name", "") or name,
        "source_column": src,
        "target_column": tgt,
        "source_kind": graph["source_kind"],
        "target_kind": graph["target_kind"],
        "directed": bool(graph.get("directed", True)),
        "time_column": _opt_str(graph.get("time_column")),
        "weight_column": _opt_str(graph.get("weight_column")),
        "evidence_model": _opt_str(graph.get("evidence_model")),
        "evidence_source_column": _opt_str(graph.get("evidence_source_column")) or src,
        "evidence_target_column": _opt_str(graph.get("evidence_target_column")) or tgt,
        "node_enrichment_model": _opt_str(graph.get("node_enrichment_model")),
        "node_enrichment_key": _opt_str(graph.get("node_enrichment_key")),
        "default_filters": {
            k: v for k, v in (graph.get("default_filters") or {}).items() if k not in _CONTROL_KEYS
        },
        "module": model.get("module", "") or "",
        "description": model.get("description", "") or "",
        "semantic_status": model.get("semantic_status", "docs_only") or "docs_only",
        "quality_tier": model.get("quality_tier", "") or "",
        "question_synonyms": list(meta.get("question_synonyms") or ()),
        "semantic_source_file": model.get("semantic_source_file", "") or "",
    }


def _relationship_traversal_cost(rel: dict[str, Any], models: dict[str, Any]) -> float:
    """Cost of traversing a relationship (mirrors cerebro-mcp graph._edge_cost).

    Cheaper = preferred. many_to_one is the baseline; one_to_many fans out and is
    expensive; preferred bridges halve; cross-module hops add a small penalty.
    """
    base = {
        "many_to_one": 1.0,
        "one_to_one": 1.2,
        "many_to_many": 3.0,
        "one_to_many": 5.0,
    }.get(rel.get("cardinality", ""), 2.0)
    if rel.get("preferred_bridge"):
        base *= 0.5
    left = models.get(rel.get("left_model", ""), {}) or {}
    right = models.get(rel.get("right_model", ""), {}) or {}
    if left.get("module") and right.get("module") and left.get("module") != right.get("module"):
        base += 0.5
    return round(base, 4)


def _catalog_hash(catalog: dict[str, Any]) -> str:
    """Deterministic content hash, excluding the hash field itself."""
    payload = {k: v for k, v in catalog.items() if k != "metadata"}
    payload["_meta"] = {
        k: v for k, v in catalog.get("metadata", {}).items() if k != "graph_catalog_hash"
    }
    return sha256_bytes(json.dumps(payload, sort_keys=True, ensure_ascii=True).encode("utf-8"))


def build_graph_catalog(
    registry: dict[str, Any], *, graph_kinds: Optional[dict[str, Any]] = None
) -> dict[str, Any]:
    """Compile the published graph catalog from the registry. Deterministic."""
    models = registry.get("models", {})
    relationships = registry.get("relationships", [])
    metrics = registry.get("metrics", {})
    graph_kinds = graph_kinds or {}

    # profiles (1:1 with GraphProfile). First-wins on duplicate ids — the
    # validator/CI gate is the authoritative uniqueness check.
    profiles: dict[str, dict[str, Any]] = {}
    for name in sorted(models):
        row = extract_graph_profile_dict(name, models[name])
        if row and row["profile"] not in profiles:
            profiles[row["profile"]] = row

    kind_providers: dict[str, set[str]] = defaultdict(set)
    for prof in profiles.values():
        kind_providers[prof["source_kind"]].add(prof["profile"])
        kind_providers[prof["target_kind"]].add(prof["profile"])
    via_kinds = {r.get("via_entity") for r in relationships if r.get("via_entity")}

    node_types = []
    for kind in sorted(set(graph_kinds) | set(kind_providers) | via_kinds):
        meta = graph_kinds.get(kind, {}) or {}
        node_types.append(
            {
                "name": kind,
                "fqn": f"node:{kind}",
                "label": kind.replace("_", " ").title(),
                "description": meta.get("description", ""),
                "synonyms": list(meta.get("synonyms") or []),
                "parent_type": meta.get("parent_type"),
                "is_relationship_axis": bool(meta.get("is_relationship_axis", False)),
                "provider_profiles": sorted(kind_providers.get(kind, set())),
                "registered": kind in graph_kinds,
            }
        )

    edge_types = [
        {
            "name": prof["profile"],
            "source_kind": prof["source_kind"],
            "target_kind": prof["target_kind"],
            "directed": prof["directed"],
            "temporal": prof["time_column"] is not None,
            "weighted": prof["weight_column"] is not None,
        }
        for prof in sorted(profiles.values(), key=lambda p: p["profile"])
    ]

    def _ref(model_name: str) -> dict[str, Any]:
        mod = (models.get(model_name, {}) or {}).get("module", "")
        return {
            "type": "SemanticModel",
            "name": model_name,
            "fqn": f"{mod}.{model_name}" if mod else model_name,
        }

    join_edges = [
        {
            "relationship_id": r.get("name"),
            "left_model": _ref(r.get("left_model", "")),
            "right_model": _ref(r.get("right_model", "")),
            "left_keys": r.get("left_keys", []),
            "right_keys": r.get("right_keys", []),
            "via_entity": r.get("via_entity"),
            "cardinality": r.get("cardinality"),
            "quality_tier": r.get("quality_tier"),
            "preferred_bridge": bool(r.get("preferred_bridge", False)),
            "traversal_cost": _relationship_traversal_cost(r, models),
        }
        for r in sorted(relationships, key=lambda x: x.get("name", ""))
    ]

    profiles_by_model: dict[str, list[str]] = defaultdict(list)
    for prof in profiles.values():
        profiles_by_model[prof["model_name"]].append(prof["profile"])
    metric_bindings = {}
    for mname in sorted(metrics):
        metric = metrics[mname]
        related = sorted(profiles_by_model.get(metric.get("root_model", ""), []))
        if not related:
            continue
        metric_bindings[mname] = {
            "metric": mname,
            "root_model": metric.get("root_model"),
            "edge_types": related,
            "node_types": sorted(
                {
                    kind
                    for pid in related
                    for kind in (profiles[pid]["source_kind"], profiles[pid]["target_kind"])
                }
            ),
            "allowed_dimensions": metric.get("allowed_dimensions", []),
            "quality_tier": metric.get("quality_tier", ""),
        }

    search_documents = []
    for prof in profiles.values():
        body = " ".join(
            part
            for part in [
                prof["profile"],
                prof["profile"].replace("_", " "),
                prof["description"],
                " ".join(prof["question_synonyms"]),
                prof["model_name"],
                prof["source_kind"],
                prof["target_kind"],
            ]
            if part
        )
        search_documents.append(
            {
                "id": f"profile:{prof['profile']}",
                "type": "edge_type",
                "title": prof["profile"],
                "module": prof["module"],
                "quality_tier": prof["quality_tier"],
                "body": body,
                "payload_ref": prof["profile"],
            }
        )
    for nt in node_types:
        body = " ".join(
            part
            for part in [nt["name"], nt["name"].replace("_", " "), nt["description"], " ".join(nt["synonyms"])]
            if part
        )
        search_documents.append(
            {
                "id": f"node:{nt['name']}",
                "type": "node_type",
                "title": nt["name"],
                "module": "",
                "quality_tier": "",
                "body": body,
                "payload_ref": nt["name"],
            }
        )
    search_documents.sort(key=lambda d: d["id"])

    catalog = {
        "metadata": {
            "schema_version": GRAPH_CATALOG_SCHEMA_VERSION,
            "project_name": registry.get("metadata", {}).get("project_name", ""),
            "registry_manifest_hash": registry.get("metadata", {}).get("manifest_hash", ""),
            "node_type_count": len(node_types),
            "edge_type_count": len(edge_types),
            "profile_count": len(profiles),
            "join_edge_count": len(join_edges),
            "metric_binding_count": len(metric_bindings),
            "search_document_count": len(search_documents),
        },
        "node_types": node_types,
        "edge_types": edge_types,
        "profiles": profiles,
        "join_edges": join_edges,
        "metric_bindings": metric_bindings,
        "search_documents": search_documents,
    }
    catalog["metadata"]["graph_catalog_hash"] = _catalog_hash(catalog)
    return catalog


def validate_registry(
    registry: dict[str, Any],
    *,
    override_warnings: Optional[list[dict[str, Any]]] = None,
    allowed_kinds: Optional[set[str]] = None,
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
            # `grain` describes the time aggregation of a model. Point-in-time /
            # "_latest" snapshot models have no time dimension, so a grain is
            # semantically meaningless for them — exempt them from the grain
            # requirement rather than forcing a sentinel value.
            has_time_dimension = any(
                dim.get("type") == "time" for dim in model.get("dimensions", []) or []
            )
            required_meta = REQUIRED_APPROVED_META
            if not has_time_dimension:
                required_meta = tuple(f for f in required_meta if f != "grain")
            for field in required_meta:
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
            # Dimension/bridge tables (time spines, entity registries, edge
            # providers) are intentionally measure-less; `role: dimension` is
            # the author's explicit opt-out from the missing-measures check.
            if not model.get("measures") and semantic_meta.get("role") != "dimension":
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

    for model_name, model in models.items():
        for issue in validate_graph_meta(model_name, model, models, allowed_kinds=allowed_kinds):
            (errors if issue["severity"] == "error" else warnings).append(issue)

    # Entity overlay gate: every non-expression entity expr must be a real
    # column on its model (hand-authored AND generated — both are merged into
    # `entities` by now). A broken expr silently kills entity joins at runtime.
    for model_name, model in models.items():
        columns = set(model.get("columns", {}) or {})
        for entity in model.get("entities", []) or []:
            expr = str(entity.get("expr", "") or "")
            if not expr or _is_sql_expression(expr):
                continue
            if expr.strip("`") not in columns:
                errors.append(
                    {
                        "code": "entity_expr_unknown_column",
                        "severity": "error",
                        "model": model_name,
                        "message": (
                            f"Entity '{entity.get('name', '')}' on {model_name} points at "
                            f"expr='{expr}' which is not a column of the model"
                        ),
                    }
                )

    # Q4 — graph profile ids must be globally unique. build_graph_catalog is
    # first-wins on a collision, so an unchecked duplicate would silently drop an
    # edge from the catalog; surface it as a build error instead.
    profile_models: dict[str, list[str]] = defaultdict(list)
    for model_name, model in models.items():
        graph = (model.get("semantic", {}).get("meta", {}) or {}).get("graph")
        if isinstance(graph, dict) and graph.get("enabled") and graph.get("profile"):
            profile_models[graph["profile"]].append(model_name)
    for profile_id, owners in sorted(profile_models.items()):
        if len(owners) > 1:
            errors.append(
                {
                    "code": "graph_meta_duplicate_profile",
                    "severity": "error",
                    "model": sorted(owners)[0],
                    "message": (
                        f"graph profile id '{profile_id}' is declared by multiple models: "
                        f"{sorted(owners)}"
                    ),
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

    # Relationship names must be globally unique — the runtime and the
    # generated overlay both assume it, and a duplicate would let one edge
    # silently mask another. Mirrors graph_meta_duplicate_profile.
    relationship_name_owners: dict[str, int] = defaultdict(int)
    for relationship in relationships:
        relationship_name_owners[relationship.get("name", "")] += 1
    for rel_name, occurrences in sorted(relationship_name_owners.items()):
        if occurrences > 1:
            errors.append(
                {
                    "code": "relationship_duplicate_name",
                    "severity": "error",
                    "relationship": rel_name,
                    "message": (
                        f"relationship name '{rel_name}' is declared "
                        f"{occurrences} times; names must be unique"
                    ),
                }
            )

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
        # Join-key gate: every non-expression join key must exist on its
        # endpoint (backticks stripped). Skipped when the endpoint model is
        # unknown (already its own error above) or has NO column inventory —
        # several models have empty manifest+catalog columns and a missing
        # inventory is a docs gap, not evidence the key is wrong.
        for side, keys_field in (("left", "left_keys"), ("right", "right_keys")):
            endpoint_model = models.get(relationship.get(f"{side}_model", ""))
            if endpoint_model is None:
                continue
            endpoint_columns = set(endpoint_model.get("columns", {}) or {})
            if not endpoint_columns:
                continue
            for key in relationship.get(keys_field, []) or []:
                key_str = str(key)
                if _is_sql_expression(key_str):
                    continue
                if key_str.strip("`") not in endpoint_columns:
                    errors.append(
                        {
                            "code": "relationship_key_unknown_column",
                            "severity": "error",
                            "relationship": relationship.get("name", ""),
                            "model": relationship.get(f"{side}_model", ""),
                            "message": (
                                f"Relationship {relationship.get('name', '')} {keys_field} "
                                f"references '{key_str}' which is not a column of "
                                f"{relationship.get(f'{side}_model', '')}"
                            ),
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
        # Dead-edge guard: an approved relationship whose endpoint model is not
        # approved is silently pruned from the runtime join graph (cerebro-mcp
        # graph.py drops the edge). That regression class took 29 of 55 edges
        # offline before the join-graph activation wave — fail the build.
        if relationship.get("quality_tier") in APPROVED_STATUSES:
            for side, endpoint in (("left", left_model), ("right", right_model)):
                endpoint_model = models.get(endpoint)
                if (
                    endpoint_model is not None
                    and endpoint_model.get("semantic_status") != "approved"
                ):
                    errors.append(
                        {
                            "code": "relationship_endpoint_not_approved",
                            "severity": "error",
                            "relationship": relationship.get("name", ""),
                            "model": endpoint,
                            "message": (
                                f"Approved relationship {relationship.get('name', '')} has a "
                                f"non-approved {side} endpoint {endpoint} "
                                f"({endpoint_model.get('semantic_status')}) — the runtime "
                                "silently drops this edge; approve the model or demote the "
                                "relationship"
                            ),
                        }
                    )

    for metric_name, metric in metrics.items():
        # Ratio/derived metrics: computed FROM other metrics post-aggregation.
        # MVP contract: every input metric must exist in the registry AND all
        # inputs must share ONE root_model (cross-root derived metrics are not
        # supported — the MCP compiler can only render a same-branch computed
        # column). When an input is broken we `continue` past the generic
        # root-model checks: root_model is legitimately empty for a broken
        # derived metric and a second `metric_missing_root_model` error would
        # just be noise on top of the precise derived_metric_* error.
        metric_type = metric.get("type", "")
        if metric_type in DERIVED_METRIC_TYPES:
            type_params = metric.get("type_params", {}) or {}
            input_names = derived_metric_input_names(metric_type, type_params)
            derived_issue = False
            required_inputs = 2 if metric_type == "ratio" else 1
            if len(input_names) < required_inputs:
                requirement = (
                    "both type_params.numerator and type_params.denominator"
                    if metric_type == "ratio"
                    else "at least one input metric in type_params.metrics"
                )
                errors.append(
                    {
                        "code": "derived_metric_unknown_input",
                        "severity": "error",
                        "metric": metric_name,
                        "message": (
                            f"{metric_type.capitalize()} metric '{metric_name}' "
                            f"must declare {requirement}"
                        ),
                    }
                )
                derived_issue = True
            unknown_inputs = [name for name in input_names if name not in metrics]
            if unknown_inputs:
                errors.append(
                    {
                        "code": "derived_metric_unknown_input",
                        "severity": "error",
                        "metric": metric_name,
                        "inputs": unknown_inputs,
                        "message": (
                            f"{metric_type.capitalize()} metric '{metric_name}' "
                            f"references unknown input metric(s): {unknown_inputs}. "
                            "Every input must be a metric defined in the registry."
                        ),
                    }
                )
                derived_issue = True
            elif input_names:
                input_roots = sorted(
                    {metrics[name].get("root_model", "") for name in input_names}
                )
                if len(input_roots) > 1:
                    errors.append(
                        {
                            "code": "derived_metric_cross_root",
                            "severity": "error",
                            "metric": metric_name,
                            "root_models": input_roots,
                            "message": (
                                f"{metric_type.capitalize()} metric '{metric_name}' "
                                f"mixes inputs from multiple root models: {input_roots}. "
                                "Only same-root post-aggregation derived metrics are "
                                "supported — query the input metrics separately instead."
                            ),
                        }
                    )
                    derived_issue = True
            if derived_issue:
                continue

        # Ambiguous measure binding: the measure name this metric points at
        # is declared in 2+ semantic_models. `build_metrics` picks the first
        # one alphabetically so the registry stays deterministic, but the
        # author almost certainly meant just one of them — surface as an
        # error with a concrete rename suggestion. See the
        # `_ambiguous_measure_models` field set in build_metrics().
        ambiguous_models = metric.get("_ambiguous_measure_models") or []
        if len(ambiguous_models) > 1:
            errors.append(
                {
                    "code": "ambiguous_measure_binding",
                    "severity": "error",
                    "metric": metric_name,
                    "measure": metric.get("measure", ""),
                    "candidate_models": ambiguous_models,
                    "message": (
                        f"Metric '{metric_name}' references measure "
                        f"'{metric.get('measure', '')}' which is defined in "
                        f"{len(ambiguous_models)} semantic_models: "
                        f"{ambiguous_models}. Rename the measure on the "
                        f"intended source to be globally unique (e.g. "
                        f"'{metric_name}_value')."
                    ),
                }
            )

        # Missing measure: metric points at a measure that no semantic_model
        # declares at all. Likely a typo or a stale reference.
        if metric.get("measure") and not metric.get("root_model"):
            errors.append(
                {
                    "code": "missing_measure",
                    "severity": "error",
                    "metric": metric_name,
                    "measure": metric.get("measure", ""),
                    "message": (
                        f"Metric '{metric_name}' references measure "
                        f"'{metric.get('measure', '')}' which is not declared "
                        f"on any semantic_model. Check for typos or remove "
                        f"the orphaned metric definition."
                    ),
                }
            )

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

    # Spine-coverage ratchet: every approved metric root with a plain-column
    # time dimension should be reachable from the shared time spines (the
    # spine-blitz invariant). Mirrors the blitz filter exactly: measures
    # present, approved, bare-column time dim (expressions like
    # toDate(first_seen_at) can't be join keys and are exempt).
    spine_bridged = {
        relationship.get("right_model")
        for relationship in relationships
        if str(relationship.get("left_model", "")).startswith("dim_time_spine")
    } | {
        relationship.get("left_model")
        for relationship in relationships
        if str(relationship.get("right_model", "")).startswith("dim_time_spine")
    }
    for model_name, model in models.items():
        if model.get("resource_type") != "model" or model_name in spine_bridged:
            continue
        if model.get("semantic_status") != "approved" or not model.get("measures"):
            continue
        time_dims = [
            dimension
            for dimension in model.get("dimensions", []) or []
            if dimension.get("type") == "time"
        ]
        if not time_dims:
            continue
        grain = (time_dims[0].get("type_params") or {}).get("time_granularity", "day")
        if grain not in ("day", "week", "month"):
            continue
        time_col = str(time_dims[0].get("expr") or time_dims[0].get("name") or "")
        if not time_col.replace("_", "").isalnum():
            continue
        warnings.append(
            {
                "code": "approved_root_missing_time_spine",
                "severity": "warning",
                "model": model_name,
                "message": (
                    f"Approved metric root {model_name} has a {grain}-grain time "
                    f"dimension ({time_col}) but no dim_time_spine bridge in "
                    "semantic/relationships/ — cross-sector time-axis composition "
                    "will not reach it"
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
    parser.add_argument(
        "--max-warnings",
        type=int,
        default=None,
        help=(
            "With --validate: fail (exit 1) when validation warnings exceed N. "
            "The warning backlog was zeroed 2026-07 — CI pins 0 so it only "
            "ratchets down."
        ),
    )
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
        graph_kinds = load_graph_kinds(repo_root / "semantic" / "graph_kinds.yml")
        overrides, override_warnings = load_overrides(repo_root / "semantic" / "overrides")
        generated_entities = load_generated_entities(repo_root)
        entity_dictionary = load_entity_dictionary(repo_root)
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
            generated_entities=generated_entities,
            entity_dictionary=entity_dictionary,
        )
        validation = validate_registry(
            registry,
            override_warnings=override_warnings,
            allowed_kinds=set(graph_kinds) or None,
        )
        graph_catalog = build_graph_catalog(registry, graph_kinds=graph_kinds)
        # Stamp the catalog hash into the registry so consumers can detect a
        # catalog that is out of sync with the registry it was built from (M3).
        registry["metadata"]["graph_catalog_hash"] = graph_catalog["metadata"]["graph_catalog_hash"]
        dump_json(target_dir / "semantic_registry.json", registry)
        dump_json(target_dir / "semantic_validation_report.json", validation)
        dump_json(target_dir / "semantic_graph_catalog.json", graph_catalog)
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
    if (
        args.validate
        and args.max_warnings is not None
        and validation["warning_count"] > args.max_warnings
    ):
        print(
            f"validation warnings {validation['warning_count']} exceed "
            f"--max-warnings {args.max_warnings}"
        )
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
            "graph": {
                "node_type_count": graph_catalog["metadata"]["node_type_count"],
                "edge_type_count": graph_catalog["metadata"]["edge_type_count"],
                "profile_count": graph_catalog["metadata"]["profile_count"],
                "join_edge_count": graph_catalog["metadata"]["join_edge_count"],
                "search_document_count": graph_catalog["metadata"]["search_document_count"],
            },
        },
    )
    write_metrics(target_dir, summary)
    if status == "validation_failed":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
