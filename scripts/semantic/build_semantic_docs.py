#!/usr/bin/env python3
"""Generate static semantic docs pages and a docs index from the semantic registry."""

from __future__ import annotations

import argparse
import html
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.semantic.build_reporting import update_summary_section, write_metrics


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_text(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def page_template(title: str, body: str) -> str:
    safe_title = html.escape(title)
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>{safe_title}</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
      body {{ font-family: ui-sans-serif, system-ui, sans-serif; margin: 2rem auto; max-width: 1100px; padding: 0 1rem; line-height: 1.5; }}
      table {{ border-collapse: collapse; width: 100%; margin: 1rem 0; }}
      th, td {{ border: 1px solid #ddd; padding: 0.5rem; text-align: left; vertical-align: top; }}
      code {{ background: #f4f4f4; padding: 0.1rem 0.3rem; border-radius: 4px; }}
      .pill {{ display: inline-block; padding: 0.2rem 0.5rem; border-radius: 999px; background: #eef2ff; margin-right: 0.4rem; }}
    </style>
  </head>
  <body>
    {body}
  </body>
</html>
"""


def build_model_page(model: dict[str, Any]) -> str:
    columns = "".join(
        f"<tr><td><code>{html.escape(name)}</code></td><td>{html.escape(col.get('data_type', ''))}</td><td>{html.escape(col.get('description', ''))}</td></tr>"
        for name, col in model.get("columns", {}).items()
    )
    dimensions = "".join(
        f"<li><code>{html.escape(dimension.get('name', ''))}</code> ({html.escape(dimension.get('type', ''))})</li>"
        for dimension in model.get("dimensions", [])
    ) or "<li>None</li>"
    measures = "".join(
        f"<li><code>{html.escape(measure.get('name', ''))}</code> ({html.escape(measure.get('agg', ''))})</li>"
        for measure in model.get("measures", [])
    ) or "<li>None</li>"
    upstream = "".join(
        f"<li><code>{html.escape(item)}</code></li>"
        for item in model.get("lineage", {}).get("upstream", [])
    ) or "<li>None</li>"
    downstream = "".join(
        f"<li><code>{html.escape(item)}</code></li>"
        for item in model.get("lineage", {}).get("downstream", [])
    ) or "<li>None</li>"
    tags = " ".join(
        f"<span class='pill'>{html.escape(tag)}</span>"
        for tag in model.get("tags", [])
    ) or "<span class='pill'>no-tags</span>"
    body = f"""
    <h1>{html.escape(model['name'])}</h1>
    <p>{html.escape(model.get('description', ''))}</p>
    <p><strong>Module:</strong> {html.escape(model.get('module', ''))}</p>
    <p><strong>Status:</strong> {html.escape(model.get('semantic_status', ''))}</p>
    <p><strong>Owner:</strong> {html.escape(model.get('owner', ''))}</p>
    <p><strong>Path:</strong> <code>{html.escape(model.get('path', ''))}</code></p>
    <p><strong>Tags:</strong> {tags}</p>
    <h2>Columns</h2>
    <table><thead><tr><th>Name</th><th>Type</th><th>Description</th></tr></thead><tbody>{columns}</tbody></table>
    <h2>Dimensions</h2><ul>{dimensions}</ul>
    <h2>Measures</h2><ul>{measures}</ul>
    <h2>Upstream</h2><ul>{upstream}</ul>
    <h2>Downstream</h2><ul>{downstream}</ul>
    """
    return page_template(model["name"], body)


def build_metric_page(metric: dict[str, Any]) -> str:
    filters = json.dumps(metric.get("default_filters", []), indent=2)
    body = f"""
    <h1>{html.escape(metric['name'])}</h1>
    <p>{html.escape(metric.get('description', ''))}</p>
    <p><strong>Label:</strong> {html.escape(metric.get('label', ''))}</p>
    <p><strong>Module:</strong> {html.escape(metric.get('module', ''))}</p>
    <p><strong>Status:</strong> {html.escape(metric.get('semantic_status', ''))}</p>
    <p><strong>Root model:</strong> <code>{html.escape(metric.get('root_model', ''))}</code></p>
    <p><strong>Measure:</strong> <code>{html.escape(metric.get('measure', ''))}</code></p>
    <p><strong>Allowed dimensions:</strong> {", ".join(metric.get('allowed_dimensions', []))}</p>
    <p><strong>Supported time grains:</strong> {", ".join(metric.get('supported_time_grains', []))}</p>
    <h2>Default filters</h2>
    <pre>{html.escape(filters)}</pre>
    """
    return page_template(metric["name"], body)


def build_relationship_page(relationship: dict[str, Any]) -> str:
    body = f"""
    <h1>{html.escape(relationship.get('name', ''))}</h1>
    <p><strong>Left model:</strong> <code>{html.escape(relationship.get('left_model', ''))}</code></p>
    <p><strong>Right model:</strong> <code>{html.escape(relationship.get('right_model', ''))}</code></p>
    <p><strong>Cardinality:</strong> {html.escape(relationship.get('cardinality', ''))}</p>
    <p><strong>Join semantics:</strong> {html.escape(relationship.get('join_semantics', ''))}</p>
    <p><strong>Quality tier:</strong> {html.escape(relationship.get('quality_tier', ''))}</p>
    <p><strong>Entity bridge:</strong> {html.escape(relationship.get('via_entity', ''))}</p>
    """
    return page_template(relationship.get("name", "relationship"), body)


def build_namespace_page(name: str, namespace: dict[str, Any]) -> str:
    providers = "".join(
        f"<tr><td><code>{html.escape(provider['model'])}</code></td><td>{html.escape(provider['module'])}</td><td>{html.escape(provider['status'])}</td></tr>"
        for provider in namespace.get("providers", [])
    )
    body = f"""
    <h1>{html.escape(name)}</h1>
    <p><strong>Type:</strong> {html.escape(namespace.get('type', ''))}</p>
    <table><thead><tr><th>Provider</th><th>Module</th><th>Status</th></tr></thead><tbody>{providers}</tbody></table>
    """
    return page_template(name, body)


def build_module_page(name: str, module_summary: dict[str, Any], module_models: list[str]) -> str:
    models_html = "".join(f"<li><code>{html.escape(model_name)}</code></li>" for model_name in module_models)
    summary_json = json.dumps(module_summary, indent=2)
    body = f"""
    <h1>Module: {html.escape(name)}</h1>
    <pre>{html.escape(summary_json)}</pre>
    <h2>Models</h2>
    <ul>{models_html}</ul>
    """
    return page_template(f"Module {name}", body)


def build_graph_overview(registry: dict[str, Any]) -> str:
    coverage_json = json.dumps(registry.get("coverage_summary", {}), indent=2)
    body = f"""
    <h1>Semantic Graph Overview</h1>
    <p><strong>Project:</strong> {html.escape(registry['metadata'].get('project_name', ''))}</p>
    <pre>{html.escape(coverage_json)}</pre>
    """
    return page_template("Semantic Graph Overview", body)


def build_cross_module_explorer(registry: dict[str, Any]) -> str:
    module_list = "".join(
        f"<li><code>{html.escape(module_name)}</code></li>"
        for module_name in registry.get("modules", {})
    )
    body = f"""
    <h1>Cross-Module Semantic Explorer</h1>
    <p>Browse semantic assets across modules.</p>
    <ul>{module_list}</ul>
    """
    return page_template("Cross-Module Semantic Explorer", body)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-dir", default="target")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    target_dir = Path(args.target_dir)
    started_at = time.perf_counter()
    registry_path = target_dir / "semantic_registry.json"
    if not registry_path.exists():
        summary = update_summary_section(
            target_dir,
            "docs",
            {
                "status": "error",
                "elapsed_seconds": round(time.perf_counter() - started_at, 6),
                "error": f"Missing semantic registry: {registry_path}",
                "page_counts": {},
                "docs_index_count": 0,
            },
        )
        write_metrics(target_dir, summary)
        print(f"Missing semantic registry: {registry_path}", file=sys.stderr)
        return 2

    try:
        registry = load_json(registry_path)
        docs_root = target_dir / "semantic_docs"
        docs_index: list[dict[str, Any]] = []
        page_counts = {
            "model": 0,
            "metric": 0,
            "relationship": 0,
            "namespace": 0,
            "module": 0,
            "overview": 0,
        }

        models_by_module: dict[str, list[str]] = defaultdict(list)
        for model_name, model in registry.get("models", {}).items():
            models_by_module[model["module"]].append(model_name)
            path = docs_root / "models" / f"{model_name}.html"
            write_text(path, build_model_page(model))
            page_counts["model"] += 1
            docs_index.append(
                {
                    "uri": f"gnosis://semantic-model/{model_name}",
                    "type": "model",
                    "title": model_name,
                    "module": model["module"],
                    "keywords": [model_name, model["module"], model["semantic_status"]],
                    "path": str(path.relative_to(target_dir)),
                }
            )

        for metric_name, metric in registry.get("metrics", {}).items():
            path = docs_root / "metrics" / f"{metric_name}.html"
            write_text(path, build_metric_page(metric))
            page_counts["metric"] += 1
            docs_index.append(
                {
                    "uri": f"gnosis://semantic-metric/{metric_name}",
                    "type": "metric",
                    "title": metric_name,
                    "module": metric.get("module", ""),
                    "keywords": [metric_name, metric.get("label", ""), metric.get("module", "")],
                    "path": str(path.relative_to(target_dir)),
                }
            )

        for relationship in registry.get("relationships", []):
            relationship_name = relationship.get("name", "relationship")
            path = docs_root / "relationships" / f"{relationship_name}.html"
            write_text(path, build_relationship_page(relationship))
            page_counts["relationship"] += 1
            docs_index.append(
                {
                    "uri": f"gnosis://semantic-relationship/{relationship_name}",
                    "type": "relationship",
                    "title": relationship_name,
                    "module": "",
                    "keywords": [
                        relationship_name,
                        relationship.get("left_model", ""),
                        relationship.get("right_model", ""),
                    ],
                    "path": str(path.relative_to(target_dir)),
                }
            )

        for namespace_name, namespace in registry.get("namespaces", {}).items():
            path = docs_root / "namespaces" / f"{namespace_name}.html"
            write_text(path, build_namespace_page(namespace_name, namespace))
            page_counts["namespace"] += 1
            docs_index.append(
                {
                    "uri": f"gnosis://semantic-namespace/{namespace_name}",
                    "type": "namespace",
                    "title": namespace_name,
                    "module": "",
                    "keywords": [namespace_name, namespace.get("type", "")],
                    "path": str(path.relative_to(target_dir)),
                }
            )

        for module_name, module_summary in registry.get("modules", {}).items():
            path = docs_root / "modules" / f"{module_name}.html"
            write_text(
                path,
                build_module_page(
                    module_name,
                    module_summary,
                    sorted(models_by_module.get(module_name, [])),
                ),
            )
            page_counts["module"] += 1
            docs_index.append(
                {
                    "uri": f"gnosis://semantic-module/{module_name}",
                    "type": "module",
                    "title": module_name,
                    "module": module_name,
                    "keywords": [module_name, "module", "graph"],
                    "path": str(path.relative_to(target_dir)),
                }
            )

        graph_path = docs_root / "graph-overview.html"
        explorer_path = docs_root / "cross-module-explorer.html"
        write_text(graph_path, build_graph_overview(registry))
        write_text(explorer_path, build_cross_module_explorer(registry))
        page_counts["overview"] += 2
        docs_index.append(
            {
                "uri": "gnosis://semantic-graph-overview",
                "type": "overview",
                "title": "Semantic Graph Overview",
                "module": "",
                "keywords": ["semantic", "graph", "overview"],
                "path": str(graph_path.relative_to(target_dir)),
            }
        )
        docs_index.append(
            {
                "uri": "gnosis://semantic-cross-module-explorer",
                "type": "overview",
                "title": "Cross-Module Semantic Explorer",
                "module": "",
                "keywords": ["semantic", "cross-module", "explorer"],
                "path": str(explorer_path.relative_to(target_dir)),
            }
        )

        write_text(
            target_dir / "semantic_docs_index.json",
            json.dumps(docs_index, indent=2, sort_keys=True) + "\n",
        )
        summary = update_summary_section(
            target_dir,
            "docs",
            {
                "status": "success",
                "elapsed_seconds": round(time.perf_counter() - started_at, 6),
                "page_counts": page_counts,
                "docs_index_count": len(docs_index),
            },
        )
        write_metrics(target_dir, summary)
        return 0
    except Exception as exc:  # pragma: no cover - fatal path
        summary = update_summary_section(
            target_dir,
            "docs",
            {
                "status": "error",
                "elapsed_seconds": round(time.perf_counter() - started_at, 6),
                "error": str(exc),
                "page_counts": {},
                "docs_index_count": 0,
            },
        )
        write_metrics(target_dir, summary)
        print(f"Fatal semantic docs build error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
