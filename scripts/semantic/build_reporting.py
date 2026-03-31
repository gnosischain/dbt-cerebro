"""Shared semantic build reporting helpers."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SUMMARY_FILENAME = "semantic_build_summary.json"
METRICS_FILENAME = "semantic_build_metrics.prom"

REGISTRY_STATUSES = ("success", "validation_failed", "error", "not_run")
DOCS_STATUSES = ("success", "error", "not_run")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _summary_path(target_dir: Path) -> Path:
    return target_dir / SUMMARY_FILENAME


def _metrics_path(target_dir: Path) -> Path:
    return target_dir / METRICS_FILENAME


def load_summary(target_dir: Path) -> dict[str, Any]:
    path = _summary_path(target_dir)
    if not path.exists():
        return {
            "generated_at": utc_now(),
            "registry": {"status": "not_run"},
            "docs": {"status": "not_run"},
        }
    return json.loads(path.read_text(encoding="utf-8"))


def write_summary(target_dir: Path, summary: dict[str, Any]) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    summary["generated_at"] = utc_now()
    _summary_path(target_dir).write_text(
        json.dumps(summary, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )


def update_summary_section(
    target_dir: Path,
    section: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    summary = load_summary(target_dir)
    summary[section] = payload
    write_summary(target_dir, summary)
    return summary


def write_metrics(target_dir: Path, summary: dict[str, Any]) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    registry = summary.get("registry", {}) or {}
    docs = summary.get("docs", {}) or {}
    validation = registry.get("validation", {}) or {}
    coverage = registry.get("coverage", {}) or {}
    page_counts = docs.get("page_counts", {}) or {}

    lines = [
        "# HELP dbt_cerebro_semantic_registry_build_status Semantic registry build status.",
        "# TYPE dbt_cerebro_semantic_registry_build_status gauge",
    ]
    current_registry_status = registry.get("status", "not_run")
    for status in REGISTRY_STATUSES:
        value = 1 if status == current_registry_status else 0
        lines.append(
            'dbt_cerebro_semantic_registry_build_status{status="%s"} %s'
            % (status, value)
        )

    lines.extend(
        [
            "# HELP dbt_cerebro_semantic_registry_build_seconds Semantic registry build duration in seconds.",
            "# TYPE dbt_cerebro_semantic_registry_build_seconds gauge",
            "dbt_cerebro_semantic_registry_build_seconds %s"
            % registry.get("elapsed_seconds", 0.0),
            "# HELP dbt_cerebro_semantic_docs_generation_status Semantic docs generation status.",
            "# TYPE dbt_cerebro_semantic_docs_generation_status gauge",
        ]
    )

    current_docs_status = docs.get("status", "not_run")
    for status in DOCS_STATUSES:
        value = 1 if status == current_docs_status else 0
        lines.append(
            'dbt_cerebro_semantic_docs_generation_status{status="%s"} %s'
            % (status, value)
        )

    lines.extend(
        [
            "# HELP dbt_cerebro_semantic_docs_generation_seconds Semantic docs generation duration in seconds.",
            "# TYPE dbt_cerebro_semantic_docs_generation_seconds gauge",
            "dbt_cerebro_semantic_docs_generation_seconds %s"
            % docs.get("elapsed_seconds", 0.0),
            "# HELP dbt_cerebro_semantic_registry_models_total Semantic registry models by semantic status.",
            "# TYPE dbt_cerebro_semantic_registry_models_total gauge",
        ]
    )
    for status, count in sorted((coverage.get("semantic_status_counts") or {}).items()):
        lines.append(
            'dbt_cerebro_semantic_registry_models_total{semantic_status="%s"} %s'
            % (status, count)
        )

    lines.extend(
        [
            "# HELP dbt_cerebro_semantic_registry_metrics_total Semantic registry metrics by quality tier.",
            "# TYPE dbt_cerebro_semantic_registry_metrics_total gauge",
        ]
    )
    for quality, count in sorted((coverage.get("metric_quality_counts") or {}).items()):
        lines.append(
            'dbt_cerebro_semantic_registry_metrics_total{quality_tier="%s"} %s'
            % (quality, count)
        )

    lines.extend(
        [
            "# HELP dbt_cerebro_semantic_registry_relationships_total Semantic registry relationships by quality tier.",
            "# TYPE dbt_cerebro_semantic_registry_relationships_total gauge",
        ]
    )
    for quality, count in sorted((coverage.get("relationship_quality_counts") or {}).items()):
        lines.append(
            'dbt_cerebro_semantic_registry_relationships_total{quality_tier="%s"} %s'
            % (quality, count)
        )

    lines.extend(
        [
            "# HELP dbt_cerebro_semantic_validation_items_total Semantic validation items by severity.",
            "# TYPE dbt_cerebro_semantic_validation_items_total gauge",
            'dbt_cerebro_semantic_validation_items_total{severity="error"} %s'
            % validation.get("error_count", 0),
            'dbt_cerebro_semantic_validation_items_total{severity="warning"} %s'
            % validation.get("warning_count", 0),
            "# HELP dbt_cerebro_semantic_docs_pages_total Semantic docs pages by doc type.",
            "# TYPE dbt_cerebro_semantic_docs_pages_total gauge",
        ]
    )
    for doc_type, count in sorted(page_counts.items()):
        lines.append(
            'dbt_cerebro_semantic_docs_pages_total{doc_type="%s"} %s'
            % (doc_type, count)
        )

    _metrics_path(target_dir).write_text("\n".join(lines) + "\n", encoding="utf-8")
