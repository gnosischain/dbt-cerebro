#!/usr/bin/env python3
"""Generate a candidate metric for every eligible measure across ALL semantic
models, and uniquify measure names so the registry validator accepts them.

Why this exists
---------------
`build_registry.py` binds a metric to a measure **by name** and
`validate_registry` raises `ambiguous_measure_binding` when a metric points at
a measure name that exists on more than one semantic model. Today ~159 measure
names collide across the ~375 authored semantic models; that is tolerated only
because no metric references a collided name. The moment we add "a metric for
every measure", every measure that gets a metric must be globally unique.

Approach (minimal-churn, validation-safe)
-----------------------------------------
1. Scan every `semantic_models.yml`. A measure name that appears on exactly one
   model is already unique and is left untouched (so every existing
   hand-authored metric binding keeps working — those only ever bind to unique
   measures, which is why the repo validates today). Only **collided** measure
   names are rewritten to `<semantic_model_name>__<measure>`.
2. Emit one `candidate` (or model-tier-inherited) metric per eligible measure
   that does not already have a metric bound to it. id-like measures and gated
   models (blocked tier / `expose_to_mcp: false` / internal tags) are skipped.
3. Merge per-domain into each existing file: preserve `semantic_models`
   (with renamed measures) + existing `metrics`, append the generated metrics.

Re-runnable: running again after `dbt docs generate` is idempotent for an
unchanged set of models/measures.
"""

from __future__ import annotations

import argparse
import copy
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.semantic.build_registry import (  # noqa: E402
    PROJECT_NAME,
    canonical_status,
    get_cerebro_meta,
    iter_semantic_authoring,
    load_json,
    resolve_model_ref,
    semantic_authoring_roots,
)
from scripts.semantic.scaffold_candidates import (  # noqa: E402
    _humanize_model_name,
    _is_id_like,
    _write_yaml,
)

# Coarsening ladder for supported_time_grains: a metric at grain G supports G
# and everything coarser (you can always roll a finer grain up).
GRAIN_LADDER = ["hour", "day", "week", "month", "quarter", "year"]
BLOCKED_TIERS = {"blocked"}
INTERNAL_TAGS = {"internal_only", "privacy:tier_internal"}
AVERAGE_TOKENS = ("pct", "percent", "ratio", "apy", "price", "avg", "mean", "rate")
CUMULATIVE_TOKENS = ("cumulative", "running")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-dir", default="target")
    parser.add_argument(
        "--modules",
        default="",
        help="Comma-separated domain folder filter (e.g. mixpanel_ga,ESG). Default: all.",
    )
    parser.add_argument("--write", action="store_true", help="Write files in place.")
    return parser.parse_args(argv)


def _load_yaml(path: Path) -> dict[str, Any]:
    import yaml

    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _humanize_measure(measure_name: str) -> str:
    name = measure_name
    if name.endswith("_value"):
        name = name[: -len("_value")]
    return name.replace("__", " ").replace("_", " ").strip()


def _supported_time_grains(grain: str) -> list[str]:
    if grain in GRAIN_LADDER:
        return GRAIN_LADDER[GRAIN_LADDER.index(grain):]
    return ["day", "week", "month"]


def _model_grain(semantic_model: dict[str, Any]) -> str:
    """Time granularity of the model's agg_time_dimension, if any."""
    agg_time = (semantic_model.get("defaults") or {}).get("agg_time_dimension")
    meta_grain = get_cerebro_meta(semantic_model).get("grain")
    for dim in semantic_model.get("dimensions", []) or []:
        if dim.get("name") == agg_time and dim.get("type") == "time":
            tg = (dim.get("type_params") or {}).get("time_granularity")
            if tg:
                return str(tg)
    return str(meta_grain or "")


def _dimension_names(semantic_model: dict[str, Any]) -> list[str]:
    return [d.get("name") for d in semantic_model.get("dimensions", []) or [] if d.get("name")]


def _agg_note(measure_name: str, agg: str) -> str:
    lowered = measure_name.lower()
    if any(token in lowered for token in CUMULATIVE_TOKENS):
        return (
            " This is a cumulative/running-total measure: read the latest value "
            "in a period; do NOT sum across periods."
        )
    if str(agg).lower() in {"average", "avg"} or any(token in lowered for token in AVERAGE_TOKENS):
        return (
            " Read this as an average over the grain; do NOT sum it across "
            "rows or periods."
        )
    return ""


def _node_is_gated(node: Optional[dict[str, Any]]) -> bool:
    if not node:
        return False
    config_meta = (node.get("config", {}) or {}).get("meta", {}) or {}
    node_meta = node.get("meta", {}) or {}
    if config_meta.get("expose_to_mcp") is False or node_meta.get("expose_to_mcp") is False:
        return True
    tags = set(node.get("tags", []) or [])
    return bool(tags & INTERNAL_TAGS)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    manifest_path = Path(args.target_dir) / "manifest.json"
    if not manifest_path.exists():
        print(f"Missing manifest: {manifest_path}", file=sys.stderr)
        return 2
    manifest, _ = load_json(manifest_path)
    nodes_by_name: dict[str, dict[str, Any]] = {
        node["name"]: node
        for node in manifest.get("nodes", {}).values()
        if node.get("resource_type") == "model" and node.get("package_name") == PROJECT_NAME
    }

    modules_filter = {m.strip() for m in args.modules.split(",") if m.strip()}
    authoring_root = REPO_ROOT / "semantic" / "authoring"
    files = iter_semantic_authoring(*semantic_authoring_roots(REPO_ROOT))

    # ---- Pass 1: global measure-name frequency + existing metric bindings ----
    # `load_semantic_authoring` keys registry models by their resolved dbt model
    # (`semantic_models[resolved_model] = ...`), so when two semantic models
    # ref the SAME dbt model, only the LAST one in iteration order survives into
    # the registry — the loser's measures are never registered and a metric
    # bound to them fails validation (`missing_measure`). Mirror that last-wins
    # rule here so we only emit metrics for the surviving semantic model.
    measure_freq: Counter[str] = Counter()
    existing_metric_names: set[str] = set()
    bound_measures: set[str] = set()
    registry_winner: dict[str, tuple[Path, str]] = {}
    for path in files:
        payload = _load_yaml(path)
        for sm in payload.get("semantic_models", []) or []:
            if not isinstance(sm, dict):
                continue
            resolved = resolve_model_ref(sm.get("model"))
            if resolved:
                registry_winner[resolved] = (path, sm.get("name", ""))
            for measure in sm.get("measures", []) or []:
                if isinstance(measure, dict) and measure.get("name"):
                    measure_freq[measure["name"]] += 1
        for metric in payload.get("metrics", []) or []:
            if not isinstance(metric, dict):
                continue
            if metric.get("name"):
                existing_metric_names.add(metric["name"])
            bound = (metric.get("type_params") or {}).get("measure")
            if bound:
                bound_measures.add(bound)

    # Final (globally-unique) name for every (model, measure). Only collided
    # names get rewritten; unique names are preserved verbatim.
    used_final_names: set[str] = {n for n, c in measure_freq.items() if c == 1}

    def _final_measure_name(model_name: str, measure_name: str) -> str:
        if measure_freq[measure_name] == 1:
            return measure_name
        candidate = f"{model_name}__{measure_name}"
        if candidate not in used_final_names:
            used_final_names.add(candidate)
            return candidate
        suffix = 2
        while f"{candidate}_{suffix}" in used_final_names:
            suffix += 1
        resolved = f"{candidate}_{suffix}"
        used_final_names.add(resolved)
        return resolved

    reserved_metric_names: set[str] = set(existing_metric_names)

    def _unique_metric_name(base: str) -> str:
        name = base
        suffix = 2
        while name in reserved_metric_names:
            name = f"{base}_{suffix}"
            suffix += 1
        reserved_metric_names.add(name)
        return name

    # ---- Pass 2: per-file rename + metric generation ----
    total_renamed = 0
    total_generated = 0
    files_changed = 0
    for path in files:
        try:
            rel = path.relative_to(authoring_root)
            domain = rel.parts[0]
        except ValueError:
            domain = path.parent.name
        if modules_filter and domain not in modules_filter:
            continue

        payload = _load_yaml(path)
        semantic_models = payload.get("semantic_models", []) or []
        if not semantic_models:
            continue
        existing_metrics = list(payload.get("metrics", []) or [])
        generated_metrics: list[dict[str, Any]] = []
        file_renamed = 0

        for sm in semantic_models:
            if not isinstance(sm, dict):
                continue
            model_name = sm.get("name", "")
            resolved = resolve_model_ref(sm.get("model"))
            node = nodes_by_name.get(resolved or "")
            meta = get_cerebro_meta(sm)
            tier = canonical_status(meta.get("quality_tier"), default="candidate")
            # Skip models whose measures won't make it into the registry: gated
            # (blocked/internal) or shadowed by another semantic model on the
            # same dbt model (last-wins keying in load_semantic_authoring).
            is_winner = registry_winner.get(resolved or "") == (path, model_name)
            model_gated = tier in BLOCKED_TIERS or _node_is_gated(node) or not is_winner

            grain = _model_grain(sm)
            allowed_dimensions = _dimension_names(sm)
            supported_grains = _supported_time_grains(grain) if grain else []
            owner = meta.get("owner") or (node and (node.get("config", {}).get("meta", {}) or {}).get("owner")) or "analytics_team"
            human_model = _humanize_model_name(model_name)

            for measure in sm.get("measures", []) or []:
                if not isinstance(measure, dict) or not measure.get("name"):
                    continue
                old_name = measure["name"]
                final_name = _final_measure_name(model_name, old_name)
                if final_name != old_name:
                    measure["name"] = final_name
                    file_renamed += 1

                # Eligibility for metric generation.
                if model_gated:
                    continue
                if _is_id_like(old_name) or _is_id_like(measure.get("expr", old_name)):
                    continue
                if final_name in bound_measures or old_name in bound_measures:
                    continue  # an existing metric already binds this measure

                agg = str(measure.get("agg", "sum"))
                human_measure = _humanize_measure(old_name)
                metric_name = _unique_metric_name(final_name)
                description = (
                    f"{human_model} - {human_measure} ({agg})."
                    f"{_agg_note(old_name, agg)} Auto-generated candidate metric;"
                    " review and promote before relying on it."
                )
                cerebro: dict[str, Any] = {
                    "quality_tier": tier,
                    "owner": owner,
                }
                if grain:
                    cerebro["grain"] = grain
                # Fresh list copies per metric so yaml.safe_dump does not emit
                # shared anchors/aliases (&id / *id) across sibling metrics.
                if allowed_dimensions:
                    cerebro["allowed_dimensions"] = list(allowed_dimensions)
                if supported_grains:
                    cerebro["supported_time_grains"] = list(supported_grains)
                cerebro["question_synonyms"] = [
                    f"{human_model} {human_measure}".strip(),
                    human_measure,
                ]
                metric = {
                    "name": metric_name,
                    "label": f"{human_model.title()} - {human_measure.title()}",
                    "description": description,
                    "type": "simple",
                    "type_params": {"measure": final_name},
                    "config": {"meta": {"cerebro": cerebro}},
                }
                generated_metrics.append(metric)

        if file_renamed == 0 and not generated_metrics:
            continue

        new_payload = copy.deepcopy(payload)
        new_payload["semantic_models"] = semantic_models
        new_payload["metrics"] = existing_metrics + generated_metrics

        total_renamed += file_renamed
        total_generated += len(generated_metrics)
        files_changed += 1
        if args.write:
            _write_yaml(path, new_payload)
        print(
            "%s %s (+%d metrics, %d measures renamed)"
            % (
                "UPDATED" if args.write else "WOULD UPDATE",
                path.relative_to(REPO_ROOT),
                len(generated_metrics),
                file_renamed,
            )
        )

    print(
        "\n%s: %d files, +%d candidate metrics, %d measures uniquified"
        % (
            "WROTE" if args.write else "DRY-RUN",
            files_changed,
            total_generated,
            total_renamed,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
