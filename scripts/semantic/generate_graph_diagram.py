#!/usr/bin/env python3
"""Emit a Mermaid diagram of the semantic-layer cross-sector graph.

Reads the built registry at ``target/semantic_registry.json`` and produces
a richly-styled network visualization with three views:

1. **Unified semantic network** — every model that participates in at
   least one cross-sector relationship, grouped into sector subgraphs
   (revenue / consensus / execution / bridges / shared spines / ...).
   Edge style encodes the join axis:

   * thick green (===)  → ``user_pseudonym`` (cross-sector user overlap)
   * amber dashed (-.-) → time-spine bridges (``day`` / ``week`` / ``month``)
   * gray dotted (-..-) → other axes (``circles_avatar``, ``safe``,
                                       ``address``, ``validator``, ...)

2. **User-pseudonym subgraph** — zoom on the headline cross-sector
   capability with concentration-tier callouts.

3. **Time-spine star** — every weekly / monthly mart fanning out from
   the three ``dim_time_spine_*`` nodes.

Node labels include the metric count + dominant quality tier so the
diagram doubles as a coverage map.

Run from the repo root::

    python3 scripts/semantic/generate_graph_diagram.py \
            --target-dir target \
            --output ../cerebro-docs/docs/data-pipeline/transformation/semantic-layer/graph.md

Deterministic: same registry → same output. Re-run after every
``build_registry.py`` so the docs stay in sync.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


_TIME_SPINE_AXES = {"day", "week", "month"}


def _short_node(name: str) -> str:
    """Sanitize a model name for use as a Mermaid node id."""
    return name.replace(".", "_").replace("-", "_")


def _label(name: str) -> str:
    """Trim common prefixes for display so the diagram doesn't read like
    a wall of ``fct_execution_...``."""
    for prefix in (
        "fct_consensus_",
        "fct_execution_",
        "api_execution_",
        "int_execution_",
        "fct_revenue_",
        "fct_bridges_",
        "fct_",
        "api_",
        "int_",
        "stg_",
        "dim_",
    ):
        if name.startswith(prefix):
            return name[len(prefix):]
    return name


def _module_of(model_name: str, models: dict[str, dict]) -> str:
    info = models.get(model_name) or {}
    mod = info.get("module")
    if mod:
        return str(mod)
    # Heuristic fallback for nodes not in the registry's model map.
    if model_name.startswith("dim_time_spine_"):
        return "shared"
    for prefix, mod in (
        ("fct_consensus_", "consensus"),
        ("fct_revenue_", "revenue"),
        ("fct_bridges_", "bridges"),
        ("fct_execution_circles", "execution.circles"),
        ("fct_execution_gpay", "execution.gpay"),
        ("fct_execution_gnosis_app", "execution.gnosis_app"),
        ("fct_execution_cow", "execution.cow"),
        ("fct_execution_lending", "execution.lending"),
        ("fct_execution_yields", "execution.yields"),
        ("fct_execution_pools", "execution.pools"),
        ("fct_execution_tokens", "execution.tokens"),
        ("fct_execution_", "execution"),
        ("api_execution_", "execution"),
        ("api_bridges_", "bridges"),
        ("fct_p2p_", "p2p"),
    ):
        if model_name.startswith(prefix):
            return mod
    return "other"


# ──────────────────────────────────────────────────────────────────────
# Unified network diagram
# ──────────────────────────────────────────────────────────────────────


def _axis_class(axis: str) -> str:
    if axis == "user_pseudonym":
        return "pseudo"
    if axis in _TIME_SPINE_AXES:
        return "spine"
    return "other"


_MERMAID_INIT = (
    "%%{init: {"
    "'theme':'base',"
    "'flowchart':{'htmlLabels':true,'curve':'basis',"
    "'nodeSpacing':40,'rankSpacing':80,'padding':10,"
    "'subGraphTitleMargin':{'top':6,'bottom':6}},"
    "'themeVariables':{'fontSize':'15px','fontFamily':'-apple-system,Segoe UI,Roboto,sans-serif'}"
    "}}%%"
)
# Note: we DO NOT set useMaxWidth:false. Letting Mermaid auto-fit to the
# page width keeps the diagram from overflowing the mkdocs content area.
# Readability comes from filtering to fewer, more meaningful nodes (see
# _is_focused_node) rather than from forcing horizontal scroll.


def _is_production_mart(name: str) -> bool:
    """Production-facing marts (api_/fct_) and the time spines."""
    if name.startswith(("api_", "fct_", "dim_time_spine_")):
        return True
    return False


def _is_focused_node(
    name: str,
    pseudonym_nodes: set[str],
    spine_nodes: set[str],
    metric_counts: dict[str, int],
) -> bool:
    """Tighter filter for the headline unified diagram: keep a node only
    if it actually carries analytical signal — a metric, a spine, or a
    user-pseudonym graph participant. This drops production marts that
    exist purely as join endpoints (e.g. the cohort projections without
    their own metrics) so the diagram stays legible at default page width.
    """
    if name in pseudonym_nodes or name in spine_nodes:
        return True
    if metric_counts.get(name, 0) > 0:
        return True
    return False


def _emit_unified_network(
    rels: list[dict[str, Any]],
    models: dict[str, dict],
    metrics: dict[str, dict],
    *,
    production_only: bool,
    focused: bool = False,
) -> str:
    """One big network grouped into module subgraphs, with edges styled
    per axis.

    With ``production_only=True`` we drop ``int_*`` / ``stg_*`` nodes so
    the unified view stays legible — those live in a separate
    "auxiliary joins" diagram below.
    """

    # Filter relationships to those whose both ends pass the filter.
    if production_only:
        rels = [
            r for r in rels
            if _is_production_mart(r["left_model"])
            and _is_production_mart(r["right_model"])
        ]

    # Metric counts per model (used both for filtering and badge labels).
    pre_metric_counts: Counter[str] = Counter()
    for mname, m in metrics.items():
        root = m.get("root_model") or m.get("model")
        if root:
            pre_metric_counts[root] += 1

    pseudo_nodes_set: set[str] = {
        m
        for rel in rels
        if rel.get("via_entity") == "user_pseudonym"
        for m in (rel["left_model"], rel["right_model"])
    }
    spine_nodes_set: set[str] = {
        m
        for rel in rels
        for m in (rel["left_model"], rel["right_model"])
        if m.startswith("dim_time_spine_")
    }

    if focused:
        rels = [
            r for r in rels
            if _is_focused_node(r["left_model"], pseudo_nodes_set,
                                spine_nodes_set, pre_metric_counts)
            and _is_focused_node(r["right_model"], pseudo_nodes_set,
                                 spine_nodes_set, pre_metric_counts)
        ]

    # Collect participating nodes per module (after all filters applied).
    node_module: dict[str, str] = {}
    for rel in rels:
        for m in (rel["left_model"], rel["right_model"]):
            node_module[m] = _module_of(m, models)

    # Metric counts per model.
    metric_counts: Counter[str] = Counter()
    metric_tier: dict[str, str] = {}
    for mname, m in metrics.items():
        root = m.get("root_model") or m.get("model")
        if not root:
            continue
        metric_counts[root] += 1
        # promote "approved" if any metric is approved
        if m.get("quality_tier") == "approved" or metric_tier.get(root) != "approved":
            metric_tier[root] = m.get("quality_tier", "candidate")

    # Group nodes by module for subgraph rendering.
    by_module: dict[str, list[str]] = defaultdict(list)
    for n, mod in node_module.items():
        by_module[mod].append(n)

    # Outer flow is TB (subgraphs stacked top-to-bottom). Each subgraph
    # internally uses `direction LR` so its nodes form a horizontal row.
    # The natural aspect ratio is then tall+narrow — Mermaid's auto-fit
    # to container width then scales the diagram UP vertically rather
    # than squishing it. (With internal TB and outer TB, the engine ends
    # up placing subgraphs side-by-side to minimize cross-subgraph edge
    # length, producing the wide+short layout we want to avoid.)
    direction = "TB"
    inner_direction = "LR"
    lines: list[str] = ["```mermaid", _MERMAID_INIT, f"flowchart {direction}"]

    # Emit subgraphs.
    for mod in sorted(by_module):
        # Use a sanitized subgraph id; the label is the module name.
        sg_id = "sg_" + _short_node(mod)
        lines.append(f"    subgraph {sg_id}[\"{mod}\"]")
        lines.append(f"        direction {inner_direction}")
        for n in sorted(by_module[mod]):
            nid = _short_node(n)
            mc = metric_counts.get(n, 0)
            tier = metric_tier.get(n, "")
            badge_parts = []
            if mc:
                badge_parts.append(f"{mc} metric" + ("s" if mc != 1 else ""))
            if tier:
                badge_parts.append(tier)
            badge = (" / ".join(badge_parts))
            label = _label(n)
            if badge:
                label = f"{label}<br/><small>{badge}</small>"
            # Spine nodes get a stadium shape; the rest stay rectangular.
            if n.startswith("dim_time_spine_"):
                lines.append(f"        {nid}([\"{label}\"])")
            else:
                lines.append(f"        {nid}[\"{label}\"]")
        lines.append("    end")

    # Emit edges. Collapse weekly/monthly duplicates into a single
    # rendered edge per (l, r, axis) combination.
    seen: set[tuple[str, str, str]] = set()
    edges_by_axis: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for rel in rels:
        l, r = sorted((rel["left_model"], rel["right_model"]))
        axis = rel.get("via_entity", "")
        key = (l, r, axis)
        if key in seen:
            continue
        seen.add(key)
        edges_by_axis[axis].append((l, r))

    # User-pseudonym = thick green ===
    for l, r in sorted(edges_by_axis.get("user_pseudonym", [])):
        lines.append(f"    {_short_node(l)} === {_short_node(r)}")
    # Time-spine = dashed amber -.-
    for axis in sorted(_TIME_SPINE_AXES):
        for l, r in sorted(edges_by_axis.get(axis, [])):
            lines.append(f"    {_short_node(l)} -. {axis} .-> {_short_node(r)}")
    # Other axes = solid gray --
    for axis, edges in sorted(edges_by_axis.items()):
        if axis == "user_pseudonym" or axis in _TIME_SPINE_AXES:
            continue
        for l, r in sorted(edges):
            lines.append(f"    {_short_node(l)} -- {axis} --- {_short_node(r)}")

    # Node styling: distinguish spines, user-keyed marts, and the rest.
    lines.append(
        "    classDef spine fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#bf360c;"
    )
    lines.append(
        "    classDef pseudo fill:#e8f5e9,stroke:#2e7d32,stroke-width:1.5px,color:#1b5e20;"
    )
    lines.append(
        "    classDef other  fill:#eceff1,stroke:#455a64,stroke-width:1px,color:#263238;"
    )

    # Mark spine and user-pseudonym participants.
    spine_nodes = sorted({n for n in node_module if n.startswith("dim_time_spine_")})
    pseudo_nodes = sorted({
        m
        for rel in rels
        if rel.get("via_entity") == "user_pseudonym"
        for m in (rel["left_model"], rel["right_model"])
    })
    other_nodes = sorted(set(node_module) - set(spine_nodes) - set(pseudo_nodes))

    if spine_nodes:
        lines.append(
            "    class " + ",".join(_short_node(n) for n in spine_nodes) + " spine;"
        )
    if pseudo_nodes:
        lines.append(
            "    class " + ",".join(_short_node(n) for n in pseudo_nodes) + " pseudo;"
        )
    if other_nodes:
        lines.append(
            "    class " + ",".join(_short_node(n) for n in other_nodes) + " other;"
        )

    # Subgraph backgrounds — a soft tint per module so the network reads
    # as clusters rather than a soup of nodes.
    palette = [
        "#fff8e1", "#e3f2fd", "#f3e5f5", "#fce4ec", "#e0f7fa",
        "#f1f8e9", "#fff3e0", "#ede7f6", "#efebe9", "#fafafa",
    ]
    for i, mod in enumerate(sorted(by_module)):
        color = palette[i % len(palette)]
        sg_id = "sg_" + _short_node(mod)
        lines.append(f"    style {sg_id} fill:{color},stroke:#90a4ae,stroke-width:1px;")

    lines.append("```")
    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────
# Focused subgraph: user_pseudonym
# ──────────────────────────────────────────────────────────────────────


def _emit_pseudonym_graph(rels: list[dict[str, Any]]) -> str:
    nodes: set[str] = set()
    edges: list[tuple[str, str]] = []
    for rel in rels:
        l, r = rel["left_model"], rel["right_model"]
        nodes.add(l)
        nodes.add(r)
        edges.append(tuple(sorted((l, r))))

    lines = ["```mermaid", _MERMAID_INIT, "flowchart LR"]
    for n in sorted(nodes):
        lines.append(f"    {_short_node(n)}[\"{_label(n)}\"]")
    lines.append(
        "    classDef user fill:#e8f5e9,stroke:#2e7d32,stroke-width:1.5px,color:#1b5e20;"
    )
    if nodes:
        lines.append(
            "    class " + ",".join(_short_node(n) for n in sorted(nodes)) + " user;"
        )
    seen: set[tuple[str, str]] = set()
    for l, r in edges:
        if (l, r) in seen:
            continue
        seen.add((l, r))
        lines.append(f"    {_short_node(l)} === {_short_node(r)}")
    lines.append("```")
    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────
# Focused subgraph: time-spine star
# ──────────────────────────────────────────────────────────────────────


def _emit_time_spine_graph(rels: list[dict[str, Any]]) -> str:
    by_spine: dict[str, list[str]] = defaultdict(list)
    for rel in rels:
        spine = (
            rel["left_model"]
            if rel["left_model"].startswith("dim_time_spine_")
            else rel["right_model"]
        )
        other = (
            rel["right_model"]
            if rel["left_model"].startswith("dim_time_spine_")
            else rel["left_model"]
        )
        if not spine.startswith("dim_time_spine_"):
            continue
        by_spine[spine].append(other)

    lines = ["```mermaid", _MERMAID_INIT, "flowchart LR"]
    for spine in sorted(by_spine):
        lines.append(f"    {_short_node(spine)}([\"{_label(spine)}\"])")
    seen_others: set[str] = set()
    for others in by_spine.values():
        for o in others:
            if o in seen_others:
                continue
            seen_others.add(o)
            lines.append(f"    {_short_node(o)}[\"{_label(o)}\"]")
    for spine, others in sorted(by_spine.items()):
        for o in sorted(set(others)):
            lines.append(f"    {_short_node(spine)} -.-> {_short_node(o)}")
    if by_spine:
        lines.append(
            "    classDef spine fill:#fff3e0,stroke:#e65100,stroke-width:2px,color:#bf360c;"
        )
        lines.append(
            "    class " + ",".join(_short_node(s) for s in sorted(by_spine)) + " spine;"
        )
    lines.append("```")
    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────────
# Other-axis quick table
# ──────────────────────────────────────────────────────────────────────


def _emit_other_axis_table(
    rels: list[dict[str, Any]],
) -> str:
    skip = {"user_pseudonym"} | _TIME_SPINE_AXES
    rows: list[str] = []
    for rel in sorted(rels, key=lambda r: (r.get("via_entity", ""), r.get("name", ""))):
        axis = rel.get("via_entity", "")
        if axis in skip:
            continue
        rows.append(
            f"| `{axis}` | `{rel.get('name', '')}` | "
            f"`{_label(rel['left_model'])}` → `{_label(rel['right_model'])}` | "
            f"{rel.get('quality_tier', '')} |"
        )
    if not rows:
        return ""
    return "\n".join([
        "| Axis | Relationship | Models | Quality |",
        "| --- | --- | --- | --- |",
        *rows,
    ])


# ──────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────


def render(registry: dict[str, Any]) -> str:
    rels = registry.get("relationships", []) or []
    models = registry.get("models", {}) or {}
    metrics = registry.get("metrics", {}) or {}

    by_axis: dict[str, list[dict]] = defaultdict(list)
    for r in rels:
        by_axis[r.get("via_entity", "(unspecified)")].append(r)

    user_rels = by_axis.get("user_pseudonym", [])
    spine_axes = [
        r
        for r in rels
        if r.get("via_entity") in _TIME_SPINE_AXES
        or r["left_model"].startswith("dim_time_spine_")
        or r["right_model"].startswith("dim_time_spine_")
    ]

    approved = sum(1 for m in metrics.values() if m.get("quality_tier") == "approved")
    candidate = sum(1 for m in metrics.values() if m.get("quality_tier") == "candidate")

    parts: list[str] = []
    parts.append("# Semantic Graph")
    parts.append("")
    parts.append(
        "Auto-generated by `scripts/semantic/generate_graph_diagram.py` from "
        "`target/semantic_registry.json`. Do not edit by hand — re-run the "
        "generator after `build_registry.py`."
    )
    parts.append("")
    parts.append("## Coverage at a glance")
    parts.append("")
    parts.append(
        f"- **Approved metrics**: {approved} / {approved + candidate} total\n"
        f"- **Cross-sector relationships**: {len(rels)} total across "
        f"{len({a for a in by_axis if a != '(unspecified)'})} axes\n"
        f"- **User-pseudonym graph nodes**: "
        f"{len({m for r in user_rels for m in (r['left_model'], r['right_model'])})}\n"
        f"- **Time-spine bridges**: {len(spine_axes)} relationships joining "
        f"sector marts to `dim_time_spine_*`"
    )
    parts.append("")

    # ──────────────────────────────────────────────────────────────
    # 1) Unified semantic network
    # ──────────────────────────────────────────────────────────────
    parts.append("## Unified semantic network")
    parts.append("")
    parts.append(
        "Every model that participates in at least one cross-sector "
        "relationship, grouped into module subgraphs. Edge style "
        "encodes the join axis:"
    )
    parts.append("")
    parts.append(
        "- **===** (thick green) — `user_pseudonym` (cross-sector user overlap)\n"
        "- **-.→** (dashed) — time-spine bridge (`day` / `week` / `month`)\n"
        "- **— axis —** (gray) — other entity-specific joins "
        "(`circles_avatar`, `safe`, `address`, `validator`, ...)\n"
    )
    parts.append("")
    parts.append(
        "Nodes show the metric count and dominant quality tier so the "
        "diagram doubles as a coverage map. Spine nodes are stadium-"
        "shaped; user-keyed marts are tinted green."
    )
    parts.append("")
    parts.append(_emit_unified_network(
        rels, models, metrics,
        production_only=True,
        focused=True,
    ))
    parts.append("")
    parts.append(
        "> Filtered to production marts that either expose at least one "
        "metric, participate in the user-pseudonym graph, or are a time "
        "spine. Intermediate joins (`int_*` ↔ `int_*`) and "
        "production marts that exist solely as join endpoints are "
        "rendered in the **Auxiliary joins** section below."
    )
    parts.append("")

    # ──────────────────────────────────────────────────────────────
    # 2) User-pseudonym focus
    # ──────────────────────────────────────────────────────────────
    if user_rels:
        parts.append("## User-pseudonym subgraph (cross-sector user overlap)")
        parts.append("")
        parts.append(
            "Zoom on the headline cross-sector capability. Each node is "
            "a user-keyed mart that exposes `user_pseudonym` as a primary "
            "entity. Edges are equi-join relationships on the pseudonym."
        )
        parts.append("")
        parts.append(_emit_pseudonym_graph(user_rels))
        parts.append("")

    # ──────────────────────────────────────────────────────────────
    # 3) Time-spine star
    # ──────────────────────────────────────────────────────────────
    if spine_axes:
        parts.append("## Time-spine star (cross-grain composition)")
        parts.append("")
        parts.append(
            "The three time spines (`dim_time_spine_daily/weekly/monthly`) "
            "are the cross-sector join axis for time-series metrics. The "
            "planner synthesises a `toMonday(date)` / `toStartOfMonth(date)` "
            "upcast when grains differ (cerebro-mcp PR 5)."
        )
        parts.append("")
        parts.append(_emit_time_spine_graph(spine_axes))
        parts.append("")

    # ──────────────────────────────────────────────────────────────
    # 4) Auxiliary joins (intermediates)
    # ──────────────────────────────────────────────────────────────
    aux_rels = [
        r for r in rels
        if not (_is_production_mart(r["left_model"])
                and _is_production_mart(r["right_model"]))
    ]
    if aux_rels:
        parts.append("## Auxiliary joins (intermediate models)")
        parts.append("")
        parts.append(
            "Lower-level joins between `int_*` / `stg_*` models — the "
            "Graph Explorer 'suggested next hops' and address-axis "
            "lookups. These don't appear in the production-marts network "
            "above to keep that view readable; the planner still uses "
            "them for entity-specific enrichment."
        )
        parts.append("")
        parts.append(_emit_unified_network(aux_rels, models, metrics,
                                          production_only=False))
        parts.append("")

    other_table = _emit_other_axis_table(rels)
    if other_table:
        parts.append("## All cross-sector join axes")
        parts.append("")
        parts.append(
            "Smaller cross-sector axes — usually 1-2 edges each — for "
            "entity-specific joins (Circles avatars, Safe addresses, "
            "validator indices, raw EVM addresses)."
        )
        parts.append("")
        parts.append(other_table)
        parts.append("")

    return "\n".join(parts)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-dir", default="target",
                        help="Directory containing semantic_registry.json")
    parser.add_argument("--output", default=None,
                        help="Output markdown path. Defaults to stdout.")
    args = parser.parse_args(argv)

    registry_path = Path(args.target_dir) / "semantic_registry.json"
    if not registry_path.exists():
        print(f"ERROR: registry not found at {registry_path}", file=sys.stderr)
        print("Run `python3 scripts/semantic/build_registry.py --target-dir target` first.",
              file=sys.stderr)
        return 1
    registry = json.loads(registry_path.read_text(encoding="utf-8"))

    body = render(registry)
    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(body, encoding="utf-8")
        print(f"Wrote {out_path}", file=sys.stderr)
    else:
        sys.stdout.write(body)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
