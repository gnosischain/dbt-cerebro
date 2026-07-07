"""Smoke + consistency tests for generate_graph_diagram (WS10).

This tool renders the model-lineage diagram (model nodes + relationship edges)
for the docs site — a different graph from the entity-kind `semantic_graph_catalog`.
These tests keep it exercised (it is now a CI step, no longer manual-only) and
assert internal consistency: every edge endpoint is a node in the graph.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.semantic import generate_graph_diagram as gd

REPO_ROOT = Path(gd.__file__).resolve().parents[2]
REGISTRY = REPO_ROOT / "target" / "semantic_registry.json"


def _registry():
    if not REGISTRY.exists():
        pytest.skip("no built registry in target/")
    return json.loads(REGISTRY.read_text(encoding="utf-8"))


def test_build_graph_model_is_internally_consistent():
    reg = _registry()
    model = gd.build_graph_model(
        reg.get("relationships", []) or [],
        reg.get("models", {}) or {},
        reg.get("metrics", {}) or {},
    )
    assert model["nodes"], "diagram produced no nodes"
    node_ids = {n["id"] for n in model["nodes"]}
    # Every edge endpoint must be a node in the emitted graph (no dangling edges).
    for edge in model["edges"]:
        assert edge["source"] in node_ids, edge
        assert edge["target"] in node_ids, edge


def test_main_emits_graph_data_json(tmp_path):
    if not REGISTRY.exists():
        pytest.skip("no built registry in target/")
    out = tmp_path / "graph_data.json"
    rc = gd.main(["--target-dir", str(REGISTRY.parent), "--json-output", str(out)])
    assert rc == 0
    data = json.loads(out.read_text())
    assert "nodes" in data and "edges" in data
