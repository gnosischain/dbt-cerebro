"""Tests for the published graph catalog builder (WS4)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.semantic import build_registry

REPO_ROOT = Path(build_registry.__file__).resolve().parents[2]
SCHEMA_PATH = REPO_ROOT / "schemas" / "semantic_graph_catalog.schema.json"


def _graph_model(name, profile, src_kind, tgt_kind, **graph):
    g = {
        "enabled": True,
        "profile": profile,
        "source_column": "a",
        "target_column": "b",
        "source_kind": src_kind,
        "target_kind": tgt_kind,
    }
    g.update(graph)
    return {
        "name": name,
        "relation_name": name,
        "module": "execution",
        "description": f"{profile} desc",
        "semantic_status": "approved",
        "quality_tier": "approved",
        "semantic_source_file": "semantic/authoring/execution/semantic_models.yml",
        "columns": {"a": {}, "b": {}},
        "entities": [],
        "semantic": {"meta": {"question_synonyms": [profile], "graph": g}},
    }


def _registry():
    return {
        "metadata": {"project_name": "gnosis_dbt", "manifest_hash": "abc123"},
        "models": {
            "m_trust": _graph_model("m_trust", "circles_trust", "circles_avatar", "circles_avatar", time_column="a"),
            "m_lp": _graph_model("m_lp", "lp_in_pool", "address", "pool", weight_column="b"),
            "plain_model": {"name": "plain_model", "module": "execution", "columns": {}},
        },
        "relationships": [
            {
                "name": "lp_to_pool_meta",
                "left_model": "m_lp",
                "right_model": "m_trust",
                "left_keys": ["a"],
                "right_keys": ["b"],
                "via_entity": "address",
                "cardinality": "many_to_one",
                "quality_tier": "approved",
                "preferred_bridge": True,
            }
        ],
        "metrics": {
            "lp_volume": {"root_model": "m_lp", "allowed_dimensions": ["pool_address"], "quality_tier": "approved"},
        },
    }


def test_catalog_has_all_sections():
    cat = build_registry.build_graph_catalog(_registry())
    assert set(cat) == {
        "metadata", "node_types", "edge_types", "profiles", "join_edges",
        "metric_bindings", "search_documents",
    }
    assert cat["metadata"]["profile_count"] == 2
    assert set(cat["profiles"]) == {"circles_trust", "lp_in_pool"}


def test_catalog_profiles_match_graphprofile_field_set():
    cat = build_registry.build_graph_catalog(_registry())
    for prof in cat["profiles"].values():
        assert set(prof) == set(build_registry.GRAPH_PROFILE_FIELDS)
    # evidence columns default to source/target; optional strings coerced to None
    trust = cat["profiles"]["circles_trust"]
    assert trust["evidence_source_column"] == "a"
    assert trust["weight_column"] is None
    assert trust["time_column"] == "a"


def test_catalog_is_deterministic():
    reg = _registry()
    a = build_registry.build_graph_catalog(reg)
    b = build_registry.build_graph_catalog(reg)
    assert a["metadata"]["graph_catalog_hash"] == b["metadata"]["graph_catalog_hash"]
    assert json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)


def test_catalog_hash_excludes_itself():
    cat = build_registry.build_graph_catalog(_registry())
    stored = cat["metadata"].pop("graph_catalog_hash")
    recomputed = build_registry._catalog_hash(cat)
    assert stored == recomputed


def test_join_edge_uses_entity_reference_and_cost():
    cat = build_registry.build_graph_catalog(_registry())
    je = cat["join_edges"][0]
    assert je["left_model"]["type"] == "SemanticModel"
    assert je["left_model"]["fqn"] == "execution.m_lp"
    # preferred_bridge halves many_to_one (1.0) -> 0.5
    assert je["traversal_cost"] == 0.5


def test_metric_binding_links_to_profile():
    cat = build_registry.build_graph_catalog(_registry())
    assert "lp_volume" in cat["metric_bindings"]
    assert cat["metric_bindings"]["lp_volume"]["edge_types"] == ["lp_in_pool"]
    assert "pool" in cat["metric_bindings"]["lp_volume"]["node_types"]


def test_node_types_include_via_entity_axes():
    cat = build_registry.build_graph_catalog(_registry(), graph_kinds={"day": {"is_relationship_axis": True}})
    names = {nt["name"] for nt in cat["node_types"]}
    assert {"circles_avatar", "pool", "address"} <= names


def test_catalog_first_wins_on_duplicate_profile_id():
    reg = _registry()
    # Add a second model declaring the same profile id as m_trust.
    reg["models"]["m_trust_dupe"] = _graph_model(
        "m_trust_dupe", "circles_trust", "circles_avatar", "circles_avatar"
    )
    cat = build_registry.build_graph_catalog(reg)
    # The catalog must not contain two entries for the same profile id.
    assert list(cat["profiles"]).count("circles_trust") == 1
    assert cat["profiles"]["circles_trust"]["model_name"] == "m_trust"  # first by sort


def test_built_catalog_validates_against_committed_schema():
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(SCHEMA_PATH.read_text())
    cat = build_registry.build_graph_catalog(_registry())
    jsonschema.validate(cat, schema)


def test_real_catalog_validates_against_schema_if_present():
    jsonschema = pytest.importorskip("jsonschema")
    catalog_path = REPO_ROOT / "target" / "semantic_graph_catalog.json"
    if not catalog_path.exists():
        pytest.skip("no built catalog in target/")
    schema = json.loads(SCHEMA_PATH.read_text())
    jsonschema.validate(json.loads(catalog_path.read_text()), schema)
