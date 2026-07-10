"""Tests for the entity overlay (CS-1): generated-entities merge precedence,
registry publication of the entity dictionary, and the two new validation
gates (entity expr columns, relationship join keys)."""

from __future__ import annotations

from scripts.semantic import build_registry


def _manifest(nodes):
    return {"metadata": {"project_name": "gnosis_dbt"}, "nodes": nodes, "sources": {}}


def _model_node(name, columns, tags=None):
    return {
        "resource_type": "model",
        "package_name": "gnosis_dbt",
        "name": name,
        "description": f"{name} description.",
        "fqn": ["gnosis_dbt", "execution", name],
        "original_file_path": f"models/execution/{name}.sql",
        "config": {"materialized": "view", "meta": {"owner": "analytics_team"}},
        "tags": tags or [],
        "relation_name": f"`dbt`.`{name}`",
        "columns": {c: {"description": "", "data_type": t} for c, t in columns.items()},
    }


def _build(nodes, *, semantic_models=None, relationships=None,
           generated_entities=None, entity_dictionary=None):
    return build_registry.build_registry(
        manifest=_manifest(nodes),
        manifest_hash="mh",
        catalog={"nodes": {}, "sources": {}},
        catalog_hash="ch",
        semantic_manifest_hash="sh",
        semantic_models=semantic_models or {},
        authored_metrics={},
        relationships=relationships or [],
        overrides=[],
        generated_entities=generated_entities,
        entity_dictionary=entity_dictionary,
    )


def test_generated_entities_fill_unauthored_models():
    nodes = {
        "model.gnosis_dbt.fct_avatar_balances": _model_node(
            "fct_avatar_balances", {"avatar": "String", "balance": "Float64"}
        ),
    }
    registry = _build(
        nodes,
        generated_entities={
            "fct_avatar_balances": [
                {"name": "circles_avatar", "type": "foreign", "expr": "avatar"}
            ]
        },
    )
    entities = registry["models"]["fct_avatar_balances"]["entities"]
    assert entities == [{"name": "circles_avatar", "type": "foreign", "expr": "avatar"}]


def test_hand_authored_entities_win_wholesale():
    nodes = {
        "model.gnosis_dbt.fct_avatar_balances": _model_node(
            "fct_avatar_balances", {"avatar": "String", "balance": "Float64"}
        ),
    }
    authored = {
        "fct_avatar_balances": {
            "model": "ref('fct_avatar_balances')",
            "entities": [{"name": "circles_avatar", "type": "primary", "expr": "avatar"}],
        }
    }
    registry = _build(
        nodes,
        semantic_models=authored,
        generated_entities={
            "fct_avatar_balances": [
                {"name": "circles_avatar", "type": "foreign", "expr": "avatar"},
                {"name": "token", "type": "foreign", "expr": "balance"},
            ]
        },
    )
    entities = registry["models"]["fct_avatar_balances"]["entities"]
    # Hand-authored set exactly — the generated overlay must not merge in.
    assert entities == [{"name": "circles_avatar", "type": "primary", "expr": "avatar"}]


def test_entity_dictionary_published_in_registry():
    nodes = {"model.gnosis_dbt.m": _model_node("m", {"x": "String"})}
    dictionary = {
        "circles_avatar": {
            "hub_model": "hub",
            "sensitivity": "subject",
            "columns": ["avatar"],
        }
    }
    registry = _build(nodes, entity_dictionary=dictionary)
    assert registry["entity_dictionary"] == dictionary
    # Absent dictionary => empty mapping, never missing.
    assert _build(nodes)["entity_dictionary"] == {}


def test_entity_expr_unknown_column_is_error():
    nodes = {
        "model.gnosis_dbt.m": _model_node("m", {"avatar": "String"}),
    }
    registry = _build(
        nodes,
        generated_entities={
            "m": [{"name": "circles_avatar", "type": "foreign", "expr": "not_a_column"}]
        },
    )
    report = build_registry.validate_registry(registry)
    codes = [e["code"] for e in report["errors"]]
    assert "entity_expr_unknown_column" in codes


def test_entity_expr_expression_form_is_allowed():
    nodes = {
        "model.gnosis_dbt.m": _model_node("m", {"user_pseudonym": "UInt64"}),
    }
    registry = _build(
        nodes,
        generated_entities={
            "m": [
                {
                    "name": "user_pseudonym",
                    "type": "primary",
                    "expr": "toString(user_pseudonym)",
                }
            ]
        },
    )
    report = build_registry.validate_registry(registry)
    assert "entity_expr_unknown_column" not in [e["code"] for e in report["errors"]]


def test_relationship_key_unknown_column_is_error():
    nodes = {
        "model.gnosis_dbt.left_m": _model_node("left_m", {"date": "Date", "k": "String"}),
        "model.gnosis_dbt.right_m": _model_node("right_m", {"date": "Date"}),
    }
    rel = {
        "name": "left_to_right",
        "left_model": "left_m",
        "right_model": "right_m",
        "left_keys": ["k"],
        "right_keys": ["k"],  # right_m has no `k`
        "cardinality": "many_to_one",
        "join_semantics": "left",
        "quality_tier": "candidate",
    }
    registry = _build(nodes, relationships=[rel])
    report = build_registry.validate_registry(registry)
    hits = [e for e in report["errors"] if e["code"] == "relationship_key_unknown_column"]
    assert len(hits) == 1
    assert hits[0]["model"] == "right_m"


def test_relationship_key_gate_skips_models_without_column_inventory():
    nodes = {
        "model.gnosis_dbt.left_m": _model_node("left_m", {"k": "String"}),
        "model.gnosis_dbt.right_m": _model_node("right_m", {}),  # no columns known
    }
    rel = {
        "name": "left_to_right",
        "left_model": "left_m",
        "right_model": "right_m",
        "left_keys": ["k"],
        "right_keys": ["k"],
        "cardinality": "many_to_one",
        "join_semantics": "left",
        "quality_tier": "candidate",
    }
    registry = _build(nodes, relationships=[rel])
    report = build_registry.validate_registry(registry)
    assert "relationship_key_unknown_column" not in [e["code"] for e in report["errors"]]


def test_load_generated_entities_missing_file_is_empty(tmp_path):
    assert build_registry.load_generated_entities(tmp_path) == {}
    assert build_registry.load_entity_dictionary(tmp_path) == {}


# ---------------------------------------------------------------------------
# multi-binding: repeated entity names per model (from the new generator)
# ---------------------------------------------------------------------------


def test_repeated_entity_names_publish_and_validate():
    nodes = {
        "model.gnosis_dbt.fct_transfers": _model_node(
            "fct_transfers", {"from_address": "String", "to_address": "String"}
        ),
    }
    registry = _build(
        nodes,
        generated_entities={
            "fct_transfers": [
                {"name": "address", "type": "foreign", "expr": "from_address"},
                {"name": "address", "type": "foreign", "expr": "to_address"},
            ]
        },
    )
    # both bindings ride through as a plain list, no clobber
    assert registry["models"]["fct_transfers"]["entities"] == [
        {"name": "address", "type": "foreign", "expr": "from_address"},
        {"name": "address", "type": "foreign", "expr": "to_address"},
    ]
    report = build_registry.validate_registry(registry)
    assert "entity_expr_unknown_column" not in [e["code"] for e in report["errors"]]


def test_repeated_entity_bad_second_expr_still_errors():
    nodes = {
        "model.gnosis_dbt.fct_transfers": _model_node(
            "fct_transfers", {"from_address": "String"}
        ),
    }
    registry = _build(
        nodes,
        generated_entities={
            "fct_transfers": [
                {"name": "address", "type": "foreign", "expr": "from_address"},
                {"name": "address", "type": "foreign", "expr": "to_address"},  # absent col
            ]
        },
    )
    report = build_registry.validate_registry(registry)
    assert "entity_expr_unknown_column" in [e["code"] for e in report["errors"]]


def test_namespace_providers_deduped_for_multi_binding():
    nodes = {
        "model.gnosis_dbt.fct_transfers": _model_node(
            "fct_transfers", {"from_address": "String", "to_address": "String"}
        ),
    }
    registry = _build(
        nodes,
        generated_entities={
            "fct_transfers": [
                {"name": "address", "type": "foreign", "expr": "from_address"},
                {"name": "address", "type": "foreign", "expr": "to_address"},
            ]
        },
    )
    providers = registry["namespaces"]["address"]["providers"]
    assert [p["model"] for p in providers] == ["fct_transfers"]  # one, not two


def test_relationship_duplicate_name_is_error():
    nodes = {
        "model.gnosis_dbt.a": _model_node("a", {"k": "String"}),
        "model.gnosis_dbt.b": _model_node("b", {"k": "String"}),
    }
    rel = {
        "name": "dup_edge",
        "left_model": "a",
        "right_model": "b",
        "left_keys": ["k"],
        "right_keys": ["k"],
        "cardinality": "many_to_one",
        "join_semantics": "left",
        "quality_tier": "candidate",
    }
    registry = _build(nodes, relationships=[dict(rel), dict(rel)])
    report = build_registry.validate_registry(registry)
    hits = [e for e in report["errors"] if e["code"] == "relationship_duplicate_name"]
    assert len(hits) == 1 and hits[0]["relationship"] == "dup_edge"
