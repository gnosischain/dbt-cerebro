"""Tests for the multi-binding entity-overlay generator (compute_overlay).

Exercises the pure core with in-memory manifests — no dbt artifacts, no I/O.
"""

from __future__ import annotations

import pytest

from scripts.semantic.generate_entities import (
    Overrides,
    compute_overlay,
    parse_overrides,
)


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


def _node(name, cols, *, tags=None, materialized="table"):
    return {
        "resource_type": "model",
        "package_name": "gnosis_dbt",
        "name": name,
        "tags": tags or [],
        "config": {"materialized": materialized, "meta": {}},
        "columns": {c: {"data_type": "String", "description": ""} for c in cols},
    }


def _manifest(*nodes):
    return {
        "metadata": {"project_name": "gnosis_dbt"},
        "nodes": {f"model.gnosis_dbt.{n['name']}": n for n in nodes},
    }


def _dict_entry(entity, columns, hub=None, sensitivity="open"):
    return {"entity": entity, "hub_model": hub, "sensitivity": sensitivity, "columns": columns}


def _overlay(nodes, dictionary, *, overrides_raw=None, authored=None, catalog=None):
    valid = {e["entity"] for e in dictionary}
    ov = parse_overrides(overrides_raw or [], valid)
    return compute_overlay(
        _manifest(*nodes),
        {"nodes": catalog or {}},
        dictionary,
        ov,
        authored or {},
    )


def _entities(result, model):
    for a in result["annotations"]:
        if a["model"] == model:
            return a["entities"]
    return []


# ---------------------------------------------------------------------------
# multi-binding
# ---------------------------------------------------------------------------


def test_multi_binding_one_annotation_per_column():
    nodes = [_node("fct_transfers", ["from_address", "to_address", "operator", "amount"])]
    dictionary = [_dict_entry("address", ["from_address", "to_address", "operator"])]
    result = _overlay(nodes, dictionary)
    ents = _entities(result, "fct_transfers")
    exprs = sorted(e["expr"] for e in ents)
    assert exprs == ["from_address", "operator", "to_address"]
    assert all(e["name"] == "address" and e["type"] == "foreign" for e in ents)
    # entity_models carries one (model, col) pair per binding
    assert len(result["entity_models"]["address"]) == 3


def test_multi_binding_reported():
    nodes = [_node("fct_transfers", ["from_address", "to_address"])]
    dictionary = [_dict_entry("address", ["from_address", "to_address"])]
    result = _overlay(nodes, dictionary)
    mb = result["report"]["multi_binding"]
    assert mb and mb[0][0] == "fct_transfers" and mb[0][1] == "address"
    assert sorted(mb[0][2]) == ["from_address", "to_address"]


def test_edge_per_column_unique_names():
    hub = _node("hub_accounts", ["address"])
    spoke = _node("fct_transfers", ["from_address", "to_address"])
    dictionary = [
        _dict_entry("address", ["address", "from_address", "to_address"], hub="hub_accounts")
    ]
    result = _overlay([hub, spoke], dictionary)
    rels = result["relationships"]
    names = sorted(r["name"] for r in rels)
    assert names == [
        "gen_address__fct_transfers__from_address__to__hub_accounts",
        "gen_address__fct_transfers__to_address__to__hub_accounts",
    ]
    # each edge keys the spoke column on the left, the hub key on the right
    by_left = {r["left_keys"][0]: r for r in rels}
    assert set(by_left) == {"from_address", "to_address"}
    assert all(r["right_keys"] == ["address"] for r in rels)


# ---------------------------------------------------------------------------
# hub / primary selection
# ---------------------------------------------------------------------------


def test_primary_only_on_hub_key_column_no_self_edge():
    # hub carries the hub key (address) AND a second address-role column (owner)
    hub = _node("hub_accounts", ["address", "owner"])
    spoke = _node("fct_x", ["owner"])
    dictionary = [_dict_entry("address", ["address", "owner"], hub="hub_accounts")]
    result = _overlay([hub, spoke], dictionary)
    hub_ents = {e["expr"]: e["type"] for e in _entities(result, "hub_accounts")}
    assert hub_ents == {"address": "primary", "owner": "foreign"}
    # no self-edge even though the hub carries a foreign column of its own entity
    assert all(r["left_model"] != r["right_model"] for r in result["relationships"])
    # only the spoke produces an edge
    assert [r["left_model"] for r in result["relationships"]] == ["fct_x"]


def test_hub_missing_key_when_key_suppressed():
    hub = _node("hub_accounts", ["address"])
    dictionary = [_dict_entry("address", ["address"], hub="hub_accounts")]
    result = _overlay(
        [hub], dictionary,
        overrides_raw=[{"model": "hub_accounts", "column": "address", "entity": None}],
    )
    assert result["report"]["hub_missing_key"]
    assert not result["relationships"]


# ---------------------------------------------------------------------------
# overrides: redirect / suppress / additive
# ---------------------------------------------------------------------------


def test_override_redirect():
    nodes = [_node("contracts_registry", ["address"])]
    dictionary = [_dict_entry("address", ["address"]), _dict_entry("contract", ["contract_address"])]
    result = _overlay(
        nodes, dictionary,
        overrides_raw=[{"model": "contracts_registry", "column": "address", "entity": "contract"}],
    )
    ents = _entities(result, "contracts_registry")
    assert ents == [{"name": "contract", "type": "foreign", "expr": "address"}]


def test_override_suppress():
    nodes = [_node("fct_cow", ["pool_address"])]
    dictionary = [_dict_entry("pool", ["pool_address"])]
    result = _overlay(
        nodes, dictionary,
        overrides_raw=[{"model": "fct_cow", "column": "pool_address", "entity": None}],
    )
    assert _entities(result, "fct_cow") == []


def test_override_additive_column_not_in_any_global_list():
    nodes = [_node("fct_submodules", ["avatar_address"])]
    dictionary = [_dict_entry("safe", ["safe_address"])]
    result = _overlay(
        nodes, dictionary,
        overrides_raw=[{"model": "fct_submodules", "column": "avatar_address", "entity": "safe"}],
    )
    assert _entities(result, "fct_submodules") == [
        {"name": "safe", "type": "foreign", "expr": "avatar_address"}
    ]


# ---------------------------------------------------------------------------
# override validation (fatal) + report (deferred)
# ---------------------------------------------------------------------------


def test_parse_overrides_fatal_unknown_entity():
    with pytest.raises(SystemExit):
        parse_overrides([{"model": "m", "column": "c", "entity": "nope"}], {"address"})


def test_parse_overrides_fatal_duplicate_model_column():
    with pytest.raises(SystemExit):
        parse_overrides(
            [
                {"model": "m", "column": "c", "entity": "address"},
                {"model": "m", "column": "c", "entity": None},
            ],
            {"address"},
        )


def test_parse_overrides_fatal_missing_entity_key():
    with pytest.raises(SystemExit):
        parse_overrides([{"model": "m", "column": "c"}], {"address"})


def test_override_unmatched_reported():
    nodes = [_node("fct_real", ["address"])]
    dictionary = [_dict_entry("address", ["address"]), _dict_entry("contract", ["x"])]
    result = _overlay(
        nodes, dictionary,
        overrides_raw=[
            {"model": "ghost_model", "column": "address", "entity": "contract"},
            {"model": "fct_real", "column": "absent_col", "entity": "contract"},
        ],
    )
    reasons = {(m, c) for m, c, _why in result["report"]["override_unmatched"]}
    assert ("ghost_model", "address") in reasons
    assert ("fct_real", "absent_col") in reasons


def test_override_on_hand_authored_reported():
    nodes = [_node("fct_hand", ["address"])]
    dictionary = [_dict_entry("address", ["address"]), _dict_entry("contract", ["contract_address"])]
    authored = {"fct_hand": {"entities": [{"name": "address", "type": "primary", "expr": "address"}]}}
    result = _overlay(
        nodes, dictionary,
        overrides_raw=[{"model": "fct_hand", "column": "address", "entity": "contract"}],
        authored=authored,
    )
    assert ("fct_hand", "address") in [(m, c) for m, c in result["report"]["override_on_hand_authored"]]
    # hand-authored model gets no generated annotation
    assert _entities(result, "fct_hand") == []


def test_dev_tagged_models_are_skipped():
    nodes = [
        _node("api_real", ["address"]),
        _node("int_wip_v2", ["address"], tags=["dev", "intermediate", "v2"]),
    ]
    dictionary = [_dict_entry("address", ["address"])]
    result = _overlay(nodes, dictionary)
    assert _entities(result, "api_real") == [
        {"name": "address", "type": "foreign", "expr": "address"}
    ]
    assert _entities(result, "int_wip_v2") == []  # dev-tagged => skipped
    assert "int_wip_v2" in result["report"]["skipped_dev"]


def test_hand_authored_suggest_edit():
    nodes = [_node("fct_hand", ["avatar"])]
    dictionary = [_dict_entry("circles_avatar", ["avatar"])]
    authored = {"fct_hand": {"entities": [{"name": "token", "type": "primary", "expr": "avatar"}]}}
    result = _overlay(nodes, dictionary, authored=authored)
    assert _entities(result, "fct_hand") == []
    assert any(r[0] == "fct_hand" and r[1] == "circles_avatar" for r in result["report"]["suggest_hand_edit"])


# ---------------------------------------------------------------------------
# determinism (guards --check)
# ---------------------------------------------------------------------------


def test_determinism_across_insertion_order():
    a = _node("api_a", ["address"])
    b = _node("fct_b", ["from_address", "to_address"])
    hub = _node("hub_accounts", ["address"])
    dictionary = [
        _dict_entry("address", ["address", "from_address", "to_address"], hub="hub_accounts")
    ]
    r1 = _overlay([a, b, hub], dictionary)
    r2 = _overlay([hub, b, a], dictionary)
    assert r1["annotations"] == r2["annotations"]
    assert r1["relationships"] == r2["relationships"]
