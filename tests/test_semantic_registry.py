from __future__ import annotations

import json

from scripts.semantic import build_registry, build_semantic_docs, scaffold_candidates


def _write_json(path, payload):
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _sample_manifest():
    return {
        "metadata": {"project_name": "gnosis_dbt"},
        "nodes": {
            "model.gnosis_dbt.api_execution_transactions_by_sector_daily": {
                "resource_type": "model",
                "package_name": "gnosis_dbt",
                "name": "api_execution_transactions_by_sector_daily",
                "description": "Daily transaction counts by sector.",
                "fqn": ["gnosis_dbt", "execution", "transactions", "api_execution_transactions_by_sector_daily"],
                "original_file_path": "models/execution/transactions/marts/api_execution_transactions_by_sector_daily.sql",
                "config": {"materialized": "view", "meta": {"owner": "analytics_team"}},
                "tags": ["execution"],
                "relation_name": "`dbt`.`api_execution_transactions_by_sector_daily`",
                "columns": {
                    "date": {"description": "Day", "data_type": "Date"},
                    "label": {"description": "Sector", "data_type": "String"},
                    "value": {"description": "Tx count", "data_type": "UInt64"},
                },
            },
            "model.gnosis_dbt.api_execution_transactions_fees_native_by_sector_daily": {
                "resource_type": "model",
                "package_name": "gnosis_dbt",
                "name": "api_execution_transactions_fees_native_by_sector_daily",
                "description": "Daily fees by sector.",
                "fqn": ["gnosis_dbt", "execution", "transactions", "api_execution_transactions_fees_native_by_sector_daily"],
                "original_file_path": "models/execution/transactions/marts/api_execution_transactions_fees_native_by_sector_daily.sql",
                "config": {"materialized": "view", "meta": {"owner": "analytics_team"}},
                "tags": ["execution"],
                "relation_name": "`dbt`.`api_execution_transactions_fees_native_by_sector_daily`",
                "columns": {
                    "date": {"description": "Day", "data_type": "Date"},
                    "label": {"description": "Sector", "data_type": "String"},
                    "value": {"description": "Fees", "data_type": "UInt64"},
                },
            },
            "model.gnosis_dbt.api_consensus_validators_active_daily": {
                "resource_type": "model",
                "package_name": "gnosis_dbt",
                "name": "api_consensus_validators_active_daily",
                "description": "Daily active validators.",
                "fqn": ["gnosis_dbt", "consensus", "marts", "api_consensus_validators_active_daily"],
                "original_file_path": "models/consensus/marts/api_consensus_validators_active_daily.sql",
                "config": {"materialized": "view", "meta": {"owner": "analytics_team"}},
                "tags": ["consensus"],
                "relation_name": "`dbt`.`api_consensus_validators_active_daily`",
                "columns": {
                    "date": {"description": "Day", "data_type": "Date"},
                    "cnt": {"description": "Validators", "data_type": "UInt64"},
                },
            },
            "model.gnosis_dbt.dim_time_spine_daily": {
                "resource_type": "model",
                "package_name": "gnosis_dbt",
                "name": "dim_time_spine_daily",
                "description": "Time spine.",
                "fqn": ["gnosis_dbt", "shared", "marts", "dim_time_spine_daily"],
                "original_file_path": "models/shared/marts/dim_time_spine_daily.sql",
                "config": {"materialized": "table", "meta": {"owner": "analytics_team"}},
                "tags": ["shared"],
                "relation_name": "`dbt`.`dim_time_spine_daily`",
                "columns": {
                    "day": {"description": "Day", "data_type": "Date"},
                },
            },
        },
        "sources": {},
        "parent_map": {
            "model.gnosis_dbt.api_execution_transactions_by_sector_daily": [],
            "model.gnosis_dbt.api_execution_transactions_fees_native_by_sector_daily": [],
            "model.gnosis_dbt.api_consensus_validators_active_daily": [],
            "model.gnosis_dbt.dim_time_spine_daily": [],
        },
        "child_map": {},
    }


def _sample_catalog():
    return {
        "nodes": {
            "model.gnosis_dbt.api_execution_transactions_by_sector_daily": {
                "columns": {
                    "date": {"type": "Date"},
                    "label": {"type": "String"},
                    "value": {"type": "UInt64"},
                }
            },
            "model.gnosis_dbt.api_execution_transactions_fees_native_by_sector_daily": {
                "columns": {
                    "date": {"type": "Date"},
                    "label": {"type": "String"},
                    "value": {"type": "UInt64"},
                }
            },
            "model.gnosis_dbt.api_consensus_validators_active_daily": {
                "columns": {
                    "date": {"type": "Date"},
                    "cnt": {"type": "UInt64"},
                }
            },
            "model.gnosis_dbt.dim_time_spine_daily": {
                "columns": {
                    "day": {"type": "Date"},
                }
            },
        },
        "sources": {},
    }


def test_build_registry_generates_outputs(tmp_path):
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    _write_json(target_dir / "manifest.json", _sample_manifest())
    _write_json(target_dir / "catalog.json", _sample_catalog())
    _write_json(target_dir / "semantic_manifest.json", {"semantic_models": {}, "metrics": {}})

    # NB: no `--validate` here. The validate pass reads the repo's full
    # `semantic/relationships/*.yml` and flags as errors every relationship
    # whose left/right model isn't present in `manifest.json`. The synthetic
    # fixture above intentionally only includes a handful of models, so a
    # validate pass would surface ~30 false positives. The CI workflow runs
    # `build_registry.py --validate` against the REAL manifest after
    # `dbt docs generate`, where every referenced model IS present — that's
    # the right place to enforce repo-wide invariants. This test exercises
    # the build pipeline's output shape only.
    exit_code = build_registry.main(["--target-dir", str(target_dir)])

    assert exit_code == 0
    registry = json.loads((target_dir / "semantic_registry.json").read_text(encoding="utf-8"))
    build_summary = json.loads((target_dir / "semantic_build_summary.json").read_text(encoding="utf-8"))
    build_metrics = (target_dir / "semantic_build_metrics.prom").read_text(encoding="utf-8")
    assert "api_execution_transactions_by_sector_daily" in registry["models"]
    assert registry["metrics"]["transaction_count"]["root_model"] == "api_execution_transactions_by_sector_daily"
    assert registry["coverage_summary"]["metric_count"] >= 3
    assert build_summary["registry"]["status"] == "success"
    assert "dbt_cerebro_semantic_registry_build_status" in build_metrics


def test_build_semantic_docs_generates_index(tmp_path):
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    _write_json(target_dir / "manifest.json", _sample_manifest())
    _write_json(target_dir / "catalog.json", _sample_catalog())
    _write_json(target_dir / "semantic_manifest.json", {"semantic_models": {}, "metrics": {}})
    assert build_registry.main(["--target-dir", str(target_dir)]) == 0

    exit_code = build_semantic_docs.main(["--target-dir", str(target_dir)])

    assert exit_code == 0
    docs_index = json.loads((target_dir / "semantic_docs_index.json").read_text(encoding="utf-8"))
    build_summary = json.loads((target_dir / "semantic_build_summary.json").read_text(encoding="utf-8"))
    build_metrics = (target_dir / "semantic_build_metrics.prom").read_text(encoding="utf-8")
    assert any(item["uri"] == "gnosis://semantic-model/api_execution_transactions_by_sector_daily" for item in docs_index)
    assert (target_dir / "semantic_docs" / "graph-overview.html").exists()
    assert build_summary["docs"]["status"] == "success"
    assert "dbt_cerebro_semantic_docs_generation_status" in build_metrics


def test_candidate_scaffold_heuristics_build_time_and_measure_fields():
    node = _sample_manifest()["nodes"][
        "model.gnosis_dbt.api_execution_transactions_by_sector_daily"
    ]

    candidate = scaffold_candidates._candidate_model(node)

    assert candidate["name"] == "execution_transactions_by_sector_daily"
    assert candidate["config"]["meta"]["cerebro"]["quality_tier"] == "candidate"
    assert candidate["defaults"]["agg_time_dimension"] == "date"
    assert candidate["dimensions"][0]["type"] == "time"
    assert candidate["measures"][0]["name"] == "value_value"


def test_unique_semantic_name_falls_back_to_full_model_name_on_collision():
    used_names = {"execution_transactions_by_sector_daily"}

    name = scaffold_candidates._unique_semantic_name(
        "fct_execution_transactions_by_sector_daily",
        used_names,
    )

    assert name == "fct_execution_transactions_by_sector_daily"


def test_agg_for_measure_uses_average_for_ratio_like_fields():
    assert scaffold_candidates._agg_for_measure("change_pct") == "average"
    assert scaffold_candidates._agg_for_measure("mean_value") == "average"
    assert scaffold_candidates._agg_for_measure("txs") == "sum"


# ─────────────────────────────────────────────────────────────────────
# Duplicate / missing measure validation
# ─────────────────────────────────────────────────────────────────────
# Regression tests for the build-time validator: catch authoring bugs
# where a metric points at a measure name that's declared by 2+
# semantic_models (silently picks the wrong root_model) or by no
# semantic_model at all (typo / stale ref).


def _registry_models_with_measures(measures_per_model: dict[str, list[str]]) -> dict:
    """Build a minimal registry_models dict for build_metrics() tests."""
    return {
        name: {
            "name": name,
            "module": "test_module",
            "resource_type": "model",
            "semantic_status": "approved",
            "measures": [{"name": m, "agg": "sum", "expr": m} for m in measures],
            "dimensions": [],
        }
        for name, measures in measures_per_model.items()
    }


def _authored_metric(name: str, measure: str, quality_tier: str = "approved") -> dict:
    return {
        "name": name,
        "label": name,
        "type": "simple",
        "type_params": {"measure": measure},
        "config": {"meta": {"cerebro": {"quality_tier": quality_tier}}},
    }


def test_build_metrics_picks_deterministic_root_when_measure_is_ambiguous():
    # `value_value` declared in TWO models. Previously last-write-wins
    # was non-deterministic; we now pick sorted-first so registry output
    # is stable across runs.
    registry_models = _registry_models_with_measures({
        "model_z": ["value_value"],
        "model_a": ["value_value"],
    })
    authored = {"my_metric": _authored_metric("my_metric", "value_value")}

    metrics = build_registry.build_metrics(authored, registry_models)

    assert metrics["my_metric"]["root_model"] == "model_a"  # sorted-first
    assert metrics["my_metric"]["_ambiguous_measure_models"] == ["model_a", "model_z"]


def test_build_metrics_no_ambiguity_flag_when_measure_unique():
    registry_models = _registry_models_with_measures({
        "model_a": ["unique_measure_value"],
    })
    authored = {"my_metric": _authored_metric("my_metric", "unique_measure_value")}

    metrics = build_registry.build_metrics(authored, registry_models)

    assert metrics["my_metric"]["root_model"] == "model_a"
    assert "_ambiguous_measure_models" not in metrics["my_metric"]


def _minimal_registry(metrics: dict, models: dict) -> dict:
    """Shape required by validate_registry()."""
    return {
        "models": models,
        "metrics": metrics,
        "relationships": [],
    }


def test_validate_flags_ambiguous_measure_binding():
    registry_models = _registry_models_with_measures({
        "model_z": ["value_value"],
        "model_a": ["value_value"],
    })
    authored = {"my_metric": _authored_metric("my_metric", "value_value")}
    metrics = build_registry.build_metrics(authored, registry_models)

    report = build_registry.validate_registry(
        _minimal_registry(metrics, registry_models)
    )

    ambig = [e for e in report["errors"] if e["code"] == "ambiguous_measure_binding"]
    assert len(ambig) == 1
    assert ambig[0]["metric"] == "my_metric"
    assert ambig[0]["measure"] == "value_value"
    assert ambig[0]["candidate_models"] == ["model_a", "model_z"]
    # Error message should suggest a concrete fix.
    assert "Rename the measure" in ambig[0]["message"]
    assert "my_metric_value" in ambig[0]["message"]


def test_validate_flags_missing_measure():
    registry_models = _registry_models_with_measures({
        "model_a": ["existing_measure"],
    })
    authored = {"orphan_metric": _authored_metric("orphan_metric", "nonexistent_measure")}
    metrics = build_registry.build_metrics(authored, registry_models)

    report = build_registry.validate_registry(
        _minimal_registry(metrics, registry_models)
    )

    missing = [e for e in report["errors"] if e["code"] == "missing_measure"]
    assert len(missing) == 1
    assert missing[0]["metric"] == "orphan_metric"
    assert missing[0]["measure"] == "nonexistent_measure"


def test_validate_no_false_positive_when_unique_measure_resolves():
    registry_models = _registry_models_with_measures({
        "model_a": ["unique_measure"],
    })
    authored = {"good_metric": _authored_metric("good_metric", "unique_measure")}
    metrics = build_registry.build_metrics(authored, registry_models)

    report = build_registry.validate_registry(
        _minimal_registry(metrics, registry_models)
    )

    # No ambiguous / missing errors for the metric itself.
    for err in report["errors"]:
        assert err.get("metric") != "good_metric", err


# ---------------------------------------------------------------------------
# Graph metadata validation + taxonomy registry (WS2/WS3)
# ---------------------------------------------------------------------------

from pathlib import Path  # noqa: E402

GRAPH_KINDS_PATH = (
    Path(build_registry.__file__).resolve().parents[2] / "semantic" / "graph_kinds.yml"
)

# Node kinds that appear as source_kind/target_kind in authored graph blocks.
# This guard fails if a kind is authored without being registered in
# graph_kinds.yml (which would surface at build time as graph_meta_unknown_kind).
_AUTHORED_GRAPH_KINDS = {
    "address",
    "bridge",
    "circles_avatar",
    "gpay_wallet",
    "pool",
    "project_label",
    "safe",
    "token",
    "validator",
}


def _graph_model(graph: dict, *, columns=("a", "b"), entities=()) -> dict:
    return {
        "columns": {c: {} for c in columns},
        "entities": [{"name": e} for e in entities],
        "semantic": {"meta": {"graph": graph}},
    }


def _enabled_graph(**overrides) -> dict:
    graph = {
        "enabled": True,
        "profile": "p1",
        "source_column": "a",
        "target_column": "b",
        "source_kind": "address",
        "target_kind": "token",
    }
    graph.update(overrides)
    return graph


def test_graph_ontology_doc_pages_render():
    catalog = {
        "metadata": {"schema_version": 1, "node_type_count": 1, "edge_type_count": 1, "join_edge_count": 0},
        "node_types": [
            {"name": "address", "fqn": "node:address", "description": "an account",
             "synonyms": ["wallet"], "provider_profiles": ["token_transfers"], "is_relationship_axis": False}
        ],
        "edge_types": [
            {"name": "token_transfers", "source_kind": "address", "target_kind": "address",
             "directed": True, "temporal": True, "weighted": False}
        ],
        "profiles": {"token_transfers": {"profile": "token_transfers", "source_kind": "address"}},
    }
    node_html = build_semantic_docs.build_node_type_page(catalog["node_types"][0], catalog)
    assert "Node type: address" in node_html and "token_transfers" in node_html
    edge_html = build_semantic_docs.build_edge_type_page(catalog["edge_types"][0], catalog)
    assert "Edge type: token_transfers" in edge_html
    overview = build_semantic_docs.build_ontology_overview(catalog)
    assert "Knowledge Graph Ontology" in overview and "address" in overview


def test_load_graph_kinds_covers_authored_kinds():
    kinds = build_registry.load_graph_kinds(GRAPH_KINDS_PATH)
    assert isinstance(kinds, dict) and kinds
    missing = _AUTHORED_GRAPH_KINDS - set(kinds)
    assert not missing, f"graph_kinds.yml missing authored kinds: {sorted(missing)}"


def test_load_graph_kinds_missing_file_returns_empty(tmp_path):
    assert build_registry.load_graph_kinds(tmp_path / "nope.yml") == {}


def test_validate_graph_meta_unknown_kind_is_error():
    model = _graph_model(_enabled_graph(source_kind="not_a_kind"))
    issues = build_registry.validate_graph_meta(
        "m", model, {"m": model}, allowed_kinds={"address", "token"}
    )
    codes = {i["code"] for i in issues if i["severity"] == "error"}
    assert "graph_meta_unknown_kind" in codes


def test_validate_graph_meta_known_kinds_no_unknown_kind_error():
    model = _graph_model(_enabled_graph())
    issues = build_registry.validate_graph_meta(
        "m", model, {"m": model}, allowed_kinds={"address", "token"}
    )
    assert "graph_meta_unknown_kind" not in {i["code"] for i in issues}


def test_validate_graph_meta_unknown_column_is_error():
    model = _graph_model(_enabled_graph(source_column="missing"))
    issues = build_registry.validate_graph_meta("m", model, {"m": model})
    assert "graph_meta_unknown_column" in {i["code"] for i in issues}


def test_validate_graph_meta_missing_required_is_error():
    graph = _enabled_graph()
    del graph["target_kind"]
    model = _graph_model(graph)
    issues = build_registry.validate_graph_meta("m", model, {"m": model})
    assert "graph_meta_missing_required" in {i["code"] for i in issues}


def test_validate_graph_meta_disabled_block_is_skipped():
    model = _graph_model({"enabled": False, "profile": "p"})
    assert build_registry.validate_graph_meta("m", model, {"m": model}) == []


def _typed_graph_model(graph: dict, columns: dict) -> dict:
    return {
        "resource_type": "model",
        "semantic_status": "candidate",
        "quality_tier": "candidate",
        "columns": columns,
        "entities": [],
        "dimensions": [],
        "measures": [],
        "semantic": {"meta": {"graph": graph}},
    }


def test_validate_graph_meta_weight_not_numeric_is_error():
    model = _typed_graph_model(
        _enabled_graph(weight_column="transfer_count"),
        {"a": {}, "b": {}, "transfer_count": {"data_type": "String"}},
    )
    issues = build_registry.validate_graph_meta("m", model, {"m": model})
    assert "graph_meta_weight_not_numeric" in {i["code"] for i in issues}


def test_validate_graph_meta_weight_numeric_ok():
    model = _typed_graph_model(
        _enabled_graph(weight_column="amount_usd"),
        {"a": {}, "b": {}, "amount_usd": {"data_type": "Float64"}},
    )
    issues = build_registry.validate_graph_meta("m", model, {"m": model})
    assert "graph_meta_weight_not_numeric" not in {i["code"] for i in issues}


def test_validate_graph_meta_unsafe_identifier_is_error():
    model = _typed_graph_model(
        _enabled_graph(source_column="a; DROP TABLE x"),
        {"a": {}, "b": {}},
    )
    issues = build_registry.validate_graph_meta("m", model, {"m": model})
    assert "graph_meta_unsafe_identifier" in {i["code"] for i in issues}


def test_validate_graph_meta_backtick_column_is_safe():
    model = _typed_graph_model(
        _enabled_graph(source_column="`from`"),
        {"`from`": {}, "b": {}},
    )
    issues = build_registry.validate_graph_meta("m", model, {"m": model})
    assert "graph_meta_unsafe_identifier" not in {i["code"] for i in issues}


def test_validate_registry_duplicate_profile_id_is_error():
    g = _enabled_graph()
    models = {
        "model_a": _typed_graph_model(dict(g), {"a": {}, "b": {}}),
        "model_b": _typed_graph_model(dict(g), {"a": {}, "b": {}}),
    }
    report = build_registry.validate_registry({"models": models, "metrics": {}, "relationships": []})
    dups = [e for e in report["errors"] if e["code"] == "graph_meta_duplicate_profile"]
    assert len(dups) == 1
    assert "model_a" in dups[0]["message"] and "model_b" in dups[0]["message"]


def test_infer_graph_meta_from_from_to_columns():
    cols = {"`from`": {}, "`to`": {}, "amount": {}}
    g = scaffold_candidates.infer_graph_meta("int_x_transfers", cols)
    assert g is not None
    assert g["profile"] == "token_transfers"
    assert g["enabled"] is False  # inert until reviewed
    assert g["source_column"] == "`from`" and g["target_column"] == "`to`"
    assert g["source_kind"] == "address" and g["target_kind"] == "address"


def test_infer_graph_meta_owner_safe_pattern():
    g = scaffold_candidates.infer_graph_meta("m", {"owner": {}, "safe_address": {}})
    assert g["profile"] == "safe_ownership"
    assert (g["source_kind"], g["target_kind"]) == ("address", "safe")


def test_infer_graph_meta_no_match_returns_none():
    assert scaffold_candidates.infer_graph_meta("m", {"x": {}, "y": {}}) is None


def test_candidate_model_includes_inferred_graph_block_as_review_required():
    node = {
        "name": "int_pools_lp",
        "columns": {"provider": {}, "pool_address": {}, "amount_usd": {"data_type": "Float64"}},
        "fqn": ["gnosis_dbt", "execution"],
    }
    model = scaffold_candidates._candidate_model(node)
    cerebro = model["config"]["meta"]["cerebro"]
    assert cerebro["graph"]["profile"] == "lp_in_pool"
    assert cerebro["graph"]["enabled"] is False
    assert cerebro["graph_review_required"] is True
    # The graph block must stay schema-clean (no unknown keys for the validator).
    assert set(cerebro["graph"]) <= build_registry.GRAPH_ALLOWED
