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

    exit_code = build_registry.main(["--target-dir", str(target_dir), "--validate"])

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
