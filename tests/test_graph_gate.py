"""Tests for the scoped graph-validation CI gate (WS8)."""

from __future__ import annotations

import json

from scripts.semantic import graph_gate


def _report(*errors):
    return {"errors": list(errors), "warnings": []}


def _err(code, model):
    return {"code": code, "severity": "error", "model": model, "message": f"{code} on {model}"}


def test_graph_errors_filters_to_graph_prefix():
    report = _report(
        _err("graph_meta_unknown_kind", "m1"),
        _err("missing_required_approved_meta", "m2"),
    )
    assert [e["code"] for e in graph_gate.graph_errors(report)] == ["graph_meta_unknown_kind"]


def test_net_new_blocks_unbaselined_graph_error():
    report = _report(_err("graph_meta_unknown_kind", "m1"))
    net_new = graph_gate.net_new_graph_errors(report, baseline=[])
    assert [e["model"] for e in net_new] == ["m1"]


def test_net_new_allows_baselined_graph_error():
    report = _report(_err("graph_meta_unknown_column", "m1"))
    baseline = [{"code": "graph_meta_unknown_column", "model": "m1"}]
    assert graph_gate.net_new_graph_errors(report, baseline) == []


def test_net_new_ignores_non_graph_errors():
    report = _report(_err("missing_required_approved_meta", "m9"))
    assert graph_gate.net_new_graph_errors(report, baseline=[]) == []


def test_baseline_match_is_by_identity_not_position():
    # Editing a model that already had a baselined error (so other errors shift
    # order) must NOT re-flag the baselined error as net-new (D6).
    report = _report(
        _err("graph_meta_unknown_kind", "new_model"),   # net-new
        _err("graph_meta_unknown_column", "legacy_model"),  # baselined
    )
    baseline = [{"code": "graph_meta_unknown_column", "model": "legacy_model"}]
    net_new = graph_gate.net_new_graph_errors(report, baseline)
    assert {e["model"] for e in net_new} == {"new_model"}


def test_load_baseline_accepts_dict_or_missing(tmp_path):
    assert graph_gate.load_baseline(tmp_path / "absent.json") == []
    p = tmp_path / "b.json"
    p.write_text(json.dumps({"graph_errors": [{"code": "graph_meta_x", "model": "m"}]}))
    assert graph_gate.load_baseline(p) == [{"code": "graph_meta_x", "model": "m"}]


def test_committed_baseline_is_loadable():
    from pathlib import Path

    repo_root = Path(graph_gate.__file__).resolve().parents[2]
    baseline = graph_gate.load_baseline(repo_root / "semantic" / "validation" / "baseline.json")
    assert isinstance(baseline, list)
