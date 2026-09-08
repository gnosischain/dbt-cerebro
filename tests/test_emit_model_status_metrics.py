"""Unit tests for scripts/observability/emit_model_status_metrics.py.

Covers the runner-event surface: a stage refused by the microbatch runner
must report as status="refused" (overriding pending/success, never error),
carry its gap as a gauge, and watermark-read fallbacks must be visible.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts" / "observability"))

import emit_model_status_metrics as emitter  # noqa: E402


MODELS = {
    "contracts_sparse_events": {"materialization": "incremental", "layer": "contracts/x"},
    "int_partly_refused": {"materialization": "incremental", "layer": "execution/y"},
    "int_errored": {"materialization": "incremental", "layer": "execution/y"},
    "int_fine": {"materialization": "table", "layer": "execution/y"},
}


def _line(payload: str, prefix: str) -> list[str]:
    return [l for l in payload.splitlines() if l.startswith(prefix)]


def test_refused_overrides_pending_and_success_but_not_error():
    events = [
        {"runner_event": "refused", "model": "contracts_sparse_events", "stage": "_default", "gap_days": 69},
        {"runner_event": "refused", "model": "int_partly_refused", "stage": "band_2", "gap_days": 45},
        {"runner_event": "refused", "model": "int_errored", "stage": "_default", "gap_days": 31},
        {"runner_event": "refused", "model": "not_in_selection", "stage": "_default", "gap_days": 9},
    ]
    status = {"int_partly_refused": "success", "int_errored": "error", "int_fine": "success"}
    payload = emitter.build_payload(MODELS, status, {}, events)
    statuses = {}
    for l in _line(payload, "dbt_model_status{"):
        name = l.split('model="')[1].split('"')[0]
        statuses[name] = l.split('status="')[1].split('"')[0]
    assert statuses == {
        "contracts_sparse_events": "refused",   # never ran -> refused, not pending
        "int_partly_refused": "refused",        # one stage refused -> not advanced
        "int_errored": "error",                 # a real dbt error still wins
        "int_fine": "success",
    }
    assert "dbt_run_models_refused 2" in payload
    assert "dbt_run_models_success 1" in payload
    assert "dbt_run_models_pending 0" in payload
    gaps = _line(payload, "dbt_runner_refused_gap_days{")
    assert 'dbt_runner_refused_gap_days{model="contracts_sparse_events",stage="_default"} 69' in gaps
    assert 'dbt_runner_refused_gap_days{model="int_partly_refused",stage="band_2"} 45' in gaps
    assert not any("not_in_selection" in l for l in payload.splitlines())


def test_watermark_fallback_gauge_and_no_events_is_backward_compatible():
    events = [{"runner_event": "watermark-fallback", "model": "int_fine", "stage": "_default"}]
    payload = emitter.build_payload(MODELS, {"int_fine": "success"}, {}, events)
    assert 'dbt_runner_watermark_fallback{model="int_fine",stage="_default"} 1' in payload
    # status is untouched by a fallback (it is a warning, not a refusal)
    assert 'model="int_fine",materialization="table",layer="execution/y",status="success"' in payload
    baseline = emitter.build_payload(MODELS, {"int_fine": "success"}, {})
    assert "dbt_run_models_refused 0" in baseline
    assert _line(baseline, "dbt_runner_refused_gap_days{") == []


def test_load_runner_events_reads_only_runner_files(tmp_path):
    d = tmp_path / "failed_batches"
    d.mkdir()
    (d / "runner-refused-m1-_default-1-2.json").write_text(
        json.dumps({"runner_event": "refused", "model": "m1", "stage": "_default", "gap_days": 40})
    )
    (d / "microbatch-m2-_default-2026-09-07-1-2.json").write_text(json.dumps({"results": []}))
    (d / "runner-refused-broken.json").write_text("{not json")
    events = emitter.load_runner_events(str(d))
    assert [e["model"] for e in events] == ["m1"]
    assert emitter.load_runner_events(str(tmp_path / "missing")) == []
