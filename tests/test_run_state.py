"""Unit tests for scripts/refresh/run_state.py — run-identity state files.

Covers the guarantees docs/lessons/refresh-state-collision.md relies on:
distinct selections get distinct state files, identity is stable across
invocations, pending-state discovery and overlap detection work, and writes
are atomic.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts" / "refresh"))

import run_state  # noqa: E402


def test_identity_stable_across_calls():
    fields = {"select": ["tag:tokens"], "exclude": None, "stage": None, "incremental_only": False}
    assert run_state.run_identity("full_refresh", fields) == run_state.run_identity(
        "full_refresh", dict(fields)
    )


def test_identity_differs_by_selection_and_tool():
    a = run_state.run_identity("full_refresh", {"select": ["tag:tokens"]})
    b = run_state.run_identity("full_refresh", {"select": ["tag:revenue"]})
    c = run_state.run_identity("microbatch", {"select": ["tag:tokens"]})
    assert len({a, b, c}) == 3
    assert all(len(x) == 12 for x in (a, b, c))


def test_identity_sensitive_to_mode_flags():
    base = {"select": ["m"], "exclude": None, "stage": None, "incremental_only": False}
    inc = dict(base, incremental_only=True)
    staged = dict(base, stage="usdc")
    assert len({
        run_state.run_identity("full_refresh", base),
        run_state.run_identity("full_refresh", inc),
        run_state.run_identity("full_refresh", staged),
    }) == 3


def test_state_path_shape(tmp_path):
    p = run_state.state_path(tmp_path, "microbatch", "abc123def456")
    assert p == tmp_path / "target" / "refresh_state" / "microbatch_abc123def456.json"


def test_save_load_clear_round_trip(tmp_path):
    rid = run_state.run_identity("full_refresh", {"select": ["m"]})
    p = run_state.state_path(tmp_path, "full_refresh", rid)
    st = run_state.new_state("full_refresh", rid, {"select": ["m"]}, ["m", "n"])
    st["completed_models"] = ["m"]
    run_state.save(p, st)
    loaded = run_state.load(p)
    assert loaded["run_id"] == rid
    assert loaded["models"] == ["m", "n"]
    assert loaded["completed_models"] == ["m"]
    assert loaded["updated_at"] >= loaded["created_at"]
    run_state.clear(p)
    assert run_state.load(p) is None


def test_save_is_atomic_no_tmp_left(tmp_path):
    p = run_state.state_path(tmp_path, "microbatch", "aaaabbbbcccc")
    run_state.save(p, {"completed": {}})
    run_state.save(p, {"completed": {"m::s": {"last_completed_end_date": "2026-07-01"}}})
    assert not p.with_suffix(".json.tmp").exists()
    assert json.loads(p.read_text())["completed"]["m::s"]["last_completed_end_date"] == "2026-07-01"


def test_load_corrupt_returns_none(tmp_path):
    p = run_state.state_path(tmp_path, "microbatch", "deadbeef0000")
    p.parent.mkdir(parents=True)
    p.write_text("{not json")
    assert run_state.load(p) is None


def test_pending_states_lists_and_filters_by_tool(tmp_path):
    for tool, rid, models in [
        ("full_refresh", "aaa111aaa111", ["m1", "m2"]),
        ("microbatch", "bbb222bbb222", ["m3"]),
    ]:
        p = run_state.state_path(tmp_path, tool, rid)
        run_state.save(p, run_state.new_state(tool, rid, {"select": models}, models))
    assert len(run_state.pending_states(tmp_path)) == 2
    only_fr = run_state.pending_states(tmp_path, "full_refresh")
    assert len(only_fr) == 1
    assert only_fr[0][1]["run_id"] == "aaa111aaa111"


def test_overlapping_detects_shared_models_and_excludes_self(tmp_path):
    rid_other = "ccc333ccc333"
    p = run_state.state_path(tmp_path, "full_refresh", rid_other)
    run_state.save(
        p, run_state.new_state("full_refresh", rid_other, {"select": ["tag:x"]}, ["m1", "m2"])
    )
    pending = run_state.pending_states(tmp_path)

    hits = run_state.overlapping(pending, ["m2", "m9"])
    assert len(hits) == 1
    assert hits[0][2] == ["m2"]

    assert run_state.overlapping(pending, ["m7"]) == []
    # A run never conflicts with its own pending state.
    assert run_state.overlapping(pending, ["m1"], exclude_run_id=rid_other) == []


def test_refresh_py_identity_fields_match_runner_shape():
    """The two runners must be able to see each other's pending runs via the
    shared module — sanity-check the id derivation both use."""
    fr = run_state.run_identity(
        "full_refresh",
        {"select": ["int_x"], "exclude": None, "stage": None, "incremental_only": False},
    )
    mb = run_state.run_identity("microbatch", {"select": "int_x", "stage": None})
    assert fr != mb
