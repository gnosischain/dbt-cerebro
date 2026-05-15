"""Unit tests for scripts/refresh/dbt_incremental_runner.py.

These tests cover the pure-Python pieces of the microbatch runner: meta
parsing, slice-list generation, state file round-trip, max-date sentinel
parsing, command construction, and the plain/microbatch partitioning logic.
External dependencies (`dbt run`, `dbt run-operation`) are mocked so the
tests are hermetic.
"""

from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path
from unittest import mock

import pytest

# Make scripts/refresh importable.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts" / "refresh"))

import dbt_incremental_runner as runner  # noqa: E402


# ---------------------------------------------------------------------------
# get_microbatch_meta
# ---------------------------------------------------------------------------


def test_get_microbatch_meta_returns_none_for_plain_node():
    node = {"meta": {"full_refresh": {"start_date": "2024-01-01"}}}
    assert runner.get_microbatch_meta(node) is None


def test_get_microbatch_meta_disabled_block_returns_none():
    node = {
        "meta": {
            "full_refresh": {
                "incremental": {"enabled": False, "date_column": "date"}
            }
        }
    }
    assert runner.get_microbatch_meta(node) is None


def test_get_microbatch_meta_default_stage_when_absent():
    node = {
        "meta": {
            "full_refresh": {
                "incremental": {"enabled": True, "date_column": "date"}
            }
        }
    }
    meta = runner.get_microbatch_meta(node)
    assert meta is not None
    assert meta["date_column"] == "date"
    assert meta["batch_days"] == 1
    assert meta["stages"] == [{"name": "_default", "vars": {}, "start_date": None}]


def test_get_microbatch_meta_propagates_stages_and_batch_days():
    node = {
        "meta": {
            "full_refresh": {
                "stages": [
                    {"name": "a", "vars": {"x": 1}},
                    {"name": "b", "vars": {"x": 2}},
                ],
                "incremental": {
                    "enabled": True,
                    "date_column": "block_timestamp",
                    "batch_days": 3,
                },
            }
        }
    }
    meta = runner.get_microbatch_meta(node)
    assert meta["date_column"] == "block_timestamp"
    assert meta["batch_days"] == 3
    assert [s["name"] for s in meta["stages"]] == ["a", "b"]
    assert meta["stages"][0]["vars"] == {"x": 1}


def test_get_microbatch_meta_falls_back_to_config_meta():
    """Some manifests surface meta under config.meta rather than top-level."""
    node = {
        "config": {
            "meta": {
                "full_refresh": {
                    "incremental": {"enabled": True, "date_column": "date"}
                }
            }
        }
    }
    assert runner.get_microbatch_meta(node) is not None


# ---------------------------------------------------------------------------
# slice_end_dates
# ---------------------------------------------------------------------------


def test_slice_end_dates_step_1_dense():
    out = list(
        runner.slice_end_dates(
            dt.date(2026, 4, 18), dt.date(2026, 4, 21), 1
        )
    )
    assert out == [
        dt.date(2026, 4, 18),
        dt.date(2026, 4, 19),
        dt.date(2026, 4, 20),
        dt.date(2026, 4, 21),
    ]


def test_slice_end_dates_step_3_caps_on_end():
    out = list(
        runner.slice_end_dates(
            dt.date(2026, 4, 1), dt.date(2026, 4, 10), 3
        )
    )
    # 04-01, 04-04, 04-07 from the loop; cap 04-10 appended.
    assert out == [
        dt.date(2026, 4, 1),
        dt.date(2026, 4, 4),
        dt.date(2026, 4, 7),
        dt.date(2026, 4, 10),
    ]


def test_slice_end_dates_no_double_cap_when_aligned():
    """When start + k*step lands exactly on end, end is not duplicated."""
    out = list(
        runner.slice_end_dates(
            dt.date(2026, 4, 1), dt.date(2026, 4, 7), 3
        )
    )
    assert out == [
        dt.date(2026, 4, 1),
        dt.date(2026, 4, 4),
        dt.date(2026, 4, 7),
    ]


def test_slice_end_dates_single_day():
    out = list(
        runner.slice_end_dates(
            dt.date(2026, 4, 20), dt.date(2026, 4, 20), 1
        )
    )
    assert out == [dt.date(2026, 4, 20)]


def test_slice_end_dates_empty_when_start_past_end():
    assert (
        list(
            runner.slice_end_dates(
                dt.date(2026, 4, 25), dt.date(2026, 4, 20), 1
            )
        )
        == []
    )


def test_slice_end_dates_rejects_non_positive_step():
    with pytest.raises(ValueError):
        list(runner.slice_end_dates(dt.date(2026, 4, 1), dt.date(2026, 4, 5), 0))


def test_slice_end_dates_crosses_month_boundary():
    out = list(
        runner.slice_end_dates(
            dt.date(2026, 1, 30), dt.date(2026, 2, 2), 1
        )
    )
    assert out == [
        dt.date(2026, 1, 30),
        dt.date(2026, 1, 31),
        dt.date(2026, 2, 1),
        dt.date(2026, 2, 2),
    ]


# ---------------------------------------------------------------------------
# State file
# ---------------------------------------------------------------------------


def test_state_round_trip(tmp_path):
    sf = tmp_path / "state.json"
    assert runner.load_state(sf) == {"completed": {}}
    state = {"completed": {"m::s": {"last_completed_end_date": "2026-04-20"}}}
    runner.save_state(sf, state)
    assert runner.load_state(sf) == state


def test_state_atomic_replace(tmp_path):
    sf = tmp_path / "state.json"
    runner.save_state(sf, {"completed": {}})
    # Write twice; tmp file should not linger.
    runner.save_state(sf, {"completed": {"m::s": {"last_completed_end_date": "2026-04-21"}}})
    assert not (tmp_path / "state.json.tmp").exists()


def test_state_corrupt_file_resets(tmp_path):
    sf = tmp_path / "state.json"
    sf.write_text("{not json")
    assert runner.load_state(sf) == {"completed": {}}


# ---------------------------------------------------------------------------
# fetch_max_date — sentinel parsing
# ---------------------------------------------------------------------------


def _completed_proc(stdout: str = "", stderr: str = "", returncode: int = 0):
    return mock.Mock(stdout=stdout, stderr=stderr, returncode=returncode)


def test_fetch_max_date_parses_sentinel(tmp_path):
    sentinel = "12:00:00 MAX_DATE_RESULT::int_x::date::2026-04-20\n"
    with mock.patch("subprocess.run", return_value=_completed_proc(stdout=sentinel)):
        out = runner.fetch_max_date("int_x", "date", tmp_path, tmp_path)
    assert out == dt.date(2026, 4, 20)


def test_fetch_max_date_picks_correct_sentinel_when_many():
    """Multiple models in a single dbt run output → match by name."""
    stdout = (
        "MAX_DATE_RESULT::int_y::date::2020-01-01\n"
        "MAX_DATE_RESULT::int_x::date::2026-04-22\n"
    )
    with mock.patch("subprocess.run", return_value=_completed_proc(stdout=stdout)):
        out = runner.fetch_max_date("int_x", "date", Path("."), Path("."))
    assert out == dt.date(2026, 4, 22)


def test_fetch_max_date_raises_when_missing():
    with mock.patch(
        "subprocess.run",
        return_value=_completed_proc(stdout="nothing useful here", returncode=1, stderr="boom"),
    ):
        with pytest.raises(RuntimeError):
            runner.fetch_max_date("int_x", "date", Path("."), Path("."))


# ---------------------------------------------------------------------------
# run_one_slice — command construction
# ---------------------------------------------------------------------------


def test_run_one_slice_constructs_expected_command(tmp_path):
    captured: dict = {}

    def fake_run(cmd):
        captured["cmd"] = cmd
        return mock.Mock(returncode=0)

    with mock.patch("subprocess.run", side_effect=fake_run):
        rc = runner.run_one_slice(
            "int_x",
            {"name": "stage_a", "vars": {"validator_index_start": 0, "validator_index_end": 100}},
            dt.date(2026, 4, 20),
            ["--defer", "--state", "/state"],
            tmp_path,
            tmp_path,
            threads=2,
        )
    assert rc == 0
    cmd = captured["cmd"]
    assert cmd[0:4] == ["dbt", "run", "--select", "int_x"]
    # vars JSON contains all three keys.
    vars_idx = cmd.index("--vars")
    payload = json.loads(cmd[vars_idx + 1])
    assert payload == {
        "incremental_end_date": "2026-04-20",
        "validator_index_start": 0,
        "validator_index_end": 100,
    }
    # Defer flags forwarded.
    assert "--defer" in cmd
    assert ["--state", "/state"] == cmd[cmd.index("--state") : cmd.index("--state") + 2]
    # Threads forwarded.
    assert ["--threads", "2"] == cmd[cmd.index("--threads") : cmd.index("--threads") + 2]


# ---------------------------------------------------------------------------
# partition_selected
# ---------------------------------------------------------------------------


def _manifest_with(*models):
    """Build a tiny manifest from (name, meta_full_refresh_dict | None) tuples."""
    nodes = {}
    for name, meta in models:
        nodes[f"model.gnosis_dbt.{name}"] = {
            "resource_type": "model",
            "name": name,
            "depends_on": {"nodes": []},
            "meta": {"full_refresh": meta} if meta else {},
            "config": {},
            "original_file_path": f"models/{name}.sql",
        }
    return {"nodes": nodes}


def test_partition_selected_emits_topo_ordered_entries():
    """partition_selected returns a single ordered sequence of
    ('plain'|'micro', name, node, meta|None) preserving the input order."""
    manifest = _manifest_with(
        ("plain_a", None),
        (
            "micro_a",
            {
                "incremental": {
                    "enabled": True,
                    "date_column": "date",
                    "batch_days": 1,
                }
            },
        ),
        ("plain_b", None),
    )
    entries = runner.partition_selected(
        ["plain_a", "micro_a", "plain_b"], manifest
    )
    assert [(kind, name) for kind, name, _, _ in entries] == [
        ("plain", "plain_a"),
        ("micro", "micro_a"),
        ("plain", "plain_b"),
    ]
    # Microbatch entry carries the parsed meta; plain entries carry None.
    assert entries[0][3] is None
    assert entries[1][3]["date_column"] == "date"
    assert entries[2][3] is None


def test_partition_selected_micro_in_middle_splits_plain_buffer_at_runtime():
    """Regression: when a microbatch model sits between two plain models,
    the runner must flush the plain buffer at the microbatch boundary so
    that downstream plain models see the microbatch's table.
    The partition function itself just preserves order; the flush logic is
    in main(). Here we just verify the order is preserved correctly."""
    manifest = _manifest_with(
        ("upstream_plain", None),
        ("the_micro", {"incremental": {"enabled": True, "date_column": "date"}}),
        ("downstream_plain", None),
    )
    entries = runner.partition_selected(
        ["upstream_plain", "the_micro", "downstream_plain"], manifest
    )
    # Critically the microbatch is between the plain entries, not after both.
    assert [kind for kind, *_ in entries] == ["plain", "micro", "plain"]


# ---------------------------------------------------------------------------
# plan_for_model
# ---------------------------------------------------------------------------


def test_plan_for_model_starts_after_max_date(tmp_path):
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [{"name": "_default", "vars": {}}],
    }
    today = dt.date(2026, 4, 22)
    state = {"completed": {}}

    with mock.patch.object(
        runner, "fetch_max_date", return_value=dt.date(2026, 4, 19)
    ):
        plan = runner.plan_for_model(
            "int_x",
            meta,
            today,
            state,
            tmp_path,
            tmp_path,
            batch_days_override=None,
            bootstrap_lookback_days=7,
            dry_run=False,
        )
    assert len(plan) == 1
    stage, slices = plan[0]
    assert stage["name"] == "_default"
    assert slices == [
        dt.date(2026, 4, 20),
        dt.date(2026, 4, 21),
        dt.date(2026, 4, 22),
    ]


def test_plan_for_model_respects_state(tmp_path):
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [{"name": "stage_a", "vars": {}}],
    }
    today = dt.date(2026, 4, 22)
    state = {
        "completed": {
            "int_x::stage_a": {"last_completed_end_date": "2026-04-21"},
        }
    }
    with mock.patch.object(
        runner, "fetch_max_date", return_value=dt.date(2026, 4, 19)
    ):
        plan = runner.plan_for_model(
            "int_x",
            meta,
            today,
            state,
            tmp_path,
            tmp_path,
            batch_days_override=None,
            bootstrap_lookback_days=7,
            dry_run=False,
        )
    _, slices = plan[0]
    assert slices == [dt.date(2026, 4, 22)]


def test_plan_for_model_caught_up_returns_empty(tmp_path):
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [{"name": "_default", "vars": {}}],
    }
    today = dt.date(2026, 4, 22)
    state = {"completed": {}}
    with mock.patch.object(
        runner, "fetch_max_date", return_value=dt.date(2026, 4, 22)
    ):
        plan = runner.plan_for_model(
            "int_x",
            meta,
            today,
            state,
            tmp_path,
            tmp_path,
            batch_days_override=None,
            bootstrap_lookback_days=7,
            dry_run=False,
        )
    _, slices = plan[0]
    assert slices == []


def test_plan_for_model_batch_days_override(tmp_path):
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [{"name": "_default", "vars": {}}],
    }
    today = dt.date(2026, 4, 28)
    state = {"completed": {}}
    with mock.patch.object(
        runner, "fetch_max_date", return_value=dt.date(2026, 4, 20)
    ):
        plan = runner.plan_for_model(
            "int_x",
            meta,
            today,
            state,
            tmp_path,
            tmp_path,
            batch_days_override=3,
            bootstrap_lookback_days=7,
            dry_run=False,
        )
    _, slices = plan[0]
    # max+1 = 04-21; step 3 → 04-21, 04-24, 04-27, cap 04-28.
    assert slices == [
        dt.date(2026, 4, 21),
        dt.date(2026, 4, 24),
        dt.date(2026, 4, 27),
        dt.date(2026, 4, 28),
    ]


def test_stage_filter_sql_range_pair():
    out = runner.stage_filter_sql(
        {"validator_index_start": 0, "validator_index_end": 100000}
    )
    assert out == "validator_index >= 0 AND validator_index < 100000"


def test_stage_filter_sql_string_literal_with_quote_escape():
    out = runner.stage_filter_sql({"chain": "gno's"})
    assert out == "chain = 'gno''s'"


def test_stage_filter_sql_mixed_pair_and_extra():
    out = runner.stage_filter_sql(
        {
            "validator_index_start": 0,
            "validator_index_end": 100000,
            "category": "active",
        }
    )
    parts = set(p.strip() for p in out.split(" AND "))
    assert "validator_index >= 0" in parts
    assert "validator_index < 100000" in parts
    assert "category = 'active'" in parts


def test_stage_filter_sql_empty_inputs():
    assert runner.stage_filter_sql({}) == ""
    assert runner.stage_filter_sql(None) == ""


def test_plan_for_model_calls_fetch_max_date_per_stage_with_where(tmp_path):
    """Each stage's max_date lookup is scoped by its own range filter."""
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [
            {"name": "a", "vars": {"validator_index_start": 0, "validator_index_end": 100000}},
            {"name": "b", "vars": {"validator_index_start": 100000, "validator_index_end": 200000}},
        ],
    }
    today = dt.date(2026, 4, 22)
    state = {"completed": {}}

    seen_filters: list[str | None] = []

    def fake_fetch(model, col, pdir, prof, where_filter=None):
        seen_filters.append(where_filter)
        # Stage a (range >= 0) is "fresh"; stage b (range >= 100000) is far behind.
        if where_filter and "validator_index >= 0 " in where_filter:
            return dt.date(2026, 4, 21)
        return dt.date(2024, 1, 1)

    with mock.patch.object(runner, "fetch_max_date", side_effect=fake_fetch):
        plan = runner.plan_for_model(
            "int_x",
            meta,
            today,
            state,
            tmp_path,
            tmp_path,
            batch_days_override=None,
            bootstrap_lookback_days=7,
            dry_run=False,
            max_slices_per_stage=0,    # disable cap so we can assert raw planning
        )

    # Both stages had their range filter passed through.
    assert seen_filters == [
        "validator_index >= 0 AND validator_index < 100000",
        "validator_index >= 100000 AND validator_index < 200000",
    ]
    by_stage = {stage["name"]: slices for stage, slices in plan}
    # Stage a: max+1 = 04-22, today = 04-22 → 1 slice.
    assert by_stage["a"] == [dt.date(2026, 4, 22)]
    # Stage b: max+1 = 2024-01-02, today = 2026-04-22 → many slices, but
    # the runner uses *that stage's actual max* as the floor, not global. The
    # important property here is that the slice list is correctly anchored to
    # the per-stage max rather than to stage a's max.
    assert by_stage["b"][0] == dt.date(2024, 1, 2)
    assert by_stage["b"][-1] == dt.date(2026, 4, 22)


def test_plan_for_model_empty_stage_bootstraps_from_stage_start_date(tmp_path):
    """When per-stage max returns the 1970 sentinel and stage has start_date."""
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [
            {
                "name": "fresh_range",
                "vars": {"x_start": 0, "x_end": 100},
                "start_date": "2026-04-15",
            }
        ],
    }
    today = dt.date(2026, 4, 18)
    state = {"completed": {}}
    with mock.patch.object(
        runner, "fetch_max_date", return_value=dt.date(1970, 1, 1)
    ):
        plan = runner.plan_for_model(
            "int_x",
            meta,
            today,
            state,
            tmp_path,
            tmp_path,
            batch_days_override=None,
            bootstrap_lookback_days=7,
            dry_run=False,
        )
    _, slices = plan[0]
    # Stage start_date 04-15 is more recent than today - 7 (= 04-11), so
    # bootstrap snaps forward to it.
    assert slices == [
        dt.date(2026, 4, 15),
        dt.date(2026, 4, 16),
        dt.date(2026, 4, 17),
        dt.date(2026, 4, 18),
    ]


def test_plan_for_model_empty_stage_ignores_far_past_start_date(tmp_path):
    """Multi-year-old start_date is NOT used for bootstrap (full-refresh job)."""
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [
            {
                "name": "old_stage",
                "vars": {},
                "start_date": "2022-01-01",   # far in the past
            }
        ],
    }
    today = dt.date(2026, 4, 18)
    state = {"completed": {}}
    with mock.patch.object(
        runner, "fetch_max_date", return_value=dt.date(1970, 1, 1)
    ):
        plan = runner.plan_for_model(
            "int_x",
            meta,
            today,
            state,
            tmp_path,
            tmp_path,
            batch_days_override=None,
            bootstrap_lookback_days=3,
            dry_run=False,
        )
    _, slices = plan[0]
    # max_t = today - 3 = 04-15 → slices [04-16, 04-17, 04-18].
    # 2022-01-01 is ignored — that's full-refresh territory.
    assert slices == [
        dt.date(2026, 4, 16),
        dt.date(2026, 4, 17),
        dt.date(2026, 4, 18),
    ]


def test_plan_for_model_empty_stage_no_start_date_falls_back_to_lookback(tmp_path):
    """Without stage start_date, sentinel triggers bootstrap_lookback_days."""
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [{"name": "_default", "vars": {}}],
    }
    today = dt.date(2026, 4, 18)
    state = {"completed": {}}
    with mock.patch.object(
        runner, "fetch_max_date", return_value=dt.date(1970, 1, 1)
    ):
        plan = runner.plan_for_model(
            "int_x",
            meta,
            today,
            state,
            tmp_path,
            tmp_path,
            batch_days_override=None,
            bootstrap_lookback_days=3,
            dry_run=False,
        )
    _, slices = plan[0]
    # max_t = today - 3 = 04-15 → first slice = 04-16.
    assert slices == [
        dt.date(2026, 4, 16),
        dt.date(2026, 4, 17),
        dt.date(2026, 4, 18),
    ]


def test_plan_for_model_refuses_multi_month_backfill(tmp_path, capsys):
    """Cap protects against accidental multi-month backfill on the cron path."""
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [{"name": "_default", "vars": {}}],
    }
    today = dt.date(2026, 4, 27)
    state = {"completed": {}}
    # Target max way in the past — 100+ days behind.
    with mock.patch.object(
        runner, "fetch_max_date", return_value=dt.date(2024, 8, 22)
    ):
        plan = runner.plan_for_model(
            "int_x",
            meta,
            today,
            state,
            tmp_path,
            tmp_path,
            batch_days_override=None,
            bootstrap_lookback_days=7,
            dry_run=False,
            max_slices_per_stage=30,
        )
    _, slices = plan[0]
    assert slices == []   # refused, empty
    err = capsys.readouterr().err
    assert "exceeds --max-slices-per-stage=30" in err
    assert "scripts/full_refresh/refresh.py" in err


def test_plan_for_model_cap_disabled_when_zero(tmp_path):
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [{"name": "_default", "vars": {}}],
    }
    today = dt.date(2026, 4, 27)
    state = {"completed": {}}
    with mock.patch.object(
        runner, "fetch_max_date", return_value=dt.date(2026, 4, 1)
    ):
        plan = runner.plan_for_model(
            "int_x",
            meta,
            today,
            state,
            tmp_path,
            tmp_path,
            batch_days_override=None,
            bootstrap_lookback_days=7,
            dry_run=False,
            max_slices_per_stage=0,
        )
    _, slices = plan[0]
    # 26 days from 04-02 to 04-27 inclusive — still allowed because cap=0.
    assert len(slices) == 26


def test_get_microbatch_meta_propagates_range_template():
    node = {
        "meta": {
            "full_refresh": {
                "range_template": {
                    "key_column": "validator_index",
                    "step": 100000,
                    "discovery_source": "int_x_upstream",
                    "auto_start_policy": "first_seen",
                },
                "incremental": {"enabled": True, "date_column": "date"},
            }
        }
    }
    meta = runner.get_microbatch_meta(node)
    assert meta["range_template"]["step"] == 100000
    assert meta["range_template"]["auto_start_policy"] == "first_seen"


def test_maybe_extend_stages_no_template_is_noop(tmp_path):
    meta = {
        "date_column": "date", "batch_days": 1, "stages": [], "range_template": None,
    }
    runner.maybe_extend_stages("int_x", meta, dt.date(2026, 4, 27), tmp_path, tmp_path)
    assert meta["stages"] == []


def test_maybe_extend_stages_appends_buckets_today_policy(tmp_path):
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [
            {
                "name": "validators_500k_600k",
                "vars": {"validator_index_start": 500000, "validator_index_end": 600000},
                "start_date": "2025-02-01",
            },
        ],
        "range_template": {
            "key_column": "validator_index",
            "step": 100000,
            "discovery_source": "int_upstream",
            "auto_start_policy": "today",
            "name_template": "validators_{start_k}k_{end_k}k_auto",
        },
    }
    today = dt.date(2026, 4, 27)
    with mock.patch.object(runner, "fetch_max_int", return_value=720_000):
        runner.maybe_extend_stages("int_x", meta, today, tmp_path, tmp_path)

    # Two new buckets: 600k_700k and 700k_800k.
    new_names = [s["name"] for s in meta["stages"][1:]]
    assert new_names == ["validators_600k_700k_auto", "validators_700k_800k_auto"]
    assert meta["stages"][1]["vars"] == {"validator_index_start": 600000, "validator_index_end": 700000}
    assert meta["stages"][2]["vars"] == {"validator_index_start": 700000, "validator_index_end": 800000}
    # Today policy
    assert meta["stages"][1]["start_date"] == today.isoformat()
    assert meta["stages"][2]["start_date"] == today.isoformat()


def test_maybe_extend_stages_first_seen_policy(tmp_path):
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [
            {"name": "v500k_600k",
             "vars": {"validator_index_start": 500000, "validator_index_end": 600000},
             "start_date": "2025-02-01"},
        ],
        "range_template": {
            "key_column": "validator_index",
            "step": 100000,
            "discovery_source": "int_upstream",
            "auto_start_policy": "first_seen",
        },
    }
    today = dt.date(2026, 4, 27)
    seen_filters: list[str] = []

    def fake_first_seen(model, col, where, *_):
        seen_filters.append(where)
        return dt.date(2026, 1, 15)

    with mock.patch.object(runner, "fetch_max_int", return_value=605_000), \
         mock.patch.object(runner, "fetch_first_seen_date", side_effect=fake_first_seen):
        runner.maybe_extend_stages("int_x", meta, today, tmp_path, tmp_path)

    assert len(meta["stages"]) == 2  # one synth bucket [600k, 700k)
    new = meta["stages"][1]
    assert new["start_date"] == "2026-01-15"
    assert seen_filters == [
        "validator_index >= 600000 AND validator_index < 700000",
    ]


def test_maybe_extend_stages_skips_when_upstream_below_declared(tmp_path):
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [
            {"name": "v500k_600k",
             "vars": {"validator_index_start": 500000, "validator_index_end": 600000},
             "start_date": "2025-02-01"},
        ],
        "range_template": {
            "key_column": "validator_index",
            "step": 100000,
            "discovery_source": "int_upstream",
            "auto_start_policy": "today",
        },
    }
    with mock.patch.object(runner, "fetch_max_int", return_value=512_345):
        runner.maybe_extend_stages("int_x", meta, dt.date(2026, 4, 27),
                                   tmp_path, tmp_path)
    assert len(meta["stages"]) == 1  # no extension; declared range covers it


def test_maybe_extend_stages_handles_upstream_null(tmp_path):
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [],
        "range_template": {
            "key_column": "validator_index",
            "step": 100000,
            "discovery_source": "int_upstream",
            "auto_start_policy": "today",
        },
    }
    with mock.patch.object(runner, "fetch_max_int", return_value=None):
        runner.maybe_extend_stages("int_x", meta, dt.date(2026, 4, 27),
                                   tmp_path, tmp_path)
    assert meta["stages"] == []


def test_maybe_extend_stages_enum_appends_one_per_new_value(tmp_path):
    """Enum template: one stage per upstream distinct value not declared."""
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [
            {"name": "chain_gnosis_manual", "vars": {"chain_id": "gnosis"}, "start_date": "2024-01-01"},
        ],
        "range_template": {
            "key_column": "chain_id",
            "enum_source": "int_chains",
            "auto_start_policy": "today",
            "name_template": "{key_column}_{value}_auto",
        },
    }
    today = dt.date(2026, 4, 27)
    with mock.patch.object(
        runner, "fetch_distinct_values",
        return_value=["chiado", "ethereum", "gnosis"],   # gnosis already declared
    ):
        runner.maybe_extend_stages("int_x", meta, today, tmp_path, tmp_path)

    new = meta["stages"][1:]
    assert len(new) == 2
    names = sorted(s["name"] for s in new)
    assert names == ["chain_id_chiado_auto", "chain_id_ethereum_auto"]
    # Vars keyed by the column name with raw value.
    chain_vars = {s["vars"]["chain_id"] for s in new}
    assert chain_vars == {"chiado", "ethereum"}
    # All synthesized stages take today.
    assert all(s["start_date"] == today.isoformat() for s in new)


def test_maybe_extend_stages_enum_first_seen_filter_string_literal(tmp_path):
    """first_seen on enum dimension generates `<col> = '<value>'` filter."""
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [],
        "range_template": {
            "key_column": "chain_id",
            "enum_source": "int_chains",
            "auto_start_policy": "first_seen",
        },
    }
    today = dt.date(2026, 4, 27)
    captured: list[str] = []

    def fake_first_seen(model, col, where, *_):
        captured.append(where)
        return dt.date(2025, 6, 1) if "ethereum" in where else dt.date(2024, 1, 1)

    with mock.patch.object(runner, "fetch_distinct_values", return_value=["ethereum", "gnosis"]), \
         mock.patch.object(runner, "fetch_first_seen_date", side_effect=fake_first_seen):
        runner.maybe_extend_stages("int_x", meta, today, tmp_path, tmp_path)

    assert captured == [
        "chain_id = 'ethereum'",
        "chain_id = 'gnosis'",
    ]
    by_value = {s["vars"]["chain_id"]: s["start_date"] for s in meta["stages"]}
    assert by_value == {"ethereum": "2025-06-01", "gnosis": "2024-01-01"}


def test_maybe_extend_stages_enum_overflow_skipped(tmp_path):
    meta = {
        "date_column": "date", "batch_days": 1, "stages": [],
        "range_template": {
            "key_column": "user_id",
            "enum_source": "int_users",
            "auto_start_policy": "today",
            "max_values": 10,
        },
    }
    with mock.patch.object(runner, "fetch_distinct_values", return_value=None):
        runner.maybe_extend_stages("int_x", meta, dt.date(2026, 4, 27),
                                   tmp_path, tmp_path)
    assert meta["stages"] == []


def test_maybe_extend_stages_composite_cartesian(tmp_path):
    """Composite template: cartesian product across dims, vars merged, latest start_date wins."""
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [],
        "range_template": [
            {
                "key_column": "chain_id",
                "enum_source": "int_chains",
                "auto_start_policy": "today",
                "name_template": "chain_{value}",
            },
            {
                "key_column": "token_id",
                "step": 1000,
                "discovery_source": "int_tokens",
                "auto_start_policy": "today",
                "name_template": "tok_{start}_{end}",
            },
        ],
    }
    today = dt.date(2026, 4, 27)
    with mock.patch.object(runner, "fetch_distinct_values", return_value=["gnosis", "ethereum"]), \
         mock.patch.object(runner, "fetch_max_int", return_value=2_500):
        runner.maybe_extend_stages("int_x", meta, today, tmp_path, tmp_path)

    # Cartesian product: 2 chains × 3 token-buckets [0,1k), [1k,2k), [2k,3k) = 6 stages.
    assert len(meta["stages"]) == 6
    # Confirm the merged vars on a sample stage.
    by_name = {s["name"]: s for s in meta["stages"]}
    sample_name = "chain_gnosis_x_tok_0_1000"
    assert sample_name in by_name
    s = by_name[sample_name]
    assert s["vars"] == {"chain_id": "gnosis", "token_id_start": 0, "token_id_end": 1000}
    assert s["start_date"] == today.isoformat()


def test_maybe_extend_stages_composite_skips_if_one_dim_empty(tmp_path):
    """If any dim yields zero new additions, composite emits nothing."""
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [
            # Already-declared chain_id values cover everything.
            {"name": "manual", "vars": {"chain_id": "gnosis"}, "start_date": "2024-01-01"},
        ],
        "range_template": [
            {
                "key_column": "chain_id",
                "enum_source": "int_chains",
                "auto_start_policy": "today",
            },
            {
                "key_column": "token_id",
                "step": 1000,
                "discovery_source": "int_tokens",
                "auto_start_policy": "today",
            },
        ],
    }
    with mock.patch.object(runner, "fetch_distinct_values", return_value=["gnosis"]), \
         mock.patch.object(runner, "fetch_max_int", return_value=2_500):
        runner.maybe_extend_stages("int_x", meta, dt.date(2026, 4, 27),
                                   tmp_path, tmp_path)
    # Only the originally-declared stage remains.
    assert len(meta["stages"]) == 1


def test_maybe_extend_stages_composite_picks_latest_start_date(tmp_path):
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [],
        "range_template": [
            {
                "key_column": "chain_id",
                "enum_source": "int_chains",
                "auto_start_policy": "first_seen",
                "name_template": "c_{value}",
            },
            {
                "key_column": "token_id",
                "step": 1000,
                "discovery_source": "int_tokens",
                "auto_start_policy": "first_seen",
                "name_template": "t_{start}_{end}",
            },
        ],
    }
    today = dt.date(2026, 4, 27)

    def fake_first_seen(model, col, where, *_):
        # Chain dim: different start dates per chain.
        if "chain_id" in where and "ethereum" in where:
            return dt.date(2025, 6, 1)
        if "chain_id" in where and "gnosis" in where:
            return dt.date(2023, 1, 1)
        # Token dim: one date for all buckets here.
        if "token_id" in where:
            return dt.date(2024, 8, 1)
        return today

    with mock.patch.object(runner, "fetch_distinct_values", return_value=["ethereum", "gnosis"]), \
         mock.patch.object(runner, "fetch_max_int", return_value=999), \
         mock.patch.object(runner, "fetch_first_seen_date", side_effect=fake_first_seen):
        runner.maybe_extend_stages("int_x", meta, today, tmp_path, tmp_path)

    by_name = {s["name"]: s for s in meta["stages"]}
    # ethereum × token bucket → max(2025-06-01, 2024-08-01) = 2025-06-01
    assert by_name["c_ethereum_x_t_0_1000"]["start_date"] == "2025-06-01"
    # gnosis × token bucket → max(2023-01-01, 2024-08-01) = 2024-08-01
    assert by_name["c_gnosis_x_t_0_1000"]["start_date"] == "2024-08-01"


def test_plan_for_model_skips_fetch_when_stage_filtered_out(tmp_path):
    """A stage that's filtered out must not trigger a max_date query."""
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [
            {"name": "a", "vars": {"x_start": 0, "x_end": 100}},
            {"name": "b", "vars": {"x_start": 100, "x_end": 200}},
        ],
    }
    today = dt.date(2026, 4, 22)
    state = {"completed": {}}

    fetch_calls: list[str | None] = []

    def fake_fetch(model, col, pdir, prof, where_filter=None):
        fetch_calls.append(where_filter)
        return dt.date(2026, 4, 21)

    with mock.patch.object(runner, "fetch_max_date", side_effect=fake_fetch):
        plan = runner.plan_for_model(
            "int_x",
            meta,
            today,
            state,
            tmp_path,
            tmp_path,
            batch_days_override=None,
            bootstrap_lookback_days=7,
            dry_run=False,
            stage_filter=["a"],
        )

    assert fetch_calls == ["x >= 0 AND x < 100"]
    by_stage = {stage["name"]: slices for stage, slices in plan}
    assert by_stage["a"] == [dt.date(2026, 4, 22)]
    assert by_stage["b"] == []  # filtered out, never queried


def test_plan_for_model_per_stage_state_independent(tmp_path):
    meta = {
        "date_column": "date",
        "batch_days": 1,
        "stages": [
            {"name": "a", "vars": {"k": 1}},
            {"name": "b", "vars": {"k": 2}},
        ],
    }
    today = dt.date(2026, 4, 22)
    state = {
        "completed": {
            "int_x::a": {"last_completed_end_date": "2026-04-22"},
        }
    }
    with mock.patch.object(
        runner, "fetch_max_date", return_value=dt.date(2026, 4, 19)
    ):
        plan = runner.plan_for_model(
            "int_x",
            meta,
            today,
            state,
            tmp_path,
            tmp_path,
            batch_days_override=None,
            bootstrap_lookback_days=7,
            dry_run=False,
        )
    by_stage = {stage["name"]: slices for stage, slices in plan}
    assert by_stage["a"] == []   # caught up via state
    assert by_stage["b"] == [
        dt.date(2026, 4, 20),
        dt.date(2026, 4, 21),
        dt.date(2026, 4, 22),
    ]
