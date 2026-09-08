"""Unit tests for scripts/checks/run_all.py helpers added 2026-09-08.

The image gate must never depend on the warehouse: a missing catalog is stubbed,
and the warehouse tier's docs-generate step retries only when ClickHouse killed it
as a server-wide (total) overcommit victim.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts" / "checks"))

import run_all  # noqa: E402


def test_write_stub_catalog_is_an_empty_valid_catalog(tmp_path):
    cat = tmp_path / "target" / "catalog.json"
    run_all.write_stub_catalog(cat)
    data = json.loads(cat.read_text())
    assert data["nodes"] == {} and data["sources"] == {}
    assert "dbt_schema_version" in data["metadata"]
    assert "stub" in data["metadata"]


def test_overcommit_detector_matches_only_the_total_signature(tmp_path, monkeypatch):
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log = log_dir / "dbt.log"
    monkeypatch.setenv("DBT_LOG_PATH", str(log_dir))
    log.write_text("Code: 241. DB::Exception: (total) memory limit exceeded: would use 10.82 GiB\n")
    assert run_all._dbt_log_shows_overcommit_victim() is True
    # a per-query limit is deterministic for our own query: never retried
    log.write_text("Code: 241. DB::Exception: Memory limit (for query) exceeded: would use 9 GiB\n")
    assert run_all._dbt_log_shows_overcommit_victim() is False
    log.write_text("Code: 47. DB::Exception: Unknown identifier\n")
    assert run_all._dbt_log_shows_overcommit_victim() is False
    monkeypatch.setenv("DBT_LOG_PATH", str(tmp_path / "missing"))
    assert run_all._dbt_log_shows_overcommit_victim() is False
