"""Pin the retry classification of the errors the cron has actually hit.

classify_failed_nodes.classify_message decides which failed nodes the cron's retry
ladder re-runs (main-run stash and, since 2026-10-01, the descendants build too).
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts" / "refresh"))

from classify_failed_nodes import classify_message  # noqa: E402


def test_341_replicas_inactive_is_transient():
    # 2026-10-01: the delete+insert DELETE was left to finish asynchronously while a
    # replica was down; a re-run recomputes the window and refills it.
    msg = (
        "Code: 341. DB::Exception: Not finished because some replicas are inactive "
        "right now: c-coalgcp-cz-61-server-nm6ctqt-0. Will finish asynchronously."
    )
    assert classify_message(msg) == "transient"


def test_241_total_victim_is_transient():
    msg = (
        "Code: 241. DB::Exception: (total) memory limit exceeded: would use 14.43 GiB "
        "(attempt to allocate chunk of 0.00 B), current RSS: 14.43 GiB, maximum: 14.40 GiB."
    )
    assert classify_message(msg) == "transient"


def test_241_per_query_oom_is_permanent():
    msg = (
        "Code: 241. DB::Exception: Query memory limit exceeded: would use 1.06 GiB "
        "(attempt to allocate chunk of 129.12 MiB), maximum: 1.00 GiB. (MEMORY_LIMIT_EXCEEDED)"
    )
    assert classify_message(msg) == "permanent"


def test_341_mutation_per_query_oom_is_permanent():
    # README "Code: 341 -- Mutation ... memory limit exceeded": the model needs the
    # microbatch path, a retry re-OOMs.
    msg = (
        "Code: 341. DB::Exception: Exception happened during execution of mutation: "
        "Memory limit (for query) exceeded: would use 9.31 GiB. (MEMORY_LIMIT_EXCEEDED)"
    )
    assert classify_message(msg) == "permanent"


def test_sql_error_is_permanent():
    assert classify_message("Code: 47. DB::Exception: Unknown identifier: foo.") == "permanent"
