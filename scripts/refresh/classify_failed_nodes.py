#!/usr/bin/env python3
"""Classify failed dbt nodes from stashed per-batch run_results.json files.

Reads every *.json under --stash-dir, inspects each node with status == "error",
and partitions the unique_ids into TRANSIENT (retry-worthy ClickHouse errors)
vs PERMANENT (logic/SQL bugs). Emits two lines on stdout consumable by bash:

  TRANSIENT=<space-separated unique_ids>
  PERMANENT=<space-separated unique_ids>

Always exits 0 so the caller can read both lists; an empty stash-dir yields two
empty assignments.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


# RETRY BY DEFAULT. This used to be an allowlist of known-transient patterns,
# with everything else filed PERMANENT (no retry). That default is wrong, and it
# failed twice in two days: 2026-09-23 the prose spelling "Unexpected EOF while
# reading bytes" did not match the constant UNEXPECTED_EOF_WHILE_READING, costing
# int_hopr_channels_events and fct_execution_circles_v2_avatar_tokens_held_count
# their retries; 2026-09-24 a Code 241 that did not carry the literal string
# "OvercommitTracker" cost int_execution_circles_v2_wrapper_transfers its retry
# and left it a day stale while the job reported success.
#
# An allowlist cannot win: every new error spelling falls through to "don't
# retry", silently. The cost is also lopsided -- a needless retry of a real SQL
# bug is one fast re-run (wrapper_transfers failed in 0.11 s) and still reports
# failed, whereas a missed retry is 24 h of stale data reported as success.
#
# So: a node is PERMANENT only when the error is provably deterministic and will
# fail identically on a re-run -- SQL and schema bugs. Everything else retries.
PERMANENT_RE = re.compile(
    # Deterministic ClickHouse query errors: the SQL itself is wrong.
    r"Code:\s*(?:"
    r"10"      # NOT_FOUND_COLUMN_IN_BLOCK
    r"|16"     # NO_SUCH_COLUMN_IN_TABLE
    r"|43"     # ILLEGAL_TYPE_OF_ARGUMENT
    r"|44"     # ILLEGAL_COLUMN
    r"|47"     # UNKNOWN_IDENTIFIER
    r"|53"     # TYPE_MISMATCH
    r"|60"     # UNKNOWN_TABLE
    r"|62"     # SYNTAX_ERROR
    r"|81"     # UNKNOWN_DATABASE
    r"|122"    # INCOMPATIBLE_COLUMNS
    r"|179"    # MULTIPLE_EXPRESSIONS_FOR_ALIAS
    r"|352"    # AMBIGUOUS_COLUMN_NAME
    r"|452"    # SETTING_CONSTRAINT_VIOLATION (CH Cloud refuses the setting)
    r")\b"
    r"|UNKNOWN_IDENTIFIER|SYNTAX_ERROR|UNKNOWN_TABLE|NOT_FOUND_COLUMN_IN_BLOCK"
    r"|ILLEGAL_TYPE_OF_ARGUMENT|TYPE_MISMATCH|INCOMPATIBLE_COLUMNS"
    r"|AMBIGUOUS_COLUMN_NAME"
    # dbt-side failures: jinja, ref() and contract errors are deterministic too.
    r"|Compilation Error"
    r"|Parsing Error",
    re.IGNORECASE,
)

# A per-query OOM IS deterministic: our own query asked for more than the limit,
# so it re-OOMs on retry (the 2026-06-08 run wasted its retry step this way).
# But a Code 241 naming `(total)` is the opposite -- the SERVER was saturated and
# this query was picked as the victim, which clears once the cron goes quiet.
# AGENTS.md states the marker explicitly: "(total)" plus "allocate chunk 0.00 B".
# Match the signature, never the presence of the word "OvercommitTracker", which
# only some builds emit.
OOM_RE = re.compile(r"Code:\s*241\b|MEMORY_LIMIT_EXCEEDED", re.IGNORECASE)
OOM_VICTIM_RE = re.compile(r"\(total\)|OvercommitTracker", re.IGNORECASE)


def classify_run_results(path: Path) -> tuple[set[str], set[str]]:
    transient: set[str] = set()
    permanent: set[str] = set()
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[classify] skipping unreadable {path}: {exc}", file=sys.stderr)
        return transient, permanent

    for result in data.get("results", []):
        if result.get("status") != "error":
            continue
        unique_id = result.get("unique_id")
        if not unique_id:
            continue
        message = result.get("message") or ""
        if classify_message(message) == "permanent":
            permanent.add(unique_id)
        else:
            transient.add(unique_id)
    return transient, permanent


def classify_message(message: str) -> str:
    """Return "permanent" only for errors that will fail identically on a re-run.

    Default is "transient": retrying costs one fast re-run, not retrying costs a
    day of stale data reported as success. See the PERMANENT_RE note above.
    """
    # A per-query OOM is deterministic; the same query re-OOMs. A `(total)` OOM
    # means the server was saturated and we were the victim -- that clears.
    if OOM_RE.search(message):
        return "transient" if OOM_VICTIM_RE.search(message) else "permanent"
    if PERMANENT_RE.search(message):
        return "permanent"
    return "transient"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stash-dir",
        type=Path,
        required=True,
        help="Directory containing per-batch run_results.json copies.",
    )
    args = parser.parse_args()

    all_transient: set[str] = set()
    all_permanent: set[str] = set()

    if args.stash_dir.is_dir():
        for path in sorted(args.stash_dir.glob("*.json")):
            t, p = classify_run_results(path)
            all_transient |= t
            all_permanent |= p

    # A node that failed permanently in one batch and transiently in another
    # (edge case: same node appears twice) is treated as permanent — a real
    # bug shouldn't be masked by a concurrent transient flake.
    all_transient -= all_permanent

    print(f"TRANSIENT={' '.join(sorted(all_transient))}")
    print(f"PERMANENT={' '.join(sorted(all_permanent))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
