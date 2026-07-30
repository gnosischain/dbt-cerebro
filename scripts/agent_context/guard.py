#!/usr/bin/env python3
"""Vendor-neutral command guard: given a shell command about to run against
this repo, report known-dangerous patterns before they execute.

This is the AUTHORITATIVE guard logic — agent-product hooks (e.g.
.claude/hooks/bash_guard.py) are thin adapters over it, and safety never
depends on it alone: state collisions are refused by the refresh runners
themselves (scripts/refresh/run_state.py) and policy violations by the CI
gates. The guard exists to surface a warning at the point of action, citing
the lesson record.

Usage:
    python scripts/agent_context/guard.py "<command string>"

Output: JSON on stdout:
    {"verdict": "ok" | "warn", "findings": [{"pattern": ..., "message": ..., "lesson": ...}]}

Exit code is always 0 for "ok"/"warn" (advisory); 2 on usage error.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def pending_refresh_states() -> list[str]:
    out = []
    state_dir = REPO_ROOT / "target" / "refresh_state"
    if state_dir.exists():
        for p in sorted(state_dir.glob("*.json")):
            try:
                st = json.loads(p.read_text())
                sel = (st.get("identity") or {}).get("select")
                sel = " ".join(sel) if isinstance(sel, list) else (sel or "?")
                out.append(f"{st.get('tool', '?')} run {st.get('run_id', '?')} (select: {sel})")
            except Exception:
                out.append(p.name)
    legacy = REPO_ROOT / "scripts" / "full_refresh" / ".refresh_state.json"
    if legacy.exists():
        out.append(f"LEGACY shared state file: {legacy}")
    return out


def analyze(command: str) -> dict:
    findings = []

    # Broad --full-refresh: tag selectors, graph operators, or multi-model lists
    # re-read entire raw histories and can OOM / wipe via strategy branches.
    if re.search(r"\bdbt\s+(run|build)\b", command) and "--full-refresh" in command:
        sel = re.search(r"(?:--select|-s)\s+((?:[^-]\S*\s*)+)", command)
        sel_text = sel.group(1).strip() if sel else ""
        broad = (
            "tag:" in sel_text
            or "+" in sel_text
            or "*" in sel_text
            or len(sel_text.split()) > 1
            or not sel_text
        )
        if broad:
            findings.append({
                "pattern": "broad-full-refresh",
                "message": (
                    f"dbt --full-refresh with a broad selector ('{sel_text or '(none)'}') "
                    "re-reads full raw history for every matched model — this OOMs on the "
                    "big chains and staged models must be rebuilt via "
                    "scripts/full_refresh/refresh.py (batch 1 recreates safely). "
                    "Prefer a single-model selector, or the orchestrator."
                ),
                "lesson": "ch-partition-cap / staged-insert-overwrite-wipe",
            })

    # Any refresh runner start while other runs are pending: the runners
    # enforce/warn themselves, but surfacing it before execution saves a
    # wasted invocation.
    if re.search(r"(refresh\.py|dbt_incremental_runner\.py|gap_window_refresh\.py)", command):
        pending = pending_refresh_states()
        if pending and "--resume" not in command:
            findings.append({
                "pattern": "pending-refresh-state",
                "message": (
                    "Pending refresh run(s) exist: " + "; ".join(pending) + ". "
                    "refresh.py will refuse overlapping selections — finish or clear "
                    "them first (--resume with the original args / --clear-state <id>)."
                ),
                "lesson": "refresh-state-collision",
            })

    # Manual ALTER ... DELETE / lightweight deletes — the wipe class.
    if re.search(r"ALTER\s+TABLE.+DELETE\s", command, re.IGNORECASE | re.DOTALL):
        findings.append({
            "pattern": "manual-alter-delete",
            "message": (
                "Manual ALTER TABLE ... DELETE is a lightweight mutation that keeps "
                "running after the client disconnects and can wipe a window mid-repair. "
                "Use insert_overwrite (atomic REPLACE PARTITION) or a per-slice reprocess."
            ),
            "lesson": "wide-delete-insert-wipe",
        })

    return {"verdict": "warn" if findings else "ok", "findings": findings}


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: guard.py '<command string>'", file=sys.stderr)
        return 2
    print(json.dumps(analyze(" ".join(sys.argv[1:]))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
