"""Run-state identity shared by the refresh runners.

Both scripts/full_refresh/refresh.py and scripts/refresh/dbt_incremental_runner.py
persist resume state. Historically each used ONE fixed path, so a new invocation
with a different --select silently clobbered a pending --resume
(docs/lessons/refresh-state-collision.md). This module keys state by *run
identity* — a hash of the fields that define the run's plan — under
<project>/target/refresh_state/, so distinct runs never share a file and a
pending run's selection can be compared against a new one before starting.

State files are self-describing: alongside the runner's own progress keys they
carry `run_id`, `tool`, the identity fields, the resolved `models` list, and
created/updated timestamps.
"""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

STATE_DIR_NAME = "refresh_state"


def state_dir(project_root: Path) -> Path:
    return project_root / "target" / STATE_DIR_NAME


def run_identity(tool: str, fields: Dict[str, Any]) -> str:
    """Stable 12-hex-char id for a run plan.

    `fields` must contain every argument that changes what the run would do
    (selection, exclusions, stage filters, mode flags) and nothing volatile
    (timestamps, pids).
    """
    canonical = json.dumps({"tool": tool, **fields}, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()[:12]


def state_path(project_root: Path, tool: str, run_id: str) -> Path:
    return state_dir(project_root) / f"{tool}_{run_id}.json"


def new_state(tool: str, run_id: str, fields: Dict[str, Any], models: List[str]) -> dict:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return {
        "run_id": run_id,
        "tool": tool,
        "identity": fields,
        "models": models,
        "created_at": now,
        "updated_at": now,
    }


def load(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        print(f"[warn] state file {path} unreadable", file=sys.stderr)
        return None


def save(path: Path, state: dict) -> None:
    """Atomic write (tmp + replace) so a crash never leaves a torn state file."""
    state["updated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True, default=str))
    tmp.replace(path)


def clear(path: Path) -> None:
    if path.exists():
        path.unlink()


def pending_states(project_root: Path, tool: Optional[str] = None) -> List[Tuple[Path, dict]]:
    """All parseable state files (optionally one tool's), newest first."""
    d = state_dir(project_root)
    if not d.exists():
        return []
    out: List[Tuple[Path, dict]] = []
    for p in sorted(d.glob("*.json"), key=lambda q: q.stat().st_mtime, reverse=True):
        if tool and not p.name.startswith(f"{tool}_"):
            continue
        st = load(p)
        if st is not None:
            out.append((p, st))
    return out


def overlapping(
    pending: List[Tuple[Path, dict]], models: List[str], exclude_run_id: Optional[str] = None
) -> List[Tuple[Path, dict, List[str]]]:
    """Pending states whose recorded model list intersects `models`."""
    mine = set(models)
    hits: List[Tuple[Path, dict, List[str]]] = []
    for path, st in pending:
        if exclude_run_id and st.get("run_id") == exclude_run_id:
            continue
        shared = sorted(mine & set(st.get("models") or []))
        if shared:
            hits.append((path, st, shared))
    return hits


def describe(st: dict) -> str:
    ident = st.get("identity") or {}
    sel = ident.get("select") or "?"
    if isinstance(sel, list):
        sel = " ".join(sel)
    return (
        f"run {st.get('run_id', '?')} (tool={st.get('tool', '?')}, select='{sel}', "
        f"updated {st.get('updated_at', '?')})"
    )
