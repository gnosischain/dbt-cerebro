#!/usr/bin/env python3
"""Claude Code PreToolUse adapter over scripts/agent_context/guard.py.

Thin by design: all detection logic lives in the vendor-neutral guard so other
agent products and CI can reuse it. On a 'warn' verdict this asks for user
confirmation (permissionDecision: ask) with the guard's message; it never
hard-denies — the refresh runners and CI gates are the authoritative
enforcement (see docs/lessons/refresh-state-collision.md).
"""

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
GUARD = REPO_ROOT / "scripts" / "agent_context" / "guard.py"


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0
    command = (payload.get("tool_input") or {}).get("command") or ""
    if not command:
        return 0
    try:
        out = subprocess.check_output(
            [sys.executable, str(GUARD), command], text=True, timeout=10
        )
        result = json.loads(out)
    except Exception:
        return 0  # guard trouble must never block normal work

    if result.get("verdict") != "warn":
        return 0

    reasons = "; ".join(
        f"{f['message']} [lesson: {f['lesson']}]" for f in result.get("findings", [])
    )
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "ask",
            "permissionDecisionReason": reasons,
        }
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
