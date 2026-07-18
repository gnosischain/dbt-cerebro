"""Raw-code incremental-strategy detection.

Shared by the agent-context builder (scripts/agent_context/build_agent_context.py)
and the incremental-policy gate (scripts/checks/no_delete_insert.py) so both see
the SAME classification — single source of truth.

Why raw code: the manifest's config.incremental_strategy is parse-time RESOLVED.
A config expression like

    incremental_strategy=('append' if start_month else 'insert_overwrite')

has already collapsed to its default branch ('insert_overwrite') by the time it
reaches the manifest — resolved config cannot distinguish the wipe-safe staged
pattern from a dangerous literal. unrendered_config has the same problem for
in-file config() calls (evaluated during jinja render). Only the raw model code
still carries the expression.
"""

from __future__ import annotations

import re

# Scope markers: vars that are set for scoped/staged/batched invocations and
# unset for the plain daily run. A conditional keyed on one of these is the
# staged-write pattern; anything else is undeterminable from static text.
_SCOPE_MARKERS = ("start_month", "reprocess", "var(")

_ASSIGN_RE = re.compile(r"incremental_strategy\s*=")
_QUOTED_RE = re.compile(r"^(?:'([^']*)'|\"([^\"]*)\")$")
_COND_RE = re.compile(
    r"(?:'(?P<t1>[^']+)'|\"(?P<t2>[^\"]+)\")\s+if\s+(?P<cond>.+?)\s+else\s+"
    r"(?:'(?P<f1>[^']+)'|\"(?P<f2>[^\"]+)\")",
    re.DOTALL,
)


def _extract_assignment(raw_code: str) -> str | None:
    """Return the RHS text of the incremental_strategy assignment, or None."""
    m = _ASSIGN_RE.search(raw_code)
    if m is None:
        return None
    rest = raw_code[m.end():].lstrip()
    if rest.startswith("("):
        depth = 0
        for i, ch in enumerate(rest):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    return rest[: i + 1]
        return rest.strip()
    out: list[str] = []
    depth = 0
    for ch in rest:
        if ch in "([":
            depth += 1
        elif ch in ")]":
            if depth == 0:
                break
            depth -= 1
        elif ch == "," and depth == 0:
            break
        elif ch == "\n":
            break
        out.append(ch)
    return "".join(out).strip() or None


def analyze_strategy(raw_code: str) -> dict:
    """Classify the incremental_strategy assignment in raw model code.

    Returns:
      assigned        bool  — the model sets incremental_strategy in-file
                              (False = it inherits the project default)
      expression      bool  — the value is computed, not a quoted literal
      literal         str|None — the literal strategy when not an expression
      scoped_branch   str|None — for a scope-var conditional: the strategy the
                                 SCOPED invocation (vars set) resolves to
      scoped_append   bool|None — scoped_branch == 'append'; None when the
                                  expression shape is undeterminable
    """
    result = {
        "assigned": False,
        "expression": False,
        "literal": None,
        "scoped_branch": None,
        "scoped_append": None,
    }
    expr = _extract_assignment(raw_code or "")
    if expr is None:
        return result
    result["assigned"] = True

    inner = expr.strip()
    if inner.startswith("(") and inner.endswith(")"):
        inner = inner[1:-1].strip()

    lit = _QUOTED_RE.match(inner)
    if lit:
        result["literal"] = lit.group(1) if lit.group(1) is not None else lit.group(2)
        return result

    result["expression"] = True
    cond_match = _COND_RE.search(inner)
    if not cond_match:
        return result

    cond = cond_match.group("cond").strip()
    if not any(marker in cond for marker in _SCOPE_MARKERS):
        return result

    true_branch = cond_match.group("t1") or cond_match.group("t2")
    false_branch = cond_match.group("f1") or cond_match.group("f2")
    scoped = false_branch if cond.startswith("not ") else true_branch
    result["scoped_branch"] = scoped
    result["scoped_append"] = scoped == "append"
    return result
