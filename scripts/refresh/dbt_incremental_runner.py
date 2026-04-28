#!/usr/bin/env python3
"""Microbatch runner for large incremental dbt models.

Wraps `dbt run --select <selector>` so that for *annotated* models the load
is sliced into bounded per-day windows whose `incremental_strategy` resolves
to `append` (no ClickHouse mutation), while plain models pass through to a
single `dbt run` invocation matching existing behavior.

A model is annotated by adding a nested `incremental` block under
`meta.full_refresh` in its schema.yml:

    meta:
      full_refresh:
        start_date: "2024-01-01"
        batch_months: 1
        stages: [...]            # existing — reused for non-time slicing
        incremental:              # NEW
          enabled: true
          date_column: date
          batch_days: 1

Daily slicing strategy (mutation-minimizing):

  1. Read max(date_column) from target via `dbt run-operation get_max_date`.
  2. Slice list begins at max(date_column) + 1 day, ends at min(today, --max-end-date).
  3. For each (stage, slice-end) pair, invoke
       dbt run --select <model> --vars '{"incremental_end_date": "<end>",
                                          ...stage.vars}'
     The model's three-branch strategy expression resolves to `append`,
     producing rows whose `date` strictly exceeds anything already in the
     table → no duplicates, no mutations.
  4. State is persisted under target/incremental_microbatch_state.json so
     `--resume` can skip already-completed slices.

Failure handling integrates with the existing `run_dbt_observability.sh`
pipeline:

  - target/run_results.json from a failed slice is copied to
    target/failed_batches/microbatch-<model>-<stage>-<end>.json.
  - The downstream classify_failed_nodes.py + transient retry block picks
    them up unchanged.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterator

# Reuse existing manifest helpers from the lineage planner.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from dbt_run_batches import (  # noqa: E402  (intentional sibling import)
    load_manifest,
    selected_models,
    selected_nodes,
    topo_sort,
)


MAX_DATE_RESULT_RE = re.compile(
    r"MAX_DATE_RESULT::([A-Za-z][\w]*)::([A-Za-z][\w]*)::(\d{4}-\d{2}-\d{2})"
)
MAX_INT_RESULT_RE = re.compile(
    r"MAX_INT_RESULT::([A-Za-z][\w]*)::([A-Za-z][\w]*)::(NULL|-?\d+)"
)
FIRST_SEEN_RESULT_RE = re.compile(
    r"FIRST_SEEN_DATE_RESULT::([A-Za-z][\w]*)::([A-Za-z][\w]*)::(NULL|\d{4}-\d{2}-\d{2})"
)
DISTINCT_VALUE_RE = re.compile(
    r"DISTINCT_VALUE::([A-Za-z][\w]*)::([A-Za-z][\w]*)::(.+)$"
)
DISTINCT_VALUES_END_RE = re.compile(
    r"DISTINCT_VALUES_END::([A-Za-z][\w]*)::([A-Za-z][\w]*)::(\d+)"
)
DISTINCT_VALUES_OVERFLOW_RE = re.compile(
    r"DISTINCT_VALUES_OVERFLOW::([A-Za-z][\w]*)::([A-Za-z][\w]*)::(\d+)"
)
ANSI_ESCAPE_RE = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")


# ---------------------------------------------------------------------------
# Manifest / metadata
# ---------------------------------------------------------------------------


def _full_refresh_block(node: dict) -> dict:
    """Return meta.full_refresh, falling back to config.meta.full_refresh."""
    full = (node.get("meta") or {}).get("full_refresh") or {}
    if not full:
        full = ((node.get("config") or {}).get("meta") or {}).get("full_refresh") or {}
    return full


def get_microbatch_meta(node: dict) -> dict | None:
    """Return microbatch config for an annotated model, else None.

    The runner reads `meta.full_refresh.incremental` (a nested block under the
    existing full-refresh metadata) — no new top-level meta key.
    """
    full = _full_refresh_block(node)
    incr = full.get("incremental")
    if not (incr and incr.get("enabled")):
        return None
    stages = full.get("stages") or [{"name": "_default", "vars": {}}]
    # Normalize: each stage must have a name, vars, and (optionally) the
    # historical start_date — used to bootstrap from a sane date when the
    # target has no rows for the stage's range yet.
    norm_stages = []
    for s in stages:
        norm_stages.append(
            {
                "name": s.get("name") or "_default",
                "vars": s.get("vars") or {},
                "start_date": s.get("start_date") or full.get("start_date"),
            }
        )
    return {
        "date_column": incr.get("date_column", "date"),
        "batch_days": int(incr.get("batch_days", 1)),
        "stages": norm_stages,
        "range_template": full.get("range_template"),  # may be None
    }


# ---------------------------------------------------------------------------
# dbt invocation helpers
# ---------------------------------------------------------------------------


def _strip_ansi(text: str) -> str:
    return ANSI_ESCAPE_RE.sub("", text)


def stage_filter_sql(stage_vars: dict | None) -> str:
    """Derive a SQL WHERE-clause body from a stage's vars.

    Convention: any pair of vars named ``<col>_start`` / ``<col>_end`` becomes
    ``<col> >= <start> AND <col> < <end>``. Other vars are emitted as
    ``<col> = <literal>`` (rarely used but supports stages keyed on a single
    enum value).

    Returns "" when no vars / no recognizable structure.

    Examples
    --------
    >>> stage_filter_sql({"validator_index_start": 0, "validator_index_end": 100000})
    'validator_index >= 0 AND validator_index < 100000'
    >>> stage_filter_sql({"chain": "gnosis"})
    "chain = 'gnosis'"
    >>> stage_filter_sql({}) or stage_filter_sql(None)
    ''
    """
    if not stage_vars:
        return ""
    parts: list[str] = []
    paired: set[str] = set()
    for k, v in stage_vars.items():
        if k.endswith("_start"):
            col = k[: -len("_start")]
            end_key = f"{col}_end"
            if end_key in stage_vars:
                parts.append(
                    f"{col} >= {_sql_literal(v)} AND {col} < {_sql_literal(stage_vars[end_key])}"
                )
                paired.add(col)
    for k, v in stage_vars.items():
        if k.endswith("_start") and k[: -len("_start")] in paired:
            continue
        if k.endswith("_end") and k[: -len("_end")] in paired:
            continue
        parts.append(f"{k} = {_sql_literal(v)}")
    return " AND ".join(parts)


def _sql_literal(v) -> str:
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    raise ValueError(f"Unsupported stage var type for SQL literalization: {type(v).__name__}={v!r}")


def fetch_max_date(
    model_name: str,
    date_column: str,
    project_dir: Path,
    profiles_dir: Path,
    where_filter: str | None = None,
) -> dt.date:
    """Run `dbt run-operation get_max_date` and parse the sentinel line.

    `where_filter` is forwarded to the macro so the runner can read max(date)
    per stage on staged models. Without it, a stage that's far behind global
    max would have its slice list exploded by the model's per-range macro
    filter into a multi-year backfill.
    """
    args = {"model_name": model_name, "date_column": date_column}
    if where_filter:
        args["where_filter"] = where_filter
    cmd = [
        "dbt",
        "run-operation",
        "get_max_date",
        "--args",
        json.dumps(args),
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    combined = _strip_ansi((proc.stdout or "") + "\n" + (proc.stderr or ""))
    for line in combined.splitlines():
        m = MAX_DATE_RESULT_RE.search(line)
        if m and m.group(1) == model_name and m.group(2) == date_column:
            return dt.date.fromisoformat(m.group(3))
    if proc.returncode != 0:
        raise RuntimeError(
            f"get_max_date failed for {model_name}.{date_column} "
            f"(rc={proc.returncode}): {proc.stderr.strip()[:500]}"
        )
    raise RuntimeError(
        f"Could not parse MAX_DATE_RESULT line for {model_name}.{date_column}. "
        f"stdout tail: {combined[-500:]!r}"
    )


def fetch_max_int(
    model_name: str,
    column_name: str,
    project_dir: Path,
    profiles_dir: Path,
) -> int | None:
    """Run `dbt run-operation get_max_int` and parse the sentinel line.

    Returns None when the upstream is empty (sentinel returned NULL).
    """
    cmd = [
        "dbt",
        "run-operation",
        "get_max_int",
        "--args",
        json.dumps({"model_name": model_name, "column_name": column_name}),
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    combined = _strip_ansi((proc.stdout or "") + "\n" + (proc.stderr or ""))
    for line in combined.splitlines():
        m = MAX_INT_RESULT_RE.search(line)
        if m and m.group(1) == model_name and m.group(2) == column_name:
            return None if m.group(3) == "NULL" else int(m.group(3))
    if proc.returncode != 0:
        raise RuntimeError(
            f"get_max_int failed for {model_name}.{column_name} "
            f"(rc={proc.returncode}): {proc.stderr.strip()[:500]}"
        )
    raise RuntimeError(
        f"Could not parse MAX_INT_RESULT line for {model_name}.{column_name}. "
        f"stdout tail: {combined[-500:]!r}"
    )


def fetch_first_seen_date(
    model_name: str,
    date_column: str,
    where_filter: str | None,
    project_dir: Path,
    profiles_dir: Path,
) -> dt.date | None:
    """Run `dbt run-operation get_first_seen_date` and parse the sentinel."""
    args = {"model_name": model_name, "date_column": date_column}
    if where_filter:
        args["where_filter"] = where_filter
    cmd = [
        "dbt",
        "run-operation",
        "get_first_seen_date",
        "--args",
        json.dumps(args),
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    combined = _strip_ansi((proc.stdout or "") + "\n" + (proc.stderr or ""))
    for line in combined.splitlines():
        m = FIRST_SEEN_RESULT_RE.search(line)
        if m and m.group(1) == model_name and m.group(2) == date_column:
            return None if m.group(3) == "NULL" else dt.date.fromisoformat(m.group(3))
    if proc.returncode != 0:
        raise RuntimeError(
            f"get_first_seen_date failed for {model_name}.{date_column} "
            f"(rc={proc.returncode}): {proc.stderr.strip()[:500]}"
        )
    return None


def fetch_distinct_values(
    model_name: str,
    column_name: str,
    project_dir: Path,
    profiles_dir: Path,
    max_values: int = 1000,
) -> list[str] | None:
    """Run `dbt run-operation get_distinct_values` and parse all sentinels.

    Returns the list of values in deterministic (lexical) order. Returns
    None if the upstream exceeded `max_values` (caller should warn / refuse).
    """
    cmd = [
        "dbt",
        "run-operation",
        "get_distinct_values",
        "--args",
        json.dumps({
            "model_name": model_name,
            "column_name": column_name,
            "max_values": max_values,
        }),
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    combined = _strip_ansi((proc.stdout or "") + "\n" + (proc.stderr or ""))
    values: list[str] = []
    saw_end = False
    for line in combined.splitlines():
        m = DISTINCT_VALUES_OVERFLOW_RE.search(line)
        if m and m.group(1) == model_name and m.group(2) == column_name:
            return None
        m = DISTINCT_VALUE_RE.search(line)
        if m and m.group(1) == model_name and m.group(2) == column_name:
            values.append(m.group(3))
            continue
        m = DISTINCT_VALUES_END_RE.search(line)
        if m and m.group(1) == model_name and m.group(2) == column_name:
            saw_end = True
    if not saw_end and proc.returncode != 0:
        raise RuntimeError(
            f"get_distinct_values failed for {model_name}.{column_name} "
            f"(rc={proc.returncode}): {proc.stderr.strip()[:500]}"
        )
    return values


# ---------------------------------------------------------------------------
# Stage synthesis from range_template
# ---------------------------------------------------------------------------


def _resolve_start_date(
    policy: str,
    today: dt.date,
    first_seen_lookup,   # callable returning dt.date | None, or None to skip
    label: str,
) -> str:
    """Resolve `auto_start_policy` to an ISO YYYY-MM-DD start date.

    Policy may be:
      - "today"          → today
      - "first_seen"     → call `first_seen_lookup()`; fall back to today on error
      - "YYYY-MM-DD"     → that literal date
    """
    if policy == "today":
        return today.isoformat()
    if policy == "first_seen":
        if first_seen_lookup is None:
            return today.isoformat()
        try:
            seen = first_seen_lookup()
            return seen.isoformat() if seen else today.isoformat()
        except Exception as exc:
            print(f"[warn] {label}: first_seen lookup failed ({exc}); "
                  f"falling back to today", file=sys.stderr)
            return today.isoformat()
    try:
        dt.date.fromisoformat(policy)
        return policy
    except ValueError:
        print(f"[warn] {label}: invalid auto_start_policy={policy!r}; "
              f"falling back to today", file=sys.stderr)
        return today.isoformat()


def _format_name(template: str, mapping: dict, fallback: str, label: str) -> str:
    try:
        return template.format(**mapping)
    except (KeyError, IndexError) as exc:
        print(f"[warn] {label}: name_template placeholder {exc} unknown; "
              f"using fallback {fallback!r}", file=sys.stderr)
        return fallback


def _additions_for_int_template(
    model_name: str,
    tmpl: dict,
    declared_stages: list[dict],
    today: dt.date,
    project_dir: Path,
    profiles_dir: Path,
    date_column: str,
) -> list[dict]:
    """Synthesize integer-range additions from a single template."""
    key_col = tmpl["key_column"]
    step = int(tmpl["step"])
    src = tmpl["discovery_source"]
    policy = tmpl.get("auto_start_policy", "today")
    name_tmpl = tmpl.get("name_template", f"{key_col}_{{start}}_{{end}}_auto")

    declared_max = max(
        (int(s["vars"].get(f"{key_col}_end", 0)) for s in declared_stages),
        default=0,
    )
    try:
        upstream_max = fetch_max_int(src, key_col, project_dir, profiles_dir)
    except Exception as exc:
        print(f"[warn] {model_name}: range_template[{key_col}] disabled "
              f"(could not read max from {src}: {exc})", file=sys.stderr)
        return []
    if upstream_max is None or upstream_max < declared_max:
        return []

    out: list[dict] = []
    cur = declared_max
    while cur <= upstream_max:
        nxt = cur + step
        label = f"{model_name} stage [{cur},{nxt})"
        start_date = _resolve_start_date(
            policy, today,
            lambda c=cur, n=nxt: fetch_first_seen_date(
                src, date_column,
                f"{key_col} >= {c} AND {key_col} < {n}",
                project_dir, profiles_dir,
            ) if policy == "first_seen" else None,
            label,
        )
        name = _format_name(
            name_tmpl,
            {"key_column": key_col, "start": cur, "end": nxt,
             "start_k": cur // 1000, "end_k": nxt // 1000, "value": f"{cur}_{nxt}"},
            fallback=f"{key_col}_{cur}_{nxt}_auto",
            label=label,
        )
        out.append({
            "name": name,
            "vars": {f"{key_col}_start": cur, f"{key_col}_end": nxt},
            "start_date": start_date,
        })
        cur = nxt
    return out


def _additions_for_enum_template(
    model_name: str,
    tmpl: dict,
    declared_stages: list[dict],
    today: dt.date,
    project_dir: Path,
    profiles_dir: Path,
    date_column: str,
) -> list[dict]:
    """Synthesize enum additions: one stage per distinct upstream value."""
    key_col = tmpl["key_column"]
    src = tmpl["enum_source"]
    policy = tmpl.get("auto_start_policy", "today")
    name_tmpl = tmpl.get("name_template", f"{key_col}_{{value}}_auto")
    max_values = int(tmpl.get("max_values", 100))

    declared_values = {
        str(s["vars"].get(key_col)) for s in declared_stages
        if s.get("vars") and s["vars"].get(key_col) is not None
    }
    try:
        values = fetch_distinct_values(src, key_col, project_dir, profiles_dir, max_values)
    except Exception as exc:
        print(f"[warn] {model_name}: enum auto-extend disabled for {key_col} "
              f"(could not enumerate from {src}: {exc})", file=sys.stderr)
        return []
    if values is None:
        print(f"[warn] {model_name}: enum {key_col} from {src} exceeded "
              f"max_values={max_values}; skipping auto-extend", file=sys.stderr)
        return []

    out: list[dict] = []
    for v in values:
        if v in declared_values:
            continue
        label = f"{model_name} stage {key_col}={v}"
        start_date = _resolve_start_date(
            policy, today,
            lambda val=v: fetch_first_seen_date(
                src, date_column,
                f"{key_col} = {_sql_literal(val)}",
                project_dir, profiles_dir,
            ) if policy == "first_seen" else None,
            label,
        )
        # Sanitize value for use in stage names.
        safe_v = re.sub(r"[^A-Za-z0-9]+", "_", v).strip("_") or "x"
        name = _format_name(
            name_tmpl,
            {"key_column": key_col, "value": safe_v, "raw_value": v,
             "start": v, "end": v, "start_k": 0, "end_k": 0},
            fallback=f"{key_col}_{safe_v}_auto",
            label=label,
        )
        out.append({
            "name": name,
            "vars": {key_col: v},
            "start_date": start_date,
        })
    return out


def _additions_for_template(
    model_name: str,
    tmpl: dict,
    declared_stages: list[dict],
    today: dt.date,
    project_dir: Path,
    profiles_dir: Path,
    date_column: str,
) -> list[dict]:
    """Dispatch on template kind: integer range vs string enum."""
    if "step" in tmpl:
        return _additions_for_int_template(
            model_name, tmpl, declared_stages, today,
            project_dir, profiles_dir, date_column,
        )
    if "enum_source" in tmpl:
        return _additions_for_enum_template(
            model_name, tmpl, declared_stages, today,
            project_dir, profiles_dir, date_column,
        )
    print(f"[warn] {model_name}: range_template has neither `step` nor "
          f"`enum_source`; nothing to discover", file=sys.stderr)
    return []


def maybe_extend_stages(
    model_name: str,
    meta: dict,
    today: dt.date,
    project_dir: Path,
    profiles_dir: Path,
) -> None:
    """Synthesize new stages from `meta.full_refresh.range_template`.

    Three template forms are supported:

    1. **Integer range** (single dim, contiguous buckets)::

         range_template:
           key_column: validator_index
           step: 100000
           discovery_source: int_consensus_validators_snapshots_daily
           auto_start_policy: first_seen
           name_template: "validators_{start_k}k_{end_k}k_auto"

    2. **Enum** (single dim, one stage per distinct value)::

         range_template:
           key_column: chain_id
           enum_source: int_chains
           auto_start_policy: first_seen
           name_template: "{key_column}_{value}_auto"

    3. **Composite** (multi-dim, cartesian product of additions)::

         range_template:
           - key_column: chain_id
             enum_source: int_chains
           - key_column: token_id
             step: 1000
             discovery_source: int_tokens

       For each dimension, the runner discovers additions independently;
       it then takes the cartesian product of the per-dim results, merges
       vars, picks the LATEST per-dim start_date (because a combined
       slice is only valid after every dimension exists), and joins the
       part-names with "_x_". Existing declared stages are skipped by name.

    The downstream `--max-slices-per-stage` cap continues to protect
    against runaway backfills regardless of policy.
    """
    tmpl_or_list = meta.get("range_template")
    if not tmpl_or_list:
        return
    templates = tmpl_or_list if isinstance(tmpl_or_list, list) else [tmpl_or_list]
    if not templates:
        return

    date_column = meta.get("date_column") or "date"
    declared_stages = list(meta["stages"])

    per_dim = [
        _additions_for_template(
            model_name, t, declared_stages, today,
            project_dir, profiles_dir, date_column,
        )
        for t in templates
    ]

    declared_names = {s["name"] for s in meta["stages"]}
    appended: list[str] = []

    if len(per_dim) == 1:
        for stage in per_dim[0]:
            if stage["name"] in declared_names:
                continue
            meta["stages"].append(stage)
            appended.append(stage["name"])
    else:
        # Composite cartesian product.
        if any(len(d) == 0 for d in per_dim):
            # If any dim has no new additions, there's nothing new to product —
            # we'd just re-emit existing combos. Skip cleanly.
            pass
        else:
            import itertools as _it
            for combo in _it.product(*per_dim):
                merged_vars: dict = {}
                for part in combo:
                    overlap = set(merged_vars) & set(part["vars"])
                    if overlap:
                        print(f"[warn] {model_name}: composite range_template var "
                              f"collision on {sorted(overlap)}; later template wins",
                              file=sys.stderr)
                    merged_vars.update(part["vars"])
                merged_name = "_x_".join(p["name"] for p in combo)
                # Latest start_date — slice valid only after every dim exists.
                latest_start = max(p["start_date"] for p in combo)
                if merged_name in declared_names:
                    continue
                meta["stages"].append({
                    "name": merged_name,
                    "vars": merged_vars,
                    "start_date": latest_start,
                })
                appended.append(merged_name)

    if appended:
        print(
            f"[info] {model_name}: range_template synthesized {len(appended)} "
            f"new stage(s): {', '.join(appended[:5])}"
            f"{' ...' if len(appended) > 5 else ''}",
            file=sys.stderr,
        )


def run_one_slice(
    model: str,
    stage: dict,
    end_date: dt.date,
    defer_args: list[str],
    project_dir: Path,
    profiles_dir: Path,
    threads: int | None,
) -> int:
    vars_payload = {
        "incremental_end_date": end_date.isoformat(),
        **(stage.get("vars") or {}),
    }
    cmd = [
        "dbt",
        "run",
        "--select",
        model,
        "--vars",
        json.dumps(vars_payload),
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
    ]
    if threads is not None:
        cmd.extend(["--threads", str(threads)])
    cmd.extend(defer_args)
    return subprocess.run(cmd).returncode


def run_passthrough(
    selector: str,
    defer_args: list[str],
    project_dir: Path,
    profiles_dir: Path,
    threads: int | None,
) -> int:
    cmd = [
        "dbt",
        "run",
        "--select",
        selector,
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
    ]
    if threads is not None:
        cmd.extend(["--threads", str(threads)])
    cmd.extend(defer_args)
    return subprocess.run(cmd).returncode


def run_kill_failed_mutations(project_dir: Path, profiles_dir: Path) -> None:
    """Self-heal: drop any poisoned mutations from a previous crash."""
    cmd = [
        "dbt",
        "run-operation",
        "kill_failed_mutations",
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
    ]
    try:
        subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=60)
    except Exception as exc:  # pragma: no cover - best-effort
        print(f"[warn] kill_failed_mutations call failed: {exc}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Slice list generation
# ---------------------------------------------------------------------------


def slice_end_dates(
    start: dt.date, end: dt.date, step: int
) -> Iterator[dt.date]:
    """Yield slice end-dates `start, start+step, ..., end`.

    The final yielded value is always `end` (capped) when `start <= end`,
    so that the catch-up always lands exactly on the requested end date.
    Duplicates are suppressed when `start + k*step == end`.
    """
    if step < 1:
        raise ValueError("step must be >= 1")
    if start > end:
        return
    cur = start
    last_yielded: dt.date | None = None
    while cur < end:
        yield cur
        last_yielded = cur
        cur += dt.timedelta(days=step)
    if last_yielded != end:
        yield end


# ---------------------------------------------------------------------------
# State file
# ---------------------------------------------------------------------------


def load_state(state_path: Path) -> dict:
    if not state_path.exists():
        return {"completed": {}}
    try:
        return json.loads(state_path.read_text())
    except Exception:
        print(
            f"[warn] state file {state_path} unreadable; starting fresh",
            file=sys.stderr,
        )
        return {"completed": {}}


def save_state(state_path: Path, state: dict) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_path.with_suffix(state_path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True))
    tmp.replace(state_path)


def state_key(model: str, stage_name: str) -> str:
    return f"{model}::{stage_name}"


# ---------------------------------------------------------------------------
# Failure stashing
# ---------------------------------------------------------------------------


def stash_failure(
    project_dir: Path,
    model: str,
    stage_name: str,
    end_date: dt.date,
    invocation_id: str | None = None,
) -> None:
    """Copy target/run_results.json to target/failed_batches/<unique>.json.

    The filename embeds an `invocation_id` so concurrent / sequential failures
    do not overwrite each other — that was the bug where every plain-passthrough
    failure stashed under `microbatch-_plain-_passthrough-<today>.json` and the
    last write clobbered the rest, hiding earlier failures from
    classify_failed_nodes.
    """
    rr = project_dir / "target" / "run_results.json"
    if not rr.exists():
        return
    failed_dir = project_dir / "target" / "failed_batches"
    failed_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"-{invocation_id}" if invocation_id else ""
    dest = failed_dir / (
        f"microbatch-{model}-{stage_name}-{end_date.isoformat()}{suffix}.json"
    )
    shutil.copy2(rr, dest)


# ---------------------------------------------------------------------------
# Plan / execute
# ---------------------------------------------------------------------------


def partition_selected(
    model_names: list[str], manifest: dict
) -> list[tuple[str, str, dict, dict | None]]:
    """Walk selected models in topological order and emit a single sequence
    of ("plain", name, node, None) or ("micro", name, node, meta) entries.

    The runner consumes this in order and groups consecutive plain entries
    into one `dbt run --select` invocation, flushing the buffer at every
    microbatch boundary. This guarantees that:
      - a microbatch model whose plain *upstream* is in the same batch sees
        that upstream built before its slices run;
      - a plain model whose *upstream* is microbatched sees the microbatch
        completed before it runs.
    Both deadlock corners are eliminated.
    """
    nodes = selected_nodes(model_names, manifest)
    out: list[tuple[str, str, dict, dict | None]] = []
    for name in model_names:
        node = nodes[name]
        meta = get_microbatch_meta(node)
        kind = "plain" if meta is None else "micro"
        out.append((kind, name, node, meta))
    return out


def plan_for_model(
    name: str,
    meta: dict,
    today: dt.date,
    state: dict,
    project_dir: Path,
    profiles_dir: Path,
    batch_days_override: int | None,
    bootstrap_lookback_days: int,
    dry_run: bool,
    stage_filter: list[str] | None = None,
    max_slices_per_stage: int = 30,
) -> list[tuple[dict, list[dt.date]]]:
    """Compute (stage, [slice_end_dates]) list for a single microbatch model.

    `max_date` is fetched **per stage** using a derived WHERE filter from the
    stage's vars (see `stage_filter_sql`). This ensures that a stage which is
    far behind global max — e.g. a validator-range stage covering indexes
    that arrived later — gets a slice list bounded by *its own* progress, not
    by the model-wide global max. Without this, the model's per-range macro
    filter would expand the runner's "1 day forward" into a multi-year
    backfill in one INSERT.
    """
    plan: list[tuple[dict, list[dt.date]]] = []
    batch_days = batch_days_override or meta["batch_days"]

    for stage in meta["stages"]:
        if stage_filter is not None and stage["name"] not in stage_filter:
            plan.append((stage, []))
            continue

        where_sql = stage_filter_sql(stage.get("vars") or {}) or None

        # Query the DB for per-stage max even in --dry-run so the printed plan
        # reflects reality. The query is read-only.
        try:
            max_t = fetch_max_date(
                name,
                meta["date_column"],
                project_dir,
                profiles_dir,
                where_filter=where_sql,
            )
        except Exception as exc:
            print(
                f"[warn] could not read max({meta['date_column']}) for "
                f"{name} stage={stage['name']}: {exc}; assuming bootstrap "
                f"(today - {bootstrap_lookback_days}d)",
                file=sys.stderr,
            )
            max_t = today - dt.timedelta(days=bootstrap_lookback_days)

        # Bootstrap policy: when the per-stage query returns the macro's
        # 1970 sentinel (target empty for this range) we bootstrap from
        # `today - bootstrap_lookback_days` — NOT from meta.full_refresh.
        # start_date. Multi-year historical backfill is the job of
        # scripts/full_refresh/refresh.py (which uses start_month/end_month
        # append batching); the microbatch runner is for daily catch-up
        # only. If the stage's declared start_date is *more recent* than
        # the bootstrap lookback (e.g. a stage deployed yesterday), we
        # respect that as a lower bound so the runner doesn't try to write
        # source rows from before the stage existed.
        SENTINEL = dt.date(1970, 1, 1)
        if max_t <= SENTINEL:
            bootstrap_default = today - dt.timedelta(days=bootstrap_lookback_days)
            stage_start = stage.get("start_date")
            stage_start_dt: dt.date | None = None
            if stage_start:
                try:
                    stage_start_dt = dt.date.fromisoformat(stage_start)
                except ValueError:
                    stage_start_dt = None
            # If the stage starts after our default bootstrap, jump forward
            # to the day before the stage start so the first slice is the
            # stage's first day.
            if stage_start_dt and stage_start_dt > bootstrap_default:
                max_t = stage_start_dt - dt.timedelta(days=1)
            else:
                max_t = bootstrap_default
            print(
                f"[info] {name} stage={stage['name']}: empty target → "
                f"bootstrapping from {(max_t + dt.timedelta(days=1)).isoformat()} "
                f"(use scripts/full_refresh/refresh.py for historical backfill)",
                file=sys.stderr,
            )

        last_done_iso = (
            state.get("completed", {})
            .get(state_key(name, stage["name"]), {})
            .get("last_completed_end_date")
        )
        last_done = dt.date.fromisoformat(last_done_iso) if last_done_iso else None
        floor = max_t + dt.timedelta(days=1)
        if last_done is not None:
            floor = max(floor, last_done + dt.timedelta(days=1))
        if floor > today:
            plan.append((stage, []))
            continue
        slices = list(slice_end_dates(floor, today, batch_days))

        # Cap: refuse to do multi-month backfill. Microbatch is for daily
        # catch-up only — the right tool for historical fill is
        # scripts/full_refresh/refresh.py.
        if max_slices_per_stage > 0 and len(slices) > max_slices_per_stage:
            print(
                f"[error] {name} stage={stage['name']}: gap is "
                f"{len(slices)} day(s) (target max={max_t}, today={today}); "
                f"this exceeds --max-slices-per-stage={max_slices_per_stage}. "
                f"The microbatch runner is for daily catch-up only. Run "
                f"`python scripts/full_refresh/refresh.py --select {name} "
                f"--stage {stage['name']}` to backfill the gap, then re-run "
                f"the microbatch runner.",
                file=sys.stderr,
            )
            plan.append((stage, []))
            continue
        plan.append((stage, slices))
    return plan


# ---------------------------------------------------------------------------
# CLI / main
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Microbatch runner for annotated incremental dbt models."
    )
    p.add_argument("--select", required=True, help="dbt selector")
    p.add_argument("--project-dir", default=".")
    p.add_argument(
        "--profiles-dir", default=str(Path.home() / ".dbt"),
    )
    p.add_argument(
        "--max-end-date",
        help="Cap slice end date (YYYY-MM-DD). Defaults to today UTC.",
    )
    p.add_argument(
        "--batch-days",
        type=int,
        default=None,
        help="Global override for per-model batch_days.",
    )
    p.add_argument(
        "--bootstrap-lookback-days",
        type=int,
        default=7,
        help=(
            "When max(date) cannot be read (target empty / first run), "
            "begin slicing from today - N days."
        ),
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--resume", action="store_true")
    p.add_argument("--delay", type=float, default=0.0, help="Sleep between slices.")
    p.add_argument("--threads", type=int, default=None)
    p.add_argument("--defer", action="store_true")
    p.add_argument("--favor-state", action="store_true")
    p.add_argument("--state", help="Path forwarded as --state to dbt run for deferral.")
    p.add_argument(
        "--no-kill-failed-mutations",
        action="store_true",
        help="Skip the pre-run kill_failed_mutations self-heal.",
    )
    p.add_argument(
        "--state-file",
        default=None,
        help="Override path to the microbatch state file (default: <project>/target/incremental_microbatch_state.json).",
    )
    p.add_argument(
        "--stage",
        action="append",
        default=None,
        help=(
            "Restrict execution to specific stage names from "
            "meta.full_refresh.stages. Pass multiple times to allow several. "
            "Stages not listed are skipped entirely (their slice list is empty)."
        ),
    )
    p.add_argument(
        "--max-slices-per-stage",
        type=int,
        default=30,
        help=(
            "Refuse to run a stage that would need more than N slices in a "
            "single invocation. The microbatch runner is for daily catch-up; "
            "multi-month historical backfill belongs to "
            "scripts/full_refresh/refresh.py. Default 30 (≈ one month). "
            "Set to 0 to disable the cap (not recommended on the cron path)."
        ),
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    project_dir = Path(args.project_dir).resolve()
    profiles_dir = Path(args.profiles_dir).resolve()
    state_path = (
        Path(args.state_file).resolve()
        if args.state_file
        else project_dir / "target" / "incremental_microbatch_state.json"
    )

    today = (
        dt.date.fromisoformat(args.max_end_date)
        if args.max_end_date
        else dt.date.today()
    )

    defer_args: list[str] = []
    if args.defer:
        defer_args.append("--defer")
    if args.favor_state:
        defer_args.append("--favor-state")
    if args.state:
        defer_args.extend(["--state", args.state])

    if not args.dry_run and not args.no_kill_failed_mutations:
        run_kill_failed_mutations(project_dir, profiles_dir)

    # Resolve selector → ordered model list (topo) → manifest nodes.
    model_names = selected_models(project_dir, profiles_dir, args.select)
    if not model_names:
        print("[info] selector matched no models; nothing to do")
        return 0
    manifest = load_manifest(project_dir)
    ordered = topo_sort(model_names, manifest)
    entries = partition_selected(ordered, manifest)

    state = load_state(state_path) if args.resume else {"completed": {}}

    # Unique-per-invocation suffix so plain-passthrough stash filenames don't
    # collide across multiple invocations (or across multiple flushes within
    # one invocation, since we now flush at every microbatch boundary).
    invocation_id = (
        f"{int(time.time())}-{os.getpid()}"
    )
    plain_flush_counter = {"n": 0}

    failures: list[tuple[str, str, str]] = []

    def flush_plain(buf: list[str]) -> None:
        """Run accumulated plain models as a single `dbt run --select` and
        record any failure into the shared `failures` list. Stash uses a
        unique filename so simultaneous failures across batches survive."""
        if not buf:
            return
        selector = " ".join(buf)
        if args.dry_run:
            print(f"[dry-run] dbt run --select '{selector}'")
            buf.clear()
            return
        rc = run_passthrough(
            selector, defer_args, project_dir, profiles_dir, args.threads
        )
        plain_flush_counter["n"] += 1
        if rc != 0:
            stash_failure(
                project_dir,
                "_plain",
                f"_passthrough_{plain_flush_counter['n']:02d}",
                today,
                invocation_id=invocation_id,
            )
            failures.append(("_plain", f"passthrough#{plain_flush_counter['n']}", "-"))
        buf.clear()

    plain_buffer: list[str] = []

    for kind, name, node, meta in entries:
        if kind == "plain":
            plain_buffer.append(name)
            continue

        # microbatch: flush any pending plain *upstream* first so the
        # microbatch's macro can read max(date) and any plain dependencies
        # exist in the warehouse.
        flush_plain(plain_buffer)

        # Auto-extend stages from range_template if the model declared one.
        maybe_extend_stages(name, meta, today, project_dir, profiles_dir)
        try:
            plan = plan_for_model(
                name,
                meta,
                today,
                state,
                project_dir,
                profiles_dir,
                args.batch_days,
                args.bootstrap_lookback_days,
                args.dry_run,
                stage_filter=args.stage,
                max_slices_per_stage=args.max_slices_per_stage,
            )
        except Exception as exc:
            print(f"[error] planning {name} failed: {exc}", file=sys.stderr)
            failures.append((name, "_plan", "-"))
            continue

        for stage, slices in plan:
            stage_name = stage["name"]
            if not slices:
                tag = "[dry-run]" if args.dry_run else "[info]"
                print(f"{tag} {name} stage={stage_name}: nothing to do (caught up or filtered)")
                continue
            for end_date in slices:
                if args.dry_run:
                    payload = {
                        "incremental_end_date": end_date.isoformat(),
                        **(stage.get("vars") or {}),
                    }
                    print(
                        f"[dry-run] dbt run -s {name} "
                        f"--vars '{json.dumps(payload, sort_keys=True)}'  "
                        f"# stage={stage_name}"
                    )
                    continue
                rc = run_one_slice(
                    name,
                    stage,
                    end_date,
                    defer_args,
                    project_dir,
                    profiles_dir,
                    args.threads,
                )
                if rc != 0:
                    failures.append((name, stage_name, end_date.isoformat()))
                    stash_failure(
                        project_dir, name, stage_name, end_date,
                        invocation_id=invocation_id,
                    )
                    # Stop advancing this stage; cumulative-window models
                    # depend on monotonic progress.
                    break
                state.setdefault("completed", {}).setdefault(
                    state_key(name, stage_name), {}
                )["last_completed_end_date"] = end_date.isoformat()
                save_state(state_path, state)
                if args.delay:
                    time.sleep(args.delay)

    # Final flush for any plain models trailing the last microbatch (or all
    # plain when no microbatch was matched).
    flush_plain(plain_buffer)

    if failures:
        print("[error] runner failures:", file=sys.stderr)
        for f in failures:
            print(f"  - {f[0]} stage={f[1]} end={f[2]}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
