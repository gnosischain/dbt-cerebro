"""Emit per-model dbt run status as a Prometheus textfile.

The observability server (app/observability_server.py) concatenates every
``*.prom`` file it finds under ``$RUNTIME_DATA_DIR/metrics`` into its
``/metrics`` scrape output. This script produces one such file so that
Grafana can show, for the *last run*:

  - which production models succeeded / errored / were skipped
  - which production models were never touched (``status="pending"``)
  - a real total-vs-done progress ratio
  - per-model build duration

Why parse ``dbt.log`` instead of ``run_results.json``?
The preview/prod orchestrator runs dbt in many small batches and microbatch
slices (see scripts/run_dbt_observability.sh + dbt_incremental_runner.py).
Each invocation OVERWRITES ``target/run_results.json``, so that file only ever
reflects the last slice. The dbt log file, by contrast, is append-only for the
whole pod run and therefore the only complete record of what actually ran.

The authoritative *expected* set comes from ``target/manifest.json`` (models
tagged ``production``) — the same selection the orchestrator runs. A production
model with no terminal log line this run is reported as ``pending``.

Stdlib only; safe to run even if inputs are missing (emits a minimal file so
the metric series still exist and Grafana panels render "0" rather than "No
data").
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

# ── dbt log line patterns ───────────────────────────────────────────────────
# Console + file logs both carry these substrings (with ANSI colour codes
# around the status word, which these regexes deliberately ignore).
#   "... OK created sql table model `dbt`.`NAME`  [OK in 2.48s]"
#   "... ERROR creating sql table model `dbt`.`NAME`  [ERROR in 4.17s]"
#   "... SKIP relation dbt.NAME ....."
_BACKTICK_MODEL = r"`[^`]+`\.`(?P<model>[^`]+)`"
_DURATION = r"in (?P<secs>[0-9.]+)s"

RE_OK = re.compile(r"OK created \w+ (?:\w+ )?model " + _BACKTICK_MODEL)
RE_ERROR = re.compile(r"ERROR creating \w+ (?:\w+ )?model " + _BACKTICK_MODEL)
RE_SKIP = re.compile(r"SKIP relation \w+\.(?P<model>\w+)")
RE_DURATION = re.compile(_DURATION)


def parse_args(argv: list[str]) -> argparse.Namespace:
    runtime = os.environ.get("RUNTIME_DATA_DIR", "/data")
    project = os.environ.get("PROJECT_DIR", ".")
    default_logs = os.environ.get("DBT_LOG_PATH", os.path.join(runtime, "logs"))
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", default=os.path.join(project, "target", "manifest.json"))
    p.add_argument(
        "--log",
        default=os.path.join(default_logs, "dbt.log"),
        help="dbt log file to parse for per-model status (append-only for the run).",
    )
    p.add_argument(
        "--out",
        default=os.path.join(runtime, "metrics", "dbt_model_status.prom"),
        help="Prometheus textfile to write (scraped by the observability server).",
    )
    p.add_argument(
        "--production-tag",
        default="production",
        help="manifest node tag identifying the run selection.",
    )
    return p.parse_args(argv)


def load_production_models(manifest_path: str, tag: str) -> dict[str, dict]:
    """Return {model_name: {materialization, layer}} for tagged, enabled models."""
    models: dict[str, dict] = {}
    try:
        with open(manifest_path, encoding="utf-8") as fh:
            manifest = json.load(fh)
    except (OSError, ValueError) as exc:
        print(f"[warn] could not read manifest {manifest_path}: {exc}", file=sys.stderr)
        return models

    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        if tag not in (node.get("tags") or []):
            continue
        config = node.get("config") or {}
        if config.get("enabled") is False:
            continue
        path = node.get("path") or ""
        # Directory of the model file → "consensus/staging",
        # "execution/gnosis_app_gt/intermediate", … (used for grouping).
        parts = [seg for seg in path.split("/") if seg]
        layer = "/".join(parts[:-1])
        models[node["name"]] = {
            "materialization": config.get("materialized") or node.get("materialized") or "",
            "layer": layer,
        }
    return models


def parse_log(log_path: str) -> tuple[dict[str, str], dict[str, float]]:
    """Return (status_by_model, seconds_by_model) from the dbt log.

    Last terminal line wins for status, so a model that errored on a batch and
    then succeeded on the transient-retry is correctly reported as ``success``.
    Durations are summed across slices/retries (total build time this run).
    """
    status: dict[str, str] = {}
    seconds: dict[str, float] = {}
    try:
        fh = open(log_path, encoding="utf-8", errors="replace")
    except OSError as exc:
        print(f"[warn] could not read dbt log {log_path}: {exc}", file=sys.stderr)
        return status, seconds

    with fh:
        for line in fh:
            m = RE_OK.search(line)
            if m:
                name = m.group("model")
                status[name] = "success"
                dur = RE_DURATION.search(line)
                if dur:
                    seconds[name] = seconds.get(name, 0.0) + float(dur.group("secs"))
                continue
            m = RE_ERROR.search(line)
            if m:
                name = m.group("model")
                status[name] = "error"
                dur = RE_DURATION.search(line)
                if dur:
                    seconds[name] = seconds.get(name, 0.0) + float(dur.group("secs"))
                continue
            m = RE_SKIP.search(line)
            if m:
                status[m.group("model")] = "skipped"
    return status, seconds


def _label(value: str) -> str:
    """Escape a Prometheus label value."""
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def build_payload(
    models: dict[str, dict],
    status: dict[str, str],
    seconds: dict[str, float],
) -> str:
    lines: list[str] = []
    counts = {"success": 0, "error": 0, "skipped": 0, "pending": 0}

    lines.append(
        "# HELP dbt_model_status Last-run status of each production dbt model "
        "(1 = current status; status one of success|error|skipped|pending)."
    )
    lines.append("# TYPE dbt_model_status gauge")
    for name in sorted(models):
        meta = models[name]
        st = status.get(name, "pending")
        counts[st] = counts.get(st, 0) + 1
        lines.append(
            'dbt_model_status{model="%s",materialization="%s",layer="%s",status="%s"} 1'
            % (_label(name), _label(meta["materialization"]), _label(meta["layer"]), st)
        )

    lines.append(
        "# HELP dbt_model_run_seconds Total wall-clock seconds building the model "
        "in the last run (summed across slices/retries)."
    )
    lines.append("# TYPE dbt_model_run_seconds gauge")
    for name in sorted(seconds):
        # Only emit durations for models in the production selection.
        if name in models:
            lines.append('dbt_model_run_seconds{model="%s"} %g' % (_label(name), seconds[name]))

    total = len(models)
    lines.append("# HELP dbt_run_models_total Production models expected in the last run.")
    lines.append("# TYPE dbt_run_models_total gauge")
    lines.append("dbt_run_models_total %d" % total)
    for key in ("success", "error", "skipped", "pending"):
        lines.append("# TYPE dbt_run_models_%s gauge" % key)
        lines.append("dbt_run_models_%s %d" % (key, counts.get(key, 0)))

    lines.append("# HELP dbt_run_emit_timestamp_seconds Unix time these metrics were written.")
    lines.append("# TYPE dbt_run_emit_timestamp_seconds gauge")
    lines.append("dbt_run_emit_timestamp_seconds %d" % int(time.time()))

    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    models = load_production_models(args.manifest, args.production_tag)
    status, seconds = parse_log(args.log)
    payload = build_payload(models, status, seconds)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp_path.write_text(payload, encoding="utf-8")
    tmp_path.replace(out_path)  # atomic — server never reads a half-written file

    done = sum(1 for m in models if status.get(m) == "success")
    print(
        "[info] emit_model_status_metrics: %d production models, %d success, "
        "%d error, %d skipped, %d pending -> %s"
        % (
            len(models),
            done,
            sum(1 for m in models if status.get(m) == "error"),
            sum(1 for m in models if status.get(m) == "skipped"),
            sum(1 for m in models if m not in status),
            out_path,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
