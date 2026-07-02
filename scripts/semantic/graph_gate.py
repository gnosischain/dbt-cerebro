"""Scoped CI gate for graph-metadata validation (WS8).

The broad ``build_registry.py --validate`` step runs ``continue-on-error`` in CI
because ~dozens of pre-existing non-graph validation errors are still being
cleaned up. This gate is the opposite: it is a *blocking* check scoped to
``graph_meta_*`` errors only, so new graph mistakes (a typo'd node kind, a column
that doesn't exist, a duplicate profile id) fail the build immediately without
waiting for the legacy backlog to clear.

It compares the freshly-written ``semantic_validation_report.json`` against a
committed baseline (``semantic/validation/baseline.json``). Each error is keyed
by ``(code, model)`` — by IDENTITY, not list position (D6) — so editing a model
that already carries a baselined error does not re-flag that error as net-new.
Only ``graph_meta_*`` errors absent from the baseline fail the gate.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

GRAPH_ERROR_PREFIX = "graph_meta_"


def graph_error_key(issue: dict[str, Any]) -> tuple[str, str]:
    """Identity of a graph error: (code, model). Stable across re-ordering."""
    return (issue.get("code", ""), issue.get("model", ""))


def graph_errors(report: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        e
        for e in report.get("errors", [])
        if str(e.get("code", "")).startswith(GRAPH_ERROR_PREFIX)
    ]


def net_new_graph_errors(
    report: dict[str, Any], baseline: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Graph errors in the report whose (code, model) is not in the baseline."""
    baselined = {graph_error_key(e) for e in baseline}
    return [e for e in graph_errors(report) if graph_error_key(e) not in baselined]


def load_baseline(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        data = data.get("graph_errors", [])
    return data or []


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-dir", default="target")
    parser.add_argument(
        "--baseline",
        default=None,
        help="Path to baseline.json (default: <repo>/semantic/validation/baseline.json)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    target_dir = Path(args.target_dir)
    report_path = target_dir / "semantic_validation_report.json"
    if not report_path.exists():
        print(f"graph-gate: {report_path} not found; run build_registry.py --validate first", file=sys.stderr)
        return 2
    repo_root = Path(__file__).resolve().parents[2]
    baseline_path = (
        Path(args.baseline)
        if args.baseline
        else repo_root / "semantic" / "validation" / "baseline.json"
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    baseline = load_baseline(baseline_path)
    net_new = net_new_graph_errors(report, baseline)
    total_graph = len(graph_errors(report))
    if net_new:
        print(
            f"graph-gate FAILED: {len(net_new)} net-new graph error(s) "
            f"({total_graph} total, {total_graph - len(net_new)} baselined):",
            file=sys.stderr,
        )
        for e in net_new:
            print(f"  - [{e.get('code')}] {e.get('message')}", file=sys.stderr)
        print(
            "Fix the graph metadata, or (if intentional) add the (code, model) to "
            f"{baseline_path.relative_to(repo_root)}.",
            file=sys.stderr,
        )
        return 1
    print(f"graph-gate OK: 0 net-new graph errors ({total_graph} baselined).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
