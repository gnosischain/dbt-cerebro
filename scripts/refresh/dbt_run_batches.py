#!/usr/bin/env python3
"""Generate lineage-aware dbt run batches for a selector."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


ANSI_ESCAPE_RE = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
MODEL_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
SKIP_PATTERNS = (
    re.compile(r"^\d{2}:\d{2}:\d{2}"),
    re.compile(r"^Running with dbt", re.IGNORECASE),
    re.compile(r"^Registered adapter", re.IGNORECASE),
    re.compile(r"^Found \d+ models", re.IGNORECASE),
    re.compile(r"^Concurrency:", re.IGNORECASE),
    re.compile(r"^Done\.", re.IGNORECASE),
)


def strip_ansi(text: str) -> str:
    """Remove ANSI escape sequences from dbt output."""
    return ANSI_ESCAPE_RE.sub("", text)


def run_dbt_command(project_dir: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    """Run a dbt command from the project root and surface stderr on failure."""
    cmd = ["dbt", *args]
    try:
        return subprocess.run(
            cmd,
            cwd=project_dir,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        if exc.stdout:
            print(exc.stdout, file=sys.stderr, end="" if exc.stdout.endswith("\n") else "\n")
        if exc.stderr:
            print(exc.stderr, file=sys.stderr, end="" if exc.stderr.endswith("\n") else "\n")
        raise


def clean_dbt_model_names(stdout: str) -> list[str]:
    """Filter dbt ls stdout down to model names only."""
    models: list[str] = []
    for raw_line in stdout.splitlines():
        line = strip_ansi(raw_line).strip()
        if not line:
            continue
        if any(pattern.match(line) for pattern in SKIP_PATTERNS):
            continue
        if MODEL_NAME_RE.match(line):
            models.append(line)
    return models


def selected_models(project_dir: Path, profiles_dir: Path, selector: str) -> list[str]:
    """Refresh manifest and return selected model names."""
    run_dbt_command(
        project_dir,
        [
            "parse",
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(profiles_dir),
        ],
    )
    result = run_dbt_command(
        project_dir,
        [
            "ls",
            "--select",
            selector,
            "--resource-type",
            "model",
            "--output",
            "name",
            "--quiet",
            "--project-dir",
            str(project_dir),
            "--profiles-dir",
            str(profiles_dir),
        ],
    )
    # Preserve the first-seen order from dbt ls while guarding against any
    # duplicate lines in stdout.
    return list(dict.fromkeys(clean_dbt_model_names(result.stdout)))


def load_manifest(project_dir: Path) -> dict:
    """Load the dbt manifest produced by dbt parse."""
    manifest_path = project_dir / "target" / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found at {manifest_path}")
    with manifest_path.open() as handle:
        return json.load(handle)


def selected_nodes(models: list[str], manifest: dict) -> dict[str, dict]:
    """Return selected manifest nodes keyed by model name."""
    selected = set(models)
    nodes: dict[str, dict] = {}
    duplicates: set[str] = set()

    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        name = node.get("name")
        if name not in selected:
            continue
        if name in nodes:
            duplicates.add(name)
        nodes[name] = node

    if duplicates:
        dup_list = ", ".join(sorted(duplicates))
        raise RuntimeError(f"Duplicate model names found in selection: {dup_list}")

    missing = selected - set(nodes)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise RuntimeError(f"Selected models missing from manifest: {missing_list}")

    return nodes


def topo_sort(models: list[str], manifest: dict) -> list[str]:
    """Topologically sort selected models using manifest dependencies."""
    nodes = selected_nodes(models, manifest)

    def sort_key(name: str) -> tuple[str, str]:
        node = nodes[name]
        return (
            node.get("original_file_path")
            or node.get("path")
            or "",
            name,
        )

    deps: dict[str, set[str]] = {}
    dependents: dict[str, set[str]] = {name: set() for name in nodes}
    for name, node in nodes.items():
        selected_deps = {
            dep.split(".")[-1]
            for dep in node.get("depends_on", {}).get("nodes", [])
            if dep.startswith("model.") and dep.split(".")[-1] in nodes
        }
        deps[name] = selected_deps
        for dep_name in selected_deps:
            dependents[dep_name].add(name)

    ready = sorted((name for name, node_deps in deps.items() if not node_deps), key=sort_key)
    ordered: list[str] = []

    while ready:
        name = ready.pop(0)
        ordered.append(name)
        for dependent in sorted(dependents[name], key=sort_key):
            deps[dependent].remove(name)
            if not deps[dependent] and dependent not in ordered and dependent not in ready:
                ready.append(dependent)
        ready.sort(key=sort_key)

    if len(ordered) != len(models):
        unresolved = sorted(set(models) - set(ordered))
        raise RuntimeError(
            "Cycle or missing node in selected model graph: " + ", ".join(unresolved)
        )

    return ordered


def build_graph(models: list[str], manifest: dict) -> tuple[dict[str, set[str]], dict[str, set[str]], list[str], dict[str, int]]:
    """Return dependency maps plus a deterministic topological order."""
    nodes = selected_nodes(models, manifest)
    ordered = topo_sort(models, manifest)
    ordered_set = set(ordered)

    deps: dict[str, set[str]] = {}
    dependents: dict[str, set[str]] = {name: set() for name in ordered}
    for name, node in nodes.items():
        selected_deps = {
            dep.split(".")[-1]
            for dep in node.get("depends_on", {}).get("nodes", [])
            if dep.startswith("model.") and dep.split(".")[-1] in ordered_set
        }
        deps[name] = selected_deps
        for dep_name in selected_deps:
            dependents[dep_name].add(name)

    topo_index = {name: idx for idx, name in enumerate(ordered)}
    return deps, dependents, ordered, topo_index


def build_chain(
    start: str,
    done: set[str],
    remaining: set[str],
    deps: dict[str, set[str]],
    dependents: dict[str, set[str]],
    topo_index: dict[str, int],
 ) -> list[str]:
    """Build one runnable chain starting from a selected node."""
    chain: list[str] = []
    chain_set: set[str] = set()
    current = start

    while True:
        chain.append(current)
        chain_set.add(current)

        candidates = [
            child
            for child in dependents[current]
            if child in remaining
            and child not in chain_set
            and deps[child] <= (done | chain_set)
        ]
        if not candidates:
            break

        current = min(candidates, key=topo_index.get)

    return chain


def build_chains(
    models: list[str],
    manifest: dict,
 ) -> list[list[str]]:
    """Peel the DAG into runnable complete chains without model repetition."""
    deps, dependents, ordered, topo_index = build_graph(models, manifest)
    done: set[str] = set()
    remaining = set(ordered)
    chains: list[list[str]] = []

    while remaining:
        start = next((name for name in ordered if name in remaining and deps[name] <= done), None)
        if start is None:
            unresolved = ", ".join(sorted(remaining))
            raise RuntimeError(f"No runnable chain start found for remaining models: {unresolved}")

        chain = build_chain(start, done, remaining, deps, dependents, topo_index)
        chains.append(chain)
        done.update(chain)
        remaining.difference_update(chain)

    return chains


def build_batches(
    models: list[str],
    manifest: dict,
    chains_per_batch: int,
) -> list[list[list[str]]]:
    """Group runnable chains into batches measured in chain units."""
    chains = build_chains(models, manifest)
    batches: list[list[list[str]]] = []
    for start in range(0, len(chains), chains_per_batch):
        batches.append(chains[start : start + chains_per_batch])
    return batches


def emit_batches(batches: list[list[list[str]]], preview: bool = False) -> None:
    """Emit machine- or human-readable batches."""
    batch_id = 1
    for chains in batches:
        batch_models = [model for chain in chains for model in chain]
        selector = " ".join(batch_models)
        if preview:
            print(f"{batch_id:03d} ({len(batch_models)} model(s), {len(chains)} chain(s)):")
            for chain_index, chain in enumerate(chains, start=1):
                print(f"  {chain_index}. {' -> '.join(chain)}")
        else:
            print(f"{batch_id:03d}\t{len(batch_models)}\t{len(chains)}\t{selector}")
        batch_id += 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate lineage-aware dbt run batches")
    parser.add_argument("--select", default="tag:production", help="dbt selector to batch")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5,
        help="Maximum number of complete chains per generated batch",
    )
    parser.add_argument(
        "--project-dir",
        default=".",
        help="Path to the dbt project directory",
    )
    parser.add_argument(
        "--profiles-dir",
        default=str(Path.home() / ".dbt"),
        help="Path to the dbt profiles directory",
    )
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Print human-readable output instead of TSV",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.batch_size < 1:
        print("--batch-size must be at least 1", file=sys.stderr)
        return 2

    project_dir = Path(args.project_dir).resolve()
    profiles_dir = Path(args.profiles_dir).resolve()

    models = selected_models(project_dir, profiles_dir, args.select)
    if not models:
        return 0

    manifest = load_manifest(project_dir)
    batches = build_batches(models, manifest, args.batch_size)
    emit_batches(batches, preview=args.preview)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
