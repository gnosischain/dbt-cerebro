#!/usr/bin/env python3
"""
dbt Full Refresh Orchestrator

A lightweight wrapper that enables batched full refreshes of large dbt models
while leveraging dbt's native features (meta, tags, selectors, manifest).

Usage:
    python refresh.py --select tag:production
    python refresh.py --select int_execution_tokens_balances_daily --dry-run
    python refresh.py --select tag:tokens --resume
    
    # Add new token without destroying existing data:
    python refresh.py --select model_name --stage new_token --incremental-only

    # Dev workflow: batch refresh in playground, defer upstream refs to prod:
    python refresh.py --select model_name --defer --state ./prod-state/ --incremental-only
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, date
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any

# ============================================================================
# Configuration
# ============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = PROJECT_ROOT / "target" / "manifest.json"
STATE_FILE = Path(__file__).parent / ".refresh_state.json"


# ============================================================================
# State Management
# ============================================================================

def load_state() -> dict:
    """Load progress state from JSON file."""
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"completed_models": [], "current_model": None, "current_batch": 0}


def save_state(state: dict) -> None:
    """Save progress state to JSON file."""
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)


def clear_state() -> None:
    """Remove state file on successful completion."""
    if STATE_FILE.exists():
        STATE_FILE.unlink()


# ============================================================================
# dbt Integration
# ============================================================================

# In-process engine (opt-in via --inprocess / REFRESH_INPROCESS=1).
#
# The subprocess engine pays a full dbt startup + project re-parse (~14s on this
# project) on EVERY batch, because each batch is a fresh `dbt` process and the
# changing --vars invalidate the partial-parse cache. Across a ~1000-batch
# backfill that is hours of pure parsing. dbt's programmatic dbtRunner accepts a
# pre-parsed manifest, so we parse ONCE and reuse it for every batch.
#
# Safe because the batch vars (start_month/end_month/stage vars) are referenced
# in model BODIES, which render at execution time per invocation. CAVEAT: any
# var() used at PARSE time (inside config() blocks / model selection) is frozen
# at the initial parse — do not use --inprocess with parse-time vars.
_DBT_MANIFEST = None

def _run_dbt_inprocess(args: List[str], capture: bool = False) -> subprocess.CompletedProcess:
    global _DBT_MANIFEST
    from dbt.cli.main import dbtRunner

    prev_cwd = os.getcwd()
    os.chdir(PROJECT_ROOT)
    try:
        if _DBT_MANIFEST is None:
            print("    [inprocess] parsing project once (manifest will be reused)...")
            res = dbtRunner().invoke(["parse"])
            if not res.success or res.result is None:
                raise RuntimeError(f"initial dbt parse failed: {res.exception}")
            _DBT_MANIFEST = res.result
        res = dbtRunner(manifest=_DBT_MANIFEST).invoke(args)
    finally:
        os.chdir(prev_cwd)

    # Synthesize a CompletedProcess so the caller's success/transient-error
    # handling works identically for both engines.
    output = ""
    try:
        for node_res in getattr(res.result, "results", []) or []:
            if getattr(node_res, "message", None):
                output += str(node_res.message) + "\n"
    except Exception:
        pass
    if not res.success:
        if res.exception and not output:
            output = str(res.exception)
        raise subprocess.CalledProcessError(1, ["dbt"] + args, output=output, stderr=output)
    return subprocess.CompletedProcess(["dbt"] + args, 0, stdout=output, stderr="")


def run_dbt_command(args: List[str], capture: bool = False) -> subprocess.CompletedProcess:
    """Execute a dbt command from project root."""
    # In-process engine handles `run` only: its synthesized stdout carries node
    # result messages, which is what the run-batch error handling needs. `ls`
    # (model selection, parsed from stdout) and other commands stay subprocess —
    # they happen once per invocation, so the re-parse cost is negligible.
    if os.environ.get("REFRESH_INPROCESS") == "1" and args and args[0] == "run":
        return _run_dbt_inprocess(args, capture)
    cmd = ["dbt"] + args
    return subprocess.run(
        cmd,
        cwd=PROJECT_ROOT,
        capture_output=capture,
        text=True,
        check=True
    )


def strip_ansi(text: str) -> str:
    """Remove ANSI escape codes from text."""
    import re
    # Match all ANSI escape sequences
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    return ansi_escape.sub('', text)


def is_valid_model_name(name: str) -> bool:
    """
    Check if a string looks like a valid dbt model name.
    Filters out dbt status messages like timestamps and "Running with dbt=..."
    """
    import re
    
    # Strip ANSI codes first
    name = strip_ansi(name).strip()
    
    if not name:
        return False
    
    # Filter out common dbt status message patterns
    skip_patterns = [
        r'^\d{2}:\d{2}:\d{2}',          # Timestamp at start (HH:MM:SS)
        r'^Running with dbt',            # dbt version message
        r'^Registered adapter',          # Adapter registration
        r'^Found \d+ models',            # Model count message
        r'^Concurrency:',                # Concurrency info
        r'^Done\.',                      # Done message
        r'^\s*$',                        # Empty or whitespace
    ]
    
    for pattern in skip_patterns:
        if re.match(pattern, name, re.IGNORECASE):
            return False
    
    # Valid model names should be alphanumeric with underscores, starting with letter
    # and not contain spaces (dbt model naming convention)
    if ' ' in name or not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', name):
        return False
    
    return True


def get_models_in_order(selector: str, exclude: str | None = None) -> List[str]:
    """
    Get models matching selector in dependency order.
    Uses dbt ls to get matching models, then sorts by dependencies from manifest.

    If `exclude` is provided it is forwarded to dbt ls as `--exclude <exclude>`
    — same semantics as `dbt run --exclude`. Useful for "downstream-only"
    refreshes that pair a `model+` selector with the model name itself
    excluded.
    """
    import re

    ls_args = ["ls", "--select", selector, "--resource-type", "model", "--output", "name", "--quiet"]
    if exclude:
        ls_args.extend(["--exclude", exclude])

    result = run_dbt_command(
        ls_args,
        capture=True
    )
    
    # Parse output, filtering out dbt status messages
    raw_lines = result.stdout.strip().split("\n")
    models = []
    
    for line in raw_lines:
        # First strip any ANSI escape codes
        cleaned = re.sub(r'\x1b\[[0-9;]*m', '', line).strip()
        
        # Skip empty lines
        if not cleaned:
            continue
            
        # Skip lines that look like dbt log output
        if any(pattern in cleaned for pattern in [
            'Running with dbt',
            'Registered adapter',
            'Found ',
            ' models,',
            ' seeds,',
            ' tests,',
            ' sources,',
            ' macros',
            'Concurrency:',
            'Done.',
        ]):
            continue
        
        # Skip lines starting with timestamps (HH:MM:SS)
        if re.match(r'^\d{2}:\d{2}:\d{2}', cleaned):
            continue
        
        # Valid model names are alphanumeric with underscores, starting with letter
        if re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', cleaned):
            models.append(cleaned)
    
    # Sort models by dependency order using manifest
    return topological_sort_models(models)


def topological_sort_models(models: List[str]) -> List[str]:
    """
    Sort models in dependency order using manifest.json.
    Models with no dependencies come first, then their dependents.
    """
    ensure_manifest()
    
    with open(MANIFEST_PATH) as f:
        manifest = json.load(f)
    
    # Find project name from manifest
    project_name = None
    for node_key in manifest.get("nodes", {}).keys():
        if node_key.startswith("model."):
            project_name = node_key.split(".")[1]
            break
    
    if not project_name:
        print("WARNING: Could not determine project name, falling back to alphabetical order")
        return sorted(models)
    
    # Build dependency graph for selected models only
    model_set = set(models)
    dependencies: Dict[str, List[str]] = {m: [] for m in models}
    
    for model in models:
        node_key = f"model.{project_name}.{model}"
        if node_key in manifest.get("nodes", {}):
            node = manifest["nodes"][node_key]
            # Get dependencies that are also in our selected models
            for dep in node.get("depends_on", {}).get("nodes", []):
                # Extract model name from node key (e.g., "model.project.int_xxx" -> "int_xxx")
                if dep.startswith("model."):
                    dep_name = dep.split(".")[-1]
                    if dep_name in model_set:
                        dependencies[model].append(dep_name)
    
    # Kahn's algorithm for topological sort
    # Count incoming edges
    in_degree = {m: 0 for m in models}
    for model, deps in dependencies.items():
        for dep in deps:
            in_degree[model] += 1  # model depends on dep, so model has an incoming edge
    
    # Start with models that have no dependencies (in_degree = 0)
    queue = [m for m in models if in_degree[m] == 0]
    queue.sort()  # Alphabetical for determinism when no dependency constraint
    
    result = []
    while queue:
        # Pop model with no remaining dependencies
        model = queue.pop(0)
        result.append(model)
        
        # Remove this model as a dependency from others
        for other_model, deps in dependencies.items():
            if model in deps:
                in_degree[other_model] -= 1
                if in_degree[other_model] == 0:
                    # Insert in sorted position for determinism
                    queue.append(other_model)
                    queue.sort()
    
    # Check for cycles (shouldn't happen in dbt, but just in case)
    if len(result) != len(models):
        print("WARNING: Dependency cycle detected, falling back to alphabetical order")
        return sorted(models)
    
    return result


def ensure_manifest() -> None:
    """Ensure manifest.json exists by running dbt compile if needed."""
    if not MANIFEST_PATH.exists():
        print("Manifest not found, running dbt compile...")
        run_dbt_command(["compile"])


_MANIFEST_CACHE: Optional[dict] = None


def _load_manifest() -> dict:
    """Load and cache manifest.json.

    Opened ONCE per refresh.py invocation. Caching is important: with 160+
    selected models, we hit get_model_meta() once per model — and any
    concurrent process touching target/manifest.json (e.g. a long-running
    `dbt docs serve` rebuilding the manifest) can leave the file partially
    written for the duration of one fsync, producing JSONDecodeError mid-
    iteration. Reading once and caching the parsed dict avoids the race.
    """
    global _MANIFEST_CACHE
    if _MANIFEST_CACHE is None:
        ensure_manifest()
        with open(MANIFEST_PATH) as f:
            _MANIFEST_CACHE = json.load(f)
    return _MANIFEST_CACHE


def get_model_meta(model_name: str) -> Optional[dict]:
    """
    Extract meta.full_refresh configuration from manifest.json.
    Returns None if model has no full_refresh config.
    """
    manifest = _load_manifest()

    # Find the model in manifest
    for node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") == "model" and node.get("name") == model_name:
            meta = node.get("meta", {})
            return meta.get("full_refresh")

    return None


# ============================================================================
# Batch Generation
# ============================================================================

def generate_time_batches(
    start_date: str,
    end_date: Optional[str] = None,
    batch_months: int = 1
) -> List[Tuple[str, str]]:
    """
    Generate (start_month, end_month) tuples for batching.
    
    Args:
        start_date: First day of first month (YYYY-MM-DD)
        end_date: Last date to include (defaults to today)
        batch_months: Number of months per batch
    
    Returns:
        List of (start_month, end_month) tuples in YYYY-MM-01 format
    """
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else date.today()
    
    # Normalize to first of month
    current = start.replace(day=1)
    end_month = end.replace(day=1)
    
    batches = []
    while current <= end_month:
        # Calculate batch end
        batch_end_month = current.month + batch_months - 1
        batch_end_year = current.year + (batch_end_month - 1) // 12
        batch_end_month = ((batch_end_month - 1) % 12) + 1
        batch_end = date(batch_end_year, batch_end_month, 1)
        
        # Don't exceed end date
        if batch_end > end_month:
            batch_end = end_month
        
        batches.append((
            current.strftime("%Y-%m-%d"),
            batch_end.strftime("%Y-%m-%d")
        ))
        
        # Move to next batch
        next_month = batch_end.month + 1
        next_year = batch_end.year + (next_month - 1) // 12
        next_month = ((next_month - 1) % 12) + 1
        current = date(next_year, next_month, 1)
    
    return batches


# ============================================================================
# Model Execution
# ============================================================================

def run_model_batched(
    model: str,
    config: dict,
    full_refresh_first_batch: bool,
    dry_run: bool = False,
    state: Optional[dict] = None,
    incremental_only: bool = False,
    stage_filter: Optional[List[str]] = None,
    delay: int = 0,
    defer_args: Optional[List[str]] = None
) -> bool:
    """
    Run a model with batched execution based on its config.
    
    Each stage can have its own start_date and batch_months.
    
    Args:
        model: The dbt model name
        config: The full_refresh config from schema.yml
        full_refresh_first_batch: If True, add --full-refresh to first batch (ignored if incremental_only)
        dry_run: If True, only print what would be executed
        state: Resume state dict
        incremental_only: If True, never use --full-refresh (append only)
        stage_filter: If set, only run stages with names in this list
        defer_args: Extra dbt flags for deferral (e.g. ["--defer", "--favor-state", "--state", "path"])
    
    Returns True if successful, False otherwise.
    """
    default_start_date = config.get("start_date")
    default_batch_months = config.get("batch_months", 1)
    stages = config.get("stages", [{"name": "default", "vars": {}}])
    
    # Filter stages if --stage argument provided
    if stage_filter:
        original_count = len(stages)
        stages = [s for s in stages if s.get("name") in stage_filter]
        if not stages:
            print(f"ERROR: No stages matched filter: {stage_filter}")
            print(f"  Available stages: {[s.get('name') for s in config.get('stages', [])]}")
            return False
        print(f"  Stage filter: {stage_filter} ({len(stages)}/{original_count} stages)")
    
    # Calculate total runs across all stages (each stage may have different batch counts)
    total_runs = 0
    stage_batches = []
    stage_batch_months = []
    for stage in stages:
        stage_start = stage.get("start_date", default_start_date)
        stage_months = stage.get("batch_months", default_batch_months)
        if not stage_start:
            print(f"ERROR: No start_date for stage '{stage.get('name', 'default')}' and no model-level start_date")
            return False
        batches = generate_time_batches(stage_start, batch_months=stage_months)
        stage_batches.append(batches)
        stage_batch_months.append(stage_months)
        total_runs += len(batches)
    
    mode = "INCREMENTAL (append only)" if incremental_only else "FULL REFRESH"
    print(f"\n{'='*60}")
    print(f"Model: {model}")
    print(f"  Mode: {mode}")
    print(f"  Stages: {len(stages)}")
    print(f"  Total runs: {total_runs}")
    print(f"{'='*60}")
    
    run_number = 0
    is_first_run = full_refresh_first_batch
    
    for stage_idx, stage in enumerate(stages):
        stage_name = stage["name"]
        stage_vars = stage.get("vars", {})
        stage_start = stage.get("start_date", default_start_date)
        stage_months = stage_batch_months[stage_idx]
        batches = stage_batches[stage_idx]
        
        batch_info = f"{stage_months}mo batches" if stage_months > 1 else "monthly"
        print(f"\n  Stage: {stage_name} ({len(batches)} batches, {stage_start} → now, {batch_info})")
        
        for i, (batch_start, batch_end) in enumerate(batches):
            run_number += 1
            
            # Skip if resuming and already completed
            if state and state.get("current_model") == model:
                if run_number <= state.get("current_batch", 0):
                    print(f"    [{run_number}/{total_runs}] {batch_start} → {batch_end} | SKIPPED (resume)")
                    is_first_run = False  # Don't full-refresh after resume
                    continue
            
            # Build vars
            all_vars = {
                "start_month": batch_start,
                "end_month": batch_end,
                **stage_vars
            }
            vars_json = json.dumps(all_vars)
            
            # Build command
            cmd = ["run", "-s", model, "--vars", vars_json]
            if is_first_run and not incremental_only:
                cmd.append("--full-refresh")
                is_first_run = False
            if defer_args:
                cmd.extend(defer_args)
            
            # Format output
            vars_display = {k: v for k, v in stage_vars.items()}
            refresh_flag = " --full-refresh" if "--full-refresh" in cmd else ""
            print(f"    [{run_number}/{total_runs}] {batch_start} → {batch_end} | {vars_display}{refresh_flag}")
            
            if not dry_run:
                # Retry on transient ClickHouse errors. The cluster's
                # OvercommitTracker can pick *our* query as the memory victim
                # whenever any tenant pushes shared usage past the ceiling,
                # even when our own query is bounded by max_memory_usage.
                # When that happens, the failure is independent of our batch
                # — wait briefly and retry.
                # Network/timeout errors are genuinely transient — full retry.
                NETWORK_TRANSIENT = (
                    "Code: 159",          # SOCKET_TIMEOUT
                    "Code: 209",          # NETWORK_ERROR
                    "Code: 210",          # NETWORK_ERROR
                    "TIMEOUT_EXCEEDED",
                    "SOCKET_TIMEOUT",
                    "NETWORK_ERROR",
                    "SSLError",
                    "UNEXPECTED_EOF_WHILE_READING",
                    "HTTPSConnectionPool",
                    "RemoteDisconnected",
                    "ConnectionResetError",
                    "Broken pipe",
                )
                # A memory error (Code 241 / MEMORY_LIMIT_EXCEEDED) is DETERMINISTIC
                # for our own query — retrying the identical batch re-OOMs and only
                # wastes the backoff window. The ONE exception is when the cluster
                # OvercommitTracker picked us as a cross-tenant victim (shared
                # ceiling), which is independent of our batch — retry that once.
                MAX_NETWORK_RETRIES = 5
                MAX_MEMORY_RETRIES = 3
                BACKOFF_SECONDS = (30, 60, 120, 240, 480)
                for attempt in range(MAX_NETWORK_RETRIES + 1):
                    try:
                        # capture=True so we can inspect e.stdout / e.stderr to
                        # classify the failure. We tee the captured output back
                        # to stdout on every attempt to preserve live progress
                        # visibility.
                        proc = run_dbt_command(cmd, capture=True)
                        if proc.stdout:
                            print(proc.stdout, end="")
                        if proc.stderr:
                            print(proc.stderr, end="")
                        if state is not None:
                            state["current_model"] = model
                            state["current_batch"] = run_number
                            save_state(state)
                        if delay > 0 and run_number < total_runs:
                            print(f"    Waiting {delay}s for background merges...")
                            time.sleep(delay)
                        break  # success
                    except subprocess.CalledProcessError as e:
                        # With capture=True, the failed run's output is on e.
                        if e.stdout:
                            print(e.stdout, end="")
                        if e.stderr:
                            print(e.stderr, end="")
                        err_blob = (e.stdout or "") + (e.stderr or "")
                        is_network = any(p in err_blob for p in NETWORK_TRANSIENT)
                        is_memory = ("Code: 241" in err_blob) or ("MEMORY_LIMIT_EXCEEDED" in err_blob)
                        is_overcommit_victim = is_memory and ("OvercommitTracker" in err_blob)
                        # Network → full retries; OvercommitTracker victim → 1 retry;
                        # a bare OOM (our query too big) → 0 retries (fail fast).
                        retry_cap = (
                            MAX_NETWORK_RETRIES if is_network
                            else MAX_MEMORY_RETRIES if is_overcommit_victim
                            else 0
                        )
                        if attempt < retry_cap:
                            wait = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
                            kind = "network" if is_network else "overcommit-victim"
                            print(
                                f"\n    [transient:{kind}] dbt run hit a retryable "
                                f"ClickHouse error; retry {attempt + 1}/{retry_cap} "
                                f"after {wait}s"
                            )
                            time.sleep(wait)
                            continue
                        if is_memory and not is_overcommit_victim:
                            print(f"\n    [permanent] memory limit exceeded (not an "
                                  f"OvercommitTracker victim) — not retrying; needs a "
                                  f"bounded build / memory hooks.")
                        print(f"\n    ERROR: dbt run failed!")
                        print(f"    Command: dbt {' '.join(cmd)}")
                        return False
    
    return True


def run_model_normal(
    model: str,
    full_refresh: bool,
    dry_run: bool = False,
    defer_args: Optional[List[str]] = None
) -> bool:
    """Run a model normally (no batching)."""
    cmd = ["run", "-s", model]
    if full_refresh:
        cmd.append("--full-refresh")
    if defer_args:
        cmd.extend(defer_args)
    
    refresh_flag = " --full-refresh" if full_refresh else ""
    print(f"\n  {model} (standard run){refresh_flag}")
    
    if not dry_run:
        try:
            run_dbt_command(cmd)
        except subprocess.CalledProcessError:
            print(f"    ERROR: dbt run failed for {model}")
            return False
    
    return True


# ============================================================================
# Main Orchestration
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="dbt Full Refresh Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --select tag:production
  %(prog)s --select int_execution_tokens_balances_daily --dry-run
  %(prog)s --select tag:tokens --resume
  
  # Add a new token without full refresh:
  %(prog)s --select int_execution_tokens_balances_daily --stage new_token --incremental-only
  
  # Dev workflow - batch refresh, defer upstream refs to prod:
  %(prog)s --select model_name --defer --state ./prod-state/ --incremental-only
        """
    )
    parser.add_argument(
        "--select", "-s",
        nargs='+',
        required=True,
        help="dbt selector(s) - model names, tag:xxx, etc. (space-separated)"
    )
    parser.add_argument(
        "--exclude", "-e",
        nargs='+',
        default=None,
        help="dbt exclusion selector(s) — passed through to dbt ls as "
             "--exclude. Use to refresh downstream-only by pairing "
             "`--select model+` with `--exclude model`."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show execution plan without running"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last saved state"
    )
    parser.add_argument(
        "--inprocess",
        action="store_true",
        help="Run batches via in-process dbtRunner with a single shared parse "
             "(skips the ~14s per-batch dbt startup/re-parse). Batch vars must "
             "be execution-time only (model bodies), not parse-time."
    )
    parser.add_argument(
        "--incremental-only",
        action="store_true",
        help="Skip --full-refresh flag (append data only, don't destroy existing)"
    )
    parser.add_argument(
        "--stage",
        type=str,
        help="Only run specific stage(s), comma-separated (e.g., --stage usdc,sdai)"
    )
    parser.add_argument(
        "--delay",
        type=int,
        default=0,
        help="Seconds to wait between batches, letting background merges drain (e.g., --delay 30)"
    )
    parser.add_argument(
        "--defer",
        action="store_true",
        help="Defer unselected upstream refs to a production manifest (requires --state)"
    )
    parser.add_argument(
        "--favor-state",
        action="store_true",
        help="Prefer production state for upstream refs even if local versions exist"
    )
    parser.add_argument(
        "--state",
        type=str,
        default=None,
        help="Path to directory containing prod manifest.json for deferral (e.g., ./prod-state/)"
    )
    
    args = parser.parse_args()

    if args.inprocess:
        os.environ["REFRESH_INPROCESS"] = "1"
    
    # Build defer flags to forward to every dbt run command
    defer_args: List[str] = []
    if args.defer:
        defer_args.append("--defer")
    if args.favor_state:
        defer_args.append("--favor-state")
    if args.state:
        defer_args.extend(["--state", args.state])
    
    # Load or initialize state
    state = load_state() if args.resume else {"completed_models": [], "current_model": None, "current_batch": 0}
    
    # Join multiple selectors into space-separated string for dbt
    selector = " ".join(args.select)
    exclude = " ".join(args.exclude) if args.exclude else None

    print(f"Getting models for: {selector}" + (f"  (exclude: {exclude})" if exclude else ""))
    try:
        models = get_models_in_order(selector, exclude=exclude)
    except subprocess.CalledProcessError:
        print("ERROR: Failed to get model list. Is dbt configured correctly?")
        sys.exit(1)
    
    if not models:
        print(f"No models found for selector: {selector}")
        sys.exit(1)
    
    print(f"Found {len(models)} model(s): {', '.join(models)}")
    
    if args.dry_run:
        print("\n" + "="*60)
        print("DRY RUN - Execution Plan")
        print("="*60)
    
    success_count = 0
    
    for model in models:
        # Skip completed models when resuming
        if args.resume and model in state.get("completed_models", []):
            print(f"\n  {model} (SKIPPED - already completed)")
            continue
        
        # Get model's full_refresh config
        config = get_model_meta(model)
        
        # Parse stage filter if provided
        stage_filter = args.stage.split(",") if args.stage else None
        
        if config:
            # Each model's first batch needs --full-refresh (unless incremental_only)
            success = run_model_batched(
                model, config, True, args.dry_run, state,
                incremental_only=args.incremental_only,
                stage_filter=stage_filter,
                delay=args.delay,
                defer_args=defer_args
            )
        else:
            success = run_model_normal(model, True, args.dry_run, defer_args=defer_args)
        
        if not success and not args.dry_run:
            print(f"\nFailed at model: {model}")
            resume_cmd = f"python {sys.argv[0]} --select {selector}"
            if exclude:
                resume_cmd += f" --exclude {exclude}"
            resume_cmd += " --resume"
            print(f"State saved. Resume with: {resume_cmd}")
            sys.exit(1)
        
        # Mark model complete
        if not args.dry_run:
            state["completed_models"].append(model)
            state["current_model"] = None
            state["current_batch"] = 0
            save_state(state)
        
        success_count += 1
    
    # Clean up on success
    if not args.dry_run:
        clear_state()
        print(f"\n{'='*60}")
        print(f"SUCCESS: Completed {success_count} models")
        print(f"{'='*60}")
    else:
        print(f"\n{'='*60}")
        print(f"DRY RUN COMPLETE: Would process {success_count} models")
        print(f"{'='*60}")


if __name__ == "__main__":
    main()