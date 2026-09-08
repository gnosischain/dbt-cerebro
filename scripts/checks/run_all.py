#!/usr/bin/env python3
"""THE verification command — one vendor-neutral entry point for every policy
gate, from a fresh checkout, with no `make` required (works in the dbt
container and on the host).

Modes:
  python scripts/checks/run_all.py            parse mode (default): everything
                                              that needs only target/manifest.json
  python scripts/checks/run_all.py --fast     static manifest gates only
  python scripts/checks/run_all.py --full     + semantic registry/graph/entities
                                              (needs target/catalog.json — i.e. a
                                              prior warehouse-connected
                                              `dbt docs generate`)
  --ci             CI strictness: base ref must resolve (--require-base), a
                   stale manifest is an error not a warning
  --docs-generate  run `dbt docs generate --exclude tag:dev` before the
                   semantic steps (CI full mode; needs warehouse credentials)
  --base-ref REF   base for the change-aware contract gate (default: main)

Tiers (why --full exists): `dbt parse` needs no warehouse, so every static gate
can run on any checkout/PR. The semantic registry, graph gate and entity overlay
read target/catalog.json; because dbt-clickhouse emits NO model nodes into the
catalog, those gates are manifest-only in practice — verified 2026-09-08: real vs
stub catalog gives identical gate results (the catalog only adds source column
types to the registry artifact). So `--full` is the IMAGE GATE and needs no
warehouse: when target/catalog.json is missing it writes an empty stub first.
`--full --docs-generate` is the WAREHOUSE tier (real catalog, docs site); it runs
in the deploy-docs job AFTER the image is published and must never gate the image
(a saturated ClickHouse blocked an image publish on 2026-09-08).

Bootstrap: if target/manifest.json is missing (or older than the newest model/
seed/dbt_project.yml file), runs `dbt parse` when dbt is installed; otherwise
fails with the exact docker command to run.

Exit code: 0 = every step passed; 1 = at least one failed (all steps run —
no fail-fast — and a PASS/FAIL summary prints at the end).
"""
from __future__ import annotations

import argparse
import filecmp
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TARGET = REPO_ROOT / "target"
MANIFEST = TARGET / "manifest.json"
CATALOG = TARGET / "catalog.json"

DOCKER_PARSE = 'docker exec dbt bash -lc "cd /app && dbt parse --no-partial-parse"'


def newest_input_mtime() -> float:
    newest = 0.0
    candidates = [REPO_ROOT / "dbt_project.yml"]
    for pattern in ("models/**/*.sql", "models/**/*.yml", "seeds/*.csv"):
        candidates.extend(REPO_ROOT.glob(pattern))
    for p in candidates:
        try:
            newest = max(newest, p.stat().st_mtime)
        except OSError:
            continue
    return newest


def bootstrap_manifest(ci: bool) -> bool:
    """Ensure a usable manifest. Returns False only on unrecoverable failure."""
    missing = not MANIFEST.exists()
    stale = not missing and MANIFEST.stat().st_mtime < newest_input_mtime()
    if not missing and not stale:
        return True

    reason = "missing" if missing else "older than the newest model/seed file"
    if shutil.which("dbt"):
        print(f"[bootstrap] manifest {reason} — running dbt parse...")
        rc = subprocess.call(["dbt", "parse"], cwd=REPO_ROOT)
        if rc != 0:
            print("[bootstrap] dbt parse FAILED")
            return False
        return True
    if missing:
        print(f"[bootstrap] manifest missing and dbt is not installed here.\n"
              f"  Run: {DOCKER_PARSE}")
        return False
    # Stale but no dbt available: CI must fail (validation against a stale
    # manifest silently skips new work); locally warn loudly and continue.
    if ci:
        print(f"[bootstrap] manifest is {reason} and dbt is unavailable — "
              f"refusing under --ci. Run: {DOCKER_PARSE}")
        return False
    print(f"[bootstrap] WARNING: manifest is {reason}; gates run against the "
          f"STALE manifest. Refresh with: {DOCKER_PARSE}")
    return True


class Runner:
    def __init__(self) -> None:
        self.results: list[tuple[str, str, float]] = []

    def run(self, name: str, argv: list, env_extra: dict = None) -> bool:
        print(f"\n=== {name} ===", flush=True)
        env = dict(os.environ)
        if env_extra:
            env.update(env_extra)
        start = time.time()
        rc = subprocess.call([str(a) for a in argv], cwd=REPO_ROOT, env=env)
        ok = rc == 0
        self.results.append((name, "PASS" if ok else "FAIL", time.time() - start))
        return ok

    def record(self, name: str, ok: bool, elapsed: float = 0.0) -> None:
        self.results.append((name, "PASS" if ok else "FAIL", elapsed))

    def run_retrying_overcommit(self, name: str, argv: list, attempts: int = 5,
                                base_sleep: int = 60) -> bool:
        """Like run(), but retry when the step died as a ClickHouse
        OvercommitTracker VICTIM (Code 241 with the `(total)` server-wide limit).

        `dbt docs generate` compiles every decode model, and each one runs its
        watermark query at compile time; on a saturated warehouse any of those
        allocations can be the one the server kills. That is the cluster's
        problem, not the change's (docs/lessons/ch-overcommit-victim.md), yet it
        blocked an image publish on 2026-09-08. Only the `(total)` signature is
        retried; a per-query memory limit or any other error fails immediately.
        """
        print(f"\n=== {name} ===", flush=True)
        start = time.time()
        for attempt in range(1, attempts + 1):
            rc = subprocess.call([str(a) for a in argv], cwd=REPO_ROOT, env=dict(os.environ))
            if rc == 0:
                self.results.append((name, "PASS", time.time() - start))
                return True
            if attempt == attempts or not _dbt_log_shows_overcommit_victim():
                break
            wait = base_sleep * attempt
            print(f"[retry] {name}: ClickHouse Code 241 `(total)` -- the server is "
                  f"saturated and picked this query as the victim; attempt "
                  f"{attempt}/{attempts} failed, sleeping {wait}s before retrying.",
                  flush=True)
            time.sleep(wait)
        self.results.append((name, "FAIL", time.time() - start))
        return False


    def summary(self) -> int:
        print("\n" + "=" * 62)
        print(f"{'step':<40} {'result':<6} {'secs':>6}")
        print("-" * 62)
        failed = 0
        for name, result, secs in self.results:
            print(f"{name:<40} {result:<6} {secs:>6.1f}")
            if result == "FAIL":
                failed += 1
        print("-" * 62)
        skipped = [name for name, result, _ in self.results if result == "SKIP"]
        if failed:
            print(f"{failed} step(s) FAILED")
            return 1
        if skipped:
            print("all steps passed (image-gate tier, manifest-only; "
                  f"{', '.join(skipped)} belongs to the warehouse tier that runs "
                  "in deploy-docs after the image)")
            return 0
        print("all steps passed")
        return 0


def write_stub_catalog(path: Path) -> None:
    """Write an empty dbt catalog so the semantic gates can run without a warehouse.

    dbt-clickhouse's `docs generate` emits sources only (0 model nodes), so the
    registry/graph/entity gates never see model columns from the catalog; an empty
    one yields the same gate verdicts. The real catalog is still produced by the
    warehouse tier (deploy-docs) for the published artifacts.
    """
    import json
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/catalog/v1.json",
            "generated_at": "1970-01-01T00:00:00Z",
            "invocation_id": None,
            "env": {},
            "stub": "written by scripts/checks/run_all.py (no warehouse)",
        },
        "nodes": {},
        "sources": {},
        "errors": None,
    }, indent=2), encoding="utf-8")


def _dbt_log_shows_overcommit_victim(tail_lines: int = 400) -> bool:
    """True when the newest dbt log lines carry the server-wide 241 signature."""
    log_dir = Path(os.environ.get("DBT_LOG_PATH") or (REPO_ROOT / "logs"))
    log = log_dir / "dbt.log"
    try:
        lines = log.read_text(encoding="utf-8", errors="replace").splitlines()[-tail_lines:]
    except OSError:
        return False
    tail = "\n".join(lines)
    return "Code: 241" in tail and "(total) memory limit exceeded" in tail


def determinism_check(runner: Runner) -> None:
    """Rebuild the artifact into a temp target and byte-compare — a fresh
    checkout has no prior artifact, so --check alone can't prove determinism."""
    name = "agent-context-determinism"
    start = time.time()
    with tempfile.TemporaryDirectory() as tmp:
        tmp_target = Path(tmp)
        shutil.copy(MANIFEST, tmp_target / "manifest.json")
        rc = subprocess.call(
            [sys.executable, "scripts/agent_context/build_agent_context.py",
             "--target-dir", str(tmp_target)],
            cwd=REPO_ROOT, stdout=subprocess.DEVNULL,
        )
        ok = rc == 0 and filecmp.cmp(
            TARGET / "agent_context.json", tmp_target / "agent_context.json",
            shallow=False,
        )
    if not ok:
        print(f"=== {name} ===\nFAIL: rebuild differs from target/agent_context.json")
    runner.record(name, ok, time.time() - start)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--fast", action="store_true", help="static manifest gates only")
    ap.add_argument("--full", action="store_true",
                    help="add the catalog-dependent semantic steps")
    ap.add_argument("--ci", action="store_true", help="CI strictness (fail closed)")
    ap.add_argument("--docs-generate", action="store_true",
                    help="run dbt docs generate before the semantic steps (needs warehouse)")
    ap.add_argument("--base-ref", default="main")
    args = ap.parse_args()

    r = Runner()
    py = sys.executable

    if not bootstrap_manifest(args.ci):
        r.record("bootstrap-manifest", False)
        return r.summary()

    # -- static manifest gates (every mode) --------------------------------
    r.run("no-delete-insert", [py, "scripts/checks/no_delete_insert.py"])
    r.run("api-tags", [py, "scripts/checks/check_api_tags.py"])
    r.run("doc-coverage", [py, "scripts/checks/check_doc_coverage.py"])
    r.run("meta-keys", [py, "scripts/checks/check_meta_keys.py"])
    r.run("envio-ga-policy", [py, "scripts/checks/envio_ga_policy.py"])
    r.run("scaffold-gate", [py, "scripts/semantic/scaffold_candidates.py", "--gate"])

    if not args.fast:
        # -- agent context: build FIRST, then prove determinism ------------
        built = r.run("agent-context-build",
                      [py, "scripts/agent_context/build_agent_context.py"])
        if built:
            determinism_check(r)
        check_cmd = [py, "scripts/agent_context/check.py",
                     "--base-ref", args.base_ref, "--skip-static"]
        if args.ci:
            check_cmd.append("--require-base")
        r.run("agent-context-check", check_cmd)

        # -- pytest (plugin autoload off: third-party plugins like web3's
        # break collection in the container; see the audit) -----------------
        r.run("pytest",
              [py, "-m", "pytest",
               "tests/test_policy_gates.py", "tests/test_run_state.py",
               "tests/test_mixpanel_privacy.py", "tests/test_semantic_registry.py",
               "-q"],
              env_extra={"PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1"})

    if args.full:
        if args.docs_generate:
            r.run_retrying_overcommit("dbt-docs-generate",
                                      ["dbt", "docs", "generate", "--exclude", "tag:dev"])
        else:
            r.results.append(("dbt-docs-generate", "SKIP", 0.0))
            if not CATALOG.exists():
                write_stub_catalog(CATALOG)
                print("\n=== catalog-stub ===\ntarget/catalog.json missing -> wrote an "
                      "empty stub; the semantic gates run manifest-only (identical "
                      "results to a real catalog, which carries no model nodes here).")
                r.record("catalog-stub", True)
        if not CATALOG.exists():
            print("\n=== semantic steps ===\nFAIL: target/catalog.json missing and "
                  "could not be stubbed.")
            r.record("semantic-registry", False)
        else:
            r.run("semantic-registry",
                  [py, "scripts/semantic/build_registry.py", "--target-dir", "target",
                   "--validate", "--max-warnings", "0"])
            r.run("graph-gate",
                  [py, "scripts/semantic/graph_gate.py", "--target-dir", "target"])
            r.run("entity-overlay",
                  [py, "scripts/semantic/generate_entities.py", "--target-dir",
                   "target", "--check"])

    return r.summary()


if __name__ == "__main__":
    raise SystemExit(main())
