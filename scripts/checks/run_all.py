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

Two-tier design (why --full exists): `dbt parse` needs no warehouse, so every
static gate can run on any checkout/PR. The semantic registry, graph gate and
entity overlay hard-require target/catalog.json, which only a credentialed
`dbt docs generate` produces — those steps are the full tier, run on main
before the Docker image publishes.

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
        if failed:
            print(f"{failed} step(s) FAILED")
            return 1
        print("all steps passed")
        return 0


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
            r.run("dbt-docs-generate",
                  ["dbt", "docs", "generate", "--exclude", "tag:dev"])
        if not CATALOG.exists():
            print("\n=== semantic steps ===\nFAIL: target/catalog.json missing — "
                  "the semantic registry/graph/entity gates need a warehouse-"
                  "connected `dbt docs generate` (or pass --docs-generate with "
                  "credentials).")
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
