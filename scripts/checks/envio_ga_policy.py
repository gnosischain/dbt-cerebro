#!/usr/bin/env python3
"""
CI guard for the envio_ga (gnosis_app_gt) build policy. Complements
no_delete_insert.py (which already bans delete+insert and requires partition_by
on every insert_overwrite model project-wide).

File-based (no manifest needed). For every model whose SQL references the
envio_ga source, enforce:

  1. materialized='incremental'  =>  must set partition_by  (heavy time-grained
     spines only; a whole-table REPLACE without a partition wipes data).
  2. reads a STRETCH table (transaction / transfer / transaction_action) =>
     must carry the 'stretch' tag (these are 36M/108M/208M rows, cost-gated:
     built via the microbatch/batch runner, never a plain full dbt run).
  3. materialized='view'/'table' reading envio_ga is fine (the small entity
     snapshots are cheap full rebuilds).

Violations fail the build unless listed in envio_ga_policy.allow
(one path-or-model-name per line; '#' comments).

Usage: python scripts/checks/envio_ga_policy.py
"""
import os, re, sys, glob

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ALLOW = os.path.join(os.path.dirname(os.path.abspath(__file__)), "envio_ga_policy.allow")
STRETCH = ("transaction_action", "transfer", "transaction")
ENVIO_RE = re.compile(r"source\(\s*['\"]envio_ga['\"]")
STRETCH_RE = re.compile(r"source\(\s*['\"]envio_ga['\"]\s*,\s*['\"](transaction_action|transfer|transaction)['\"]")


def load_allow():
    if not os.path.exists(ALLOW):
        return set()
    return {l.split("#", 1)[0].strip() for l in open(ALLOW) if l.split("#", 1)[0].strip()}


def config_block(sql):
    m = re.search(r"config\((.*?)\)\s*}}", sql, re.DOTALL)
    return m.group(1) if m else ""


def main():
    allow = load_allow()
    violations = []
    for path in glob.glob(os.path.join(REPO, "models", "**", "*.sql"), recursive=True):
        sql = open(path).read()
        if not ENVIO_RE.search(sql):
            continue
        name = os.path.basename(path)[:-4]
        cfg = config_block(sql)
        materialized = (re.search(r"materialized\s*=\s*['\"](\w+)['\"]", cfg) or [None, ""])[1]

        def fail(rule, msg):
            if name not in allow and os.path.relpath(path, REPO) not in allow:
                violations.append(f"{name}  [{rule}]  {msg}")

        if materialized == "incremental" and "partition_by" not in cfg:
            fail("incremental_needs_partition", "incremental model over envio_ga must set partition_by")
        if STRETCH_RE.search(sql) and "stretch" not in cfg:
            fail("stretch_needs_tag", "reads a stretch table (transaction/transfer/transaction_action) — add the 'stretch' tag and build via the microbatch/batch runner")

    if violations:
        print("envio_ga policy violations:\n  " + "\n  ".join(violations))
        sys.exit(1)
    print("envio_ga policy: OK")


if __name__ == "__main__":
    main()
