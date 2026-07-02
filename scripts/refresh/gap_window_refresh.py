#!/usr/bin/env python3
"""
Gap-window refresh — backfill a SPECIFIC past month-window through an affected
incremental subgraph after a raw-source backfill (e.g. the May 30 + June 14 2026
`execution.logs` ingestion gap).

Unlike scripts/full_refresh/refresh.py (which rebuilds FULL history from each
model's configured start_date — thousands of batches), this targets ONLY the
given gap months, so a 14-minute source backfill costs ~1-2h, not days.

RESUMABLE: completed models are recorded in a JSON state file; re-run with the
same --state and it skips them and continues where it failed.

Per model (table marts + views are SKIPPED — `table` marts get truncated by
windowed batching, and the cron rebuilds them cleanly; views are live):

  - month/YYYYMM-partitioned models -> DROP the gap-month partition(s) (so the
    decode/aggregate watermark drops below the window), then run the model with
    `--vars {start_month,end_month}` when it supports that branch, else a plain
    run (insert_overwrite branch-1 recomputes the whole current window after the
    drop).
  - append / delete+insert models WITHOUT a start_month branch -> these cannot
    re-pull a past window incrementally, so `--full-refresh` (rebuild from
    scratch). Only safe for small tables; the script prints a row estimate and
    requires --allow-full-refresh-rows to exceed --full-refresh-row-cap.

Topo order: `dbt ls` + manifest depends_on, so upstreams are rebuilt before the
single-model downstream runs read them.
"""
from __future__ import annotations
import argparse, glob, json, os, re, subprocess, sys, datetime as dt
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]


def sh(cmd, env=None, capture=False):
    return subprocess.run(cmd, env=env, cwd=str(PROJECT_DIR),
                          capture_output=capture, text=True)


def ch_client():
    import clickhouse_connect
    host = os.environ["CLICKHOUSE_URL"].replace("https://", "").replace("http://", "").split(":")[0]
    return clickhouse_connect.get_client(
        host=host, port=int(os.environ.get("CLICKHOUSE_PORT", "8443")),
        username=os.environ["CLICKHOUSE_USER"], password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ.get("CLICKHOUSE_DATABASE", "dbt"),
        secure=os.environ.get("CLICKHOUSE_SECURE", "true").lower() == "true")


def find_sql(model: str):
    hits = glob.glob(str(PROJECT_DIR / f"models/**/{model}.sql"), recursive=True)
    return hits[0] if hits else None


def classify(model: str):
    """Return dict: materialized, strategy, partition_by, has_start_month, part_expr."""
    p = find_sql(model)
    if not p:
        return None
    t = open(p).read()
    mat = (re.search(r"materialized\s*=\s*'(\w+)'", t) or [None, "view"])[1]
    pbm = re.search(r"partition_by\s*=\s*'([^']*)'", t)
    pb = pbm.group(1) if pbm else None
    sm = re.search(r"incremental_strategy\s*=\s*(\([^)]*\)|'[^']*')", t)
    strat = sm.group(1) if sm else "(default)"
    # decode_logs / decode_calls macros read var('start_month')/var('end_month')
    # internally, so decode models support the windowed backfill even though the
    # string lives in the macro, not the model file.
    uses_decode_macro = ("decode_logs" in t) or ("decode_calls" in t)
    return {
        "materialized": mat,
        "partition_by": pb,
        "strategy": strat,
        "has_start_month": ("start_month" in t) or uses_decode_macro,
        "is_conditional": strat.startswith("("),
    }


def partition_ids(part_by: str, months: list[str]) -> list[str] | None:
    """Map gap months (YYYY-MM-01) to ClickHouse partition ids for this part expr.
    Returns None if the grain is unsafe to drop (e.g. yearly) or unknown."""
    if not part_by:
        return None
    pb = part_by.lower().strip()
    if "tostartofyear" in pb:           # dropping a year partition would nuke the year
        return None
    ids = []
    for m in months:
        y, mo, _ = m.split("-")
        if "tostartofmonth" in pb:
            ids.append(f"{y}-{mo}-01")
        elif "toyyyymm" in pb:
            ids.append(f"{y}{mo}")
        elif pb in ("month", "month_date"):   # bare monthly Date column
            ids.append(f"{y}-{mo}-01")
        else:
            return None                 # unknown grain -> don't guess (drop is best-effort)
    return ids


def get_models_topo(selector: str, exclude: str | None) -> list[str]:
    args = ["dbt", "ls", "--select", selector, "--resource-type", "model",
            "--output", "name", "--quiet", "--profiles-dir", str(PROJECT_DIR)]
    if exclude:
        args += ["--exclude", exclude]
    env = dict(os.environ, DBT_WRITE_JSON="False")
    res = sh(args, env=env, capture=True)
    names = [l.strip() for l in res.stdout.splitlines()
             if l.strip() and re.fullmatch(r"[a-zA-Z0-9_]+", l.strip())]
    # topo sort using the manifest depends_on (fallback: dbt ls order)
    man = PROJECT_DIR / "target" / "manifest.json"
    if not man.exists():
        return names
    try:
        nodes = json.load(open(man)).get("nodes", {})
        name_to_uid = {v["name"]: k for k, v in nodes.items() if v.get("resource_type") == "model"}
        sel = set(names)
        deps = {}
        for n in names:
            uid = name_to_uid.get(n)
            parents = nodes.get(uid, {}).get("depends_on", {}).get("nodes", []) if uid else []
            deps[n] = {nodes[p]["name"] for p in parents if p in nodes and nodes[p]["name"] in sel}
        out, seen = [], set()
        def visit(n, stack):
            if n in seen:
                return
            for d in deps.get(n, ()):
                if d not in stack:
                    visit(d, stack | {n})
            seen.add(n); out.append(n)
        for n in names:
            visit(n, set())
        return out
    except Exception:
        return names


def main():
    ap = argparse.ArgumentParser(description="Resumable gap-window incremental backfill")
    ap.add_argument("--select", nargs="+", required=True, help="dbt selector(s)")
    ap.add_argument("--exclude", nargs="+", default=None)
    ap.add_argument("--months", default="2026-05-01,2026-06-01",
                    help="comma-separated YYYY-MM-01 gap months (default May+June 2026)")
    ap.add_argument("--state", default=str(PROJECT_DIR / ".gap_refresh_state.json"))
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip", nargs="*", default=[],
                    help="models to treat as already-done (e.g. verified decodes)")
    ap.add_argument("--full-refresh-row-cap", type=int, default=5_000_000,
                    help="block --full-refresh of tables bigger than this unless --allow-big-full-refresh")
    ap.add_argument("--allow-big-full-refresh", action="store_true")
    args = ap.parse_args()

    months = [m.strip() for m in args.months.split(",") if m.strip()]
    first, last = months[0], months[-1]
    selector = " ".join(args.select)
    exclude = " ".join(args.exclude) if args.exclude else None

    state = {"completed": [], "failed": []}
    if args.resume and os.path.exists(args.state):
        state = json.load(open(args.state))
    state.setdefault("completed", [])
    state["completed"] = sorted(set(state["completed"]) | set(args.skip))

    def save():
        json.dump(state, open(args.state, "w"), indent=2)

    models = get_models_topo(selector, exclude)
    ch = None if args.dry_run else ch_client()
    db = os.environ.get("CLICKHOUSE_DATABASE", "dbt")

    plan = []
    for m in models:
        c = classify(m)
        if not c:
            continue
        if c["materialized"] in ("view", "table"):    # views live; tables -> cron
            continue
        if m in state["completed"]:
            continue
        # decide action
        if c["has_start_month"] or c["is_conditional"]:
            action, mode = "drop+vars", "vars"
        elif "insert_overwrite" in c["strategy"]:
            action, mode = "drop+plain", "plain"
        else:                                          # append / delete+insert, no start_month
            action, mode = "full-refresh", "full"
        plan.append((m, c, action, mode))

    print(f"gap months: {months}   models to process: {len(plan)}  "
          f"(skipped done/views/tables; selector matched {len(models)})")
    for m, c, action, mode in plan:
        print(f"  {action:12s} {m:52s} strat={c['strategy'][:24]:24s} part={c['partition_by']}")
    if args.dry_run:
        print("\nDRY RUN — nothing executed.")
        return 0

    env = dict(os.environ, DBT_WRITE_JSON="False")
    ok = fail = 0
    for m, c, action, mode in plan:
        print(f"\n=== {action}: {m} ===", flush=True)
        try:
            if mode in ("vars", "plain"):
                # Drop is a CLEANLINESS optimization (avoids ReplacingMergeTree dup
                # bloat on append models, and is required for append/insert_overwrite
                # whose watermark would otherwise exclude the past window). It's
                # best-effort: insert_overwrite/delete+insert overwrite the window via
                # the start_month branch anyway. plain mode (insert_overwrite, no
                # start_month) DOES need the drop to lower the watermark.
                pids = partition_ids(c["partition_by"], months)
                if pids is None:
                    if mode == "plain":
                        print(f"  !! plain-mode model with unmappable partition "
                              f"'{c['partition_by']}' -> cannot backfill safely; SKIP")
                        state.setdefault("failed", []).append(m); save(); fail += 1; continue
                    print(f"  (no partition to drop for '{c['partition_by']}'; "
                          f"relying on start_month overwrite/dedup)")
                else:
                    for pid in pids:
                        ch.command(f"ALTER TABLE `{db}`.`{m}` DROP PARTITION '{pid}'")
                        print(f"  dropped partition {pid}")
                cmd = ["dbt", "run", "--select", m, "--profiles-dir", str(PROJECT_DIR)]
                if mode == "vars":
                    cmd += ["--vars", json.dumps({"start_month": first, "end_month": last})]
            else:  # full-refresh
                rows = ch.query(f"SELECT count() FROM `{db}`.`{m}`").result_rows[0][0]
                if rows > args.full_refresh_row_cap and not args.allow_big_full_refresh:
                    print(f"  !! {rows:,} rows > cap {args.full_refresh_row_cap:,} -> SKIP "
                          f"(re-run with --allow-big-full-refresh to force)")
                    state.setdefault("failed", []).append(m); save(); fail += 1; continue
                cmd = ["dbt", "run", "--select", m, "--full-refresh", "--profiles-dir", str(PROJECT_DIR)]

            rc = sh(cmd, env=env).returncode
            if rc == 0:
                state["completed"].append(m)
                if m in state.get("failed", []):
                    state["failed"].remove(m)
                save(); ok += 1
                print(f"  OK ({ok}/{len(plan)})")
            else:
                state.setdefault("failed", []).append(m); save(); fail += 1
                print(f"  FAILED rc={rc} (recorded; re-run with --resume to retry)")
        except Exception as e:
            state.setdefault("failed", []).append(m); save(); fail += 1
            print(f"  ERROR {e} (recorded; --resume to retry)")

    print(f"\nDONE. ok={ok} fail={fail}  state={args.state}")
    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
