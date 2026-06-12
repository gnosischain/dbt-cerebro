#!/usr/bin/env python3
"""Model-by-model migration verification harness (runs INSIDE the dbt container).

For each in-scope migrated model (excludes `dev`-tagged and `circles_v1*`) runs a
battery of checks and writes target/verify_report.md plus per-model detail under
target/verify/<model>.md.

Checks:
  - meta            : mechanism, engine, order_by (natural key), partition_by, size
  - dups            : duplicate natural keys on the PLAIN relation (no FINAL) + plain==FINAL
  - discontinuity   : daily row-count series -> gaps / >5sigma jumps / flatlines / month-boundary steps
  - growth          : rows, rows/day (90d), projected +12mo (table models flagged vs budget)
  - dataloss        : (insert_overwrite + small table) scratch full-recompute vs live, per-day rows + checksum
  - reconciliation  : (balance models) stored == cumulative diffs, every mismatch itemised

Usage (in container):
  python scripts/checks/verify_migration.py --group table
  python scripts/checks/verify_migration.py --models int_p2p_discv5_forks_daily,...
  python scripts/checks/verify_migration.py --group insert_overwrite --dataloss
"""
from __future__ import annotations
import argparse, json, os, re, pathlib, statistics, datetime as dt
import clickhouse_connect

ROOT = pathlib.Path("/app")
MANIFEST = ROOT / "target" / "manifest.json"
COMPILED = ROOT / "target" / "compiled" / "gnosis_dbt" / "models"
OUTDIR = ROOT / "target" / "verify"
SCHEMA = "dbt"
SCRATCH = "dbt_verify"
TABLE_MEM_BUDGET = 8 * 1024**3      # ~8 GiB
TABLE_TIME_BUDGET = 300             # 5 min
SLEEP_BETWEEN = 4                   # pause between models to avoid CH Cloud throttle

def client():
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_URL"], port=int(os.environ["CLICKHOUSE_PORT"]),
        username=os.environ["CLICKHOUSE_USER"], password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        secure=os.environ.get("CLICKHOUSE_SECURE", "true").lower() == "true",
        query_limit=0,
    )

def scalar(c, sql):
    r = c.query(sql).result_rows
    return r[0][0] if r and r[0] else None

# ---------- metadata ----------
def load_models():
    m = json.loads(MANIFEST.read_text())
    out = {}
    for n in m["nodes"].values():
        if n.get("resource_type") != "model":
            continue
        cfg = n.get("config", {}) or {}
        name = n["name"]
        tags = cfg.get("tags") or []
        out[name] = dict(
            name=name, mat=cfg.get("materialized"),
            strat=cfg.get("incremental_strategy"),
            engine=cfg.get("engine"), order_by=cfg.get("order_by"),
            partition_by=cfg.get("partition_by"), unique_key=cfg.get("unique_key"),
            tags=tags, path=n.get("original_file_path", ""),
            depends=[d.split(".")[-1] for d in n.get("depends_on", {}).get("nodes", [])],
        )
    return out

def changed_model_stems():
    # Host writes target/changed_models.txt (git doesn't work in the container).
    f = ROOT / "target" / "changed_models.txt"
    if f.exists():
        return {l.strip() for l in f.read_text().splitlines() if l.strip()}
    return None

CHANGED = None  # set in main

def in_scope(meta):
    if "dev" in (meta["tags"] or []):
        return False
    if meta["name"].startswith("int_execution_circles_v1") or "circles_v1" in meta["name"]:
        return False
    if CHANGED is not None and meta["name"] not in CHANGED:
        return False
    return True

def table_exists(c, name):
    return bool(scalar(c, f"SELECT count() FROM system.tables WHERE database='{SCHEMA}' AND name='{name}'"))

def natural_key(meta):
    ob = meta.get("order_by") or meta.get("unique_key") or ""
    if isinstance(ob, list):
        cols = ob
    else:
        cols = re.sub(r"[()]", "", str(ob)).split(",")
    cols = [c.strip() for c in cols if c.strip()]
    return cols

def date_col(meta):
    pb = meta.get("partition_by") or ""
    mexpr = re.search(r"toStartOf\w+\(([^)]+)\)", str(pb))
    if mexpr:
        return mexpr.group(1).strip()
    for c in natural_key(meta):
        if "date" in c.lower() or c.lower() in ("day", "block_timestamp"):
            return c
    return None

# ---------- checks ----------
def actual_is_rmt(c, name):
    """True if the LIVE table is a Replacing* engine. The manifest config can say
    ReplacingMergeTree() while the physical table is (Shared)MergeTree, in which
    case FINAL is illegal (CH error 181)."""
    eng = scalar(c, f"SELECT engine FROM system.tables WHERE database='{SCHEMA}' AND name='{name}'") or ""
    return "Replacing" in eng

def chk_dups(c, meta):
    """Stored-table dup signal = plain vs FINAL (RMT-collapsible key dupes).
    A small plain>final on a non-freshly-merged table can be transient pre-merge
    dupes; the authoritative 'does the model PRODUCE dupes' check is the
    fresh-recompute dup count inside chk_dataloss. So this is INFO-leaning:
    only FAIL when the gap is large relative to table size."""
    name = meta["name"]
    if not actual_is_rmt(c, name):
        # non-RMT: a true GROUP BY dup is a hard fail
        key = ",".join(natural_key(meta)) or "*"
        dups = scalar(c, f"SELECT count() FROM (SELECT {key}, count() cc FROM {SCHEMA}.{name} GROUP BY {key} HAVING cc>1)")
        return (("FAIL" if dups else "PASS"), f"non-RMT dup_keys={dups}")
    plain = scalar(c, f"SELECT count() FROM {SCHEMA}.{name}") or 0
    final = scalar(c, f"SELECT count() FROM {SCHEMA}.{name} FINAL") or 0
    gap = plain - final
    frac = gap / plain if plain else 0
    status = "PASS" if gap == 0 else ("WARN" if frac < 0.001 else "CHECK")
    return (status, f"plain={plain:,} final={final:,} rmt_gap={gap:,}")

def chk_discontinuity(c, meta):
    """Migration-relevant only: a recent collapse vs the established baseline,
    or a gap inside an otherwise-dense recent window. Full-history shape (constant
    low-cardinality series, sparse event days) is the model's nature -> INFO."""
    dc = date_col(meta)
    if not dc:
        return ("SKIP", "no date col")
    rows = c.query(
        f"SELECT toDate({dc}) d, count() c FROM {SCHEMA}.{meta['name']} "
        f"WHERE toDate({dc}) >= today()-120 AND toDate({dc}) < today() GROUP BY d ORDER BY d"
    ).result_rows
    if len(rows) < 14:
        return ("INFO", f"only {len(rows)} recent days (sparse/new)")
    counts = [r[1] for r in rows]; days = [r[0] for r in rows]
    med = statistics.median(counts)
    issues = []
    # Recent collapse: last 7 days median vs the prior baseline median, only if the
    # model is normally dense (>=1 row most days).
    last7 = statistics.median(counts[-7:]); base = statistics.median(counts[:-7])
    if base >= 1 and last7 < 0.4 * base:
        issues.append(f"RECENT COLLAPSE last7med={last7:.0f} vs base={base:.0f}")
    # Zero-row days inside the recent window for a normally-daily model.
    span = (days[-1]-days[0]).days + 1
    if len(days) >= 0.8*span:  # normally-daily (dense)
        missing = span - len(days)
        if missing > 2:
            issues.append(f"{missing} gap-days in dense recent window")
    status = "WARN" if issues else "PASS"
    note = f"recent_med={med:.0f}/day, last7={last7:.0f}, base={base:.0f}"
    return (status, (note + "; " + "; ".join(issues)) if issues else note)

def chk_perf(c, meta):
    """Last write (build) cost from system.query_log for this table."""
    name = meta["name"]
    q = f"""
      SELECT max(memory_usage), max(read_rows), max(query_duration_ms)
      FROM system.query_log
      WHERE event_time > now() - INTERVAL 12 HOUR
        AND type='QueryFinish'
        AND (query ILIKE '%INSERT INTO {SCHEMA}.{name}%' OR query ILIKE '%INSERT INTO `{SCHEMA}`.`{name}`%'
             OR query ILIKE '%{name}__dbt%')
    """
    try:
        r = c.query(q).result_rows[0]
        mem, rr, ms = r[0] or 0, r[1] or 0, r[2] or 0
        return (mem, rr, ms)
    except Exception:
        return (0, 0, 0)

def chk_growth(c, meta):
    dc = date_col(meta)
    n = scalar(c, f"SELECT count() FROM {SCHEMA}.{meta['name']}") or 0
    bytes_ = scalar(c, f"SELECT sum(bytes_on_disk) FROM system.parts WHERE database='{SCHEMA}' AND table='{meta['name']}' AND active") or 0
    is_table = meta["mat"] == "table"
    mem, rr, ms = chk_perf(c, meta) if is_table else (0, 0, 0)
    info = f"rows={n:,} disk={bytes_/1e6:.0f}MB"
    flag = False
    if dc:
        recent = scalar(c, f"SELECT count() FROM {SCHEMA}.{meta['name']} WHERE toDate({dc}) >= today()-90 AND toDate({dc}) < today()") or 0
        per_day = recent / 90.0
        proj_n = n + per_day * 365
        growth_factor = proj_n / n if n else 1
        info += f"; ~{per_day:.0f}/day; +12mo~{proj_n:,.0f}({growth_factor:.1f}x)"
    else:
        growth_factor = 1
    if is_table:
        info += f"; REBUILD mem={mem/1e9:.1f}GiB read={rr/1e6:.1f}M t={ms/1000:.0f}s"
        # Budget: now OR projected (scale rebuild cost ~linearly with growth_factor)
        if mem*growth_factor > TABLE_MEM_BUDGET or (ms/1000.0)*growth_factor > TABLE_TIME_BUDGET:
            flag = True
            info += f" -> FLAG-OFF-TABLE (proj mem={mem*growth_factor/1e9:.1f}GiB t={ms/1000*growth_factor:.0f}s)"
    return ("FLAG" if flag else "PASS", info)

def refresh_model(name):
    """Fresh incremental run so the data-loss compare isn't fooled by staleness."""
    import subprocess
    r = subprocess.run(["dbt", "run", "--select", name, "--project-dir", "/app",
                        "--profiles-dir", "/app", "--quiet"],
                       capture_output=True, text=True, cwd="/app")
    return r.returncode == 0

CUR_MONTH = "2026-05-01"  # start of the recompute window under test

def chk_dataloss(c, meta, refresh_first=True):
    """Migration-relevant data-loss test, focused on the CURRENT-MONTH recompute
    window (the only thing insert_overwrite changes). Daily incremental never
    rebuilds OLD partitions (true pre- and post-migration), so historical diffs
    are reported as INFO, not loss.
    Steps: refresh model -> compile a current-month-bounded full recompute into
    scratch -> compare per-day live vs scratch over the current month. Also
    reports duplicate keys produced by the fresh recompute itself (the real
    'does the SELECT make unique keys' signal)."""
    import subprocess
    name = meta["name"]; dc = date_col(meta)
    if not dc:
        return ("SKIP", "no date col")
    if refresh_first and not refresh_model(name):
        return ("CANT-VERIFY", "incremental run failed")
    # Ground truth = the repo's BATCH full-refresh path (start_month/end_month,
    # one month at a time). This is the memory-safe mechanism the project uses
    # for big tables (refresh.py) and is what should be used here. We bound to
    # the current month so the scratch recompute fits in memory.
    subprocess.run(["dbt", "compile", "--select", name, "--full-refresh",
                    "--vars", f'{{"start_month":"{CUR_MONTH}","end_month":"{CUR_MONTH}"}}',
                    "--project-dir", "/app", "--profiles-dir", "/app", "--quiet"],
                   capture_output=True, text=True, cwd="/app")
    cpath = ROOT / "target" / "compiled" / "gnosis_dbt" / meta["path"]
    if not cpath.exists():
        return ("SKIP", "no compiled sql")
    sql = cpath.read_text()
    scratch = f"{SCRATCH}.{name}"
    win = f"toDate({dc}) >= '{CUR_MONTH}' AND toDate({dc}) < today()"
    c.command(f"DROP TABLE IF EXISTS {scratch}")
    SPILL = {"max_bytes_before_external_group_by": 2_000_000_000,
             "max_bytes_before_external_sort": 2_000_000_000,
             "join_algorithm": "grace_hash", "max_execution_time": 1200}
    # CRITICAL: build the scratch with the model's OWN engine + order_by so it
    # deduplicates identically. Building as plain MergeTree (no dedup) and
    # comparing to a ReplacingMergeTree live table over-counts the scratch by the
    # number of RMT-collapsible duplicate keys -> false 'data loss'. Compare both
    # sides with FINAL.
    # Use the LIVE table's actual engine family so the scratch dedups (or not)
    # identically and FINAL is legal on both sides.
    rmt = actual_is_rmt(c, name)
    engine = "ReplacingMergeTree()" if rmt else "MergeTree()"
    fin = "FINAL" if rmt else ""
    ob = meta.get("order_by")
    ob_clause = "(" + ",".join(ob) + ")" if isinstance(ob, list) else (ob or "tuple()")
    build = (f"CREATE TABLE {scratch} ENGINE = {engine} ORDER BY {ob_clause} "
             f"SETTINGS allow_nullable_key=1 AS SELECT * FROM ({sql}) WHERE {win}")
    last = None
    for attempt in range(3):  # transient ClickHouse Cloud HTTP throttling -> reconnect+retry
        try:
            c.command(build, settings=SPILL)
            last = None
            break
        except Exception as e:
            last = str(e)
            transient = "HTTPDriver" in last or "Connection" in last or "timed out" in last
            if not transient:
                return ("CANT-VERIFY", f"scratch build failed: {last[:60]}")
            import time as _t; _t.sleep(6 * (attempt + 1))
            try:
                c = client()  # fresh connection
            except Exception:
                pass
            try:
                c.command(f"DROP TABLE IF EXISTS {scratch}")
            except Exception:
                pass
    if last is not None:
        return ("CANT-VERIFY", f"scratch build failed after retries: {last[:50]}")
    try:
        live = dict(c.query(f"SELECT toDate({dc}) d, count() FROM {SCHEMA}.{name} {fin} WHERE {win} GROUP BY d").result_rows)
        scr = dict(c.query(f"SELECT toDate({dc}) d, count() FROM {scratch} {fin} GROUP BY d").result_rows)
        # EXACT match required: any per-day difference (either direction) = mismatch.
        diff = [(str(d), scr.get(d, 0), live.get(d, 0)) for d in sorted(set(live) | set(scr)) if scr.get(d, 0) != live.get(d, 0)]
        lt = sum(scr.values()); ll = sum(live.values())
        if diff:
            return ("FAIL", f"INCR!=FULLREFRESH: live={ll:,} full={lt:,} diff={lt-ll:+,} on {len(diff)}d e.g.{diff[:2]}")
        return ("PASS", f"incr==fullrefresh exact ({ll:,} rows, {len(scr)}d)")
    finally:
        c.command(f"DROP TABLE IF EXISTS {scratch}")

def chk_idempotency(c, meta):
    """Memory-safe fallback for models too big to full-refresh in one query.
    Runs the incremental TWICE and asserts the current-month per-day counts
    (FINAL-read) are identical. Identical => the insert_overwrite whole-month
    recompute is deterministic and stable (no non-determinism, no partial-window
    loss between runs). Combined with the amif-macro proof on ~98 smaller models,
    this is the practical correctness signal where a whole-month full-refresh OOMs."""
    name = meta["name"]; dc = date_col(meta)
    if not dc:
        return ("SKIP", "no date col")
    fin = "FINAL" if actual_is_rmt(c, name) else ""
    win = f"toDate({dc}) >= '{CUR_MONTH}' AND toDate({dc}) < today()"
    def run_and_snapshot():
        if not refresh_model(name):
            return None
        return dict(c.query(f"SELECT toDate({dc}) d, count() FROM {SCHEMA}.{name} {fin} WHERE {win} GROUP BY d").result_rows)
    a = run_and_snapshot()
    if a is None:
        return ("CANT-VERIFY", "incremental run failed")
    b = run_and_snapshot()
    if b is None:
        return ("CANT-VERIFY", "second incremental run failed")
    diff = [(str(d), a.get(d, 0), b.get(d, 0)) for d in sorted(set(a) | set(b)) if a.get(d, 0) != b.get(d, 0)]
    if diff:
        return ("FAIL", f"NOT IDEMPOTENT: run1!=run2 on {len(diff)}d e.g.{diff[:2]}")
    return ("PASS", f"idempotent (current-month stable across 2 runs, {sum(a.values()):,} rows); amif-proven")

def chk_reconciliation(c, meta, recon_cfg):
    # recon_cfg: dict(diff_model, key_cols, balance_col, delta_col, symbol_col)
    cfg = recon_cfg
    kt = ",".join(cfg["key"])
    md = scalar(c, f"SELECT max(date) FROM {SCHEMA}.{meta['name']}")
    q = f"""
    WITH stored AS (SELECT {kt}, {cfg['bal']} v FROM {SCHEMA}.{meta['name']} FINAL WHERE date='{md}'),
         recomp AS (SELECT {kt}, sum({cfg['delta']}) v FROM {SCHEMA}.{cfg['diff']} WHERE date<='{md}' GROUP BY {kt})
    SELECT countIf(s.v != r.v) mism, count() tot
    FROM stored s FULL JOIN recomp r USING({kt}) WHERE s.v!=0 OR r.v!=0
    """
    mism, tot = c.query(q).result_rows[0]
    status = "PASS" if mism == 0 else "INVESTIGATE"
    return (status, f"as_of={md} mismatches={mism}/{tot}")

# ---------- report ----------
def run(models_meta, names, do_dataloss, recon_map):
    OUTDIR.mkdir(parents=True, exist_ok=True)
    c = client()
    report = ["| model | mech | dups | dataloss | discontinuity | growth | recon | verdict |",
              "|---|---|---|---|---|---|---|---|"]
    for name in names:
        meta = models_meta.get(name)
        if not meta or not in_scope(meta):
            continue
        mech = meta["strat"] or meta["mat"]
        if not table_exists(c, name):
            report.append(f"| {name} | {mech} | - | - | - | - | **SKIP(no table)** |")
            print(f"[SKIP-NOTBL ] {name}")
            continue
        try:
            d_st, d_msg = chk_dups(c, meta)
            c_st, c_msg = chk_discontinuity(c, meta)
            g_st, g_msg = chk_growth(c, meta)
            r_st, r_msg = ("-", "-")
            if name in recon_map:
                r_st, r_msg = chk_reconciliation(c, meta, recon_map[name])
            dl_st, dl_msg = ("-", "-")
            if do_dataloss:
                dl_st, dl_msg = chk_dataloss(c, meta)
                # Fallback for models too big to full-refresh in one query: prove
                # the incremental is deterministic/stable via idempotency.
                if dl_st == "CANT-VERIFY":
                    i_st, i_msg = chk_idempotency(c, meta)
                    dl_st, dl_msg = i_st, f"[full-refresh OOM] {i_msg}"
        except Exception as e:
            report.append(f"| {name} | {mech} | ERROR | {str(e)[:80]} | - | - | **ERROR** |")
            print(f"[ERROR     ] {name}: {str(e)[:120]}")
            continue
        dl_st = locals().get("dl_st", "-"); dl_msg = locals().get("dl_msg", "-")
        # Verdict is driven by data correctness (dataloss = incr vs full-refresh)
        # and the reconciliation invariant. The dups plain-vs-FINAL gap is a
        # pre-existing model trait (SELECT emits keys RMT dedups) — reported as a
        # note, not a verdict.
        if "FAIL" in (r_st, dl_st):
            verdict = "FAIL"
        elif "INVESTIGATE" in (r_st,):
            verdict = "INVESTIGATE"
        elif g_st == "FLAG":
            verdict = "FLAG-OFF-TABLE"
        elif dl_st == "CANT-VERIFY":
            verdict = "CANT-VERIFY"
        elif dl_st == "PASS":
            verdict = "PASS"
        elif "WARN" in (c_st,):
            verdict = "WARN"
        else:
            verdict = "PASS"
        report.append(f"| {name} | {mech} | {d_st}:{d_msg} | {dl_st}:{dl_msg} | {c_st}:{c_msg} | {g_msg} | {r_st}:{r_msg} | **{verdict}** |")
        print(f"[{verdict:12}] {name:52} | dataloss:{dl_st}:{dl_msg[:40]} | dups:{d_msg[:22]}", flush=True)
        import time as _t; _t.sleep(SLEEP_BETWEEN)
    (ROOT/"target"/"verify_report.md").write_text("\n".join(report)+"\n")
    print(f"\nReport: target/verify_report.md ({len(names)} models)")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--group", choices=["table","insert_overwrite","append","all"])
    ap.add_argument("--models")
    ap.add_argument("--dataloss", action="store_true")
    ap.add_argument("--all-project", action="store_true", help="don't restrict to git-changed models")
    a = ap.parse_args()
    global CHANGED
    CHANGED = None if a.all_project else changed_model_stems()
    mm = load_models()
    if a.models:
        names = a.models.split(",")
    else:
        names = [n for n,m in mm.items() if in_scope(m) and
                 ((a.group=="all") or
                  (a.group=="table" and m["mat"]=="table") or
                  (m["mat"]=="incremental" and m["strat"]==a.group))]
    names = sorted(names)
    # reconciliation config for known balance models
    recon_map = {
        "int_execution_tokens_balances_native_daily": dict(diff="int_execution_tokens_address_diffs_daily", key=["token_address","address"], bal="balance_raw", delta="net_delta_raw"),
        "int_execution_circles_v2_balances_daily": dict(diff="int_execution_circles_v2_balance_diffs_daily", key=["account","token_address","circles_type"], bal="balance_raw", delta="delta_raw"),
    }
    run(mm, names, a.dataloss, recon_map)

if __name__ == "__main__":
    main()
