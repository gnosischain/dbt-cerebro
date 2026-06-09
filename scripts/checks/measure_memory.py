#!/usr/bin/env python3
"""Measure memory/storage for dbt tables & views (metadata-only, cheap).

Two dimensions:
  - STORAGE (tables only): system.parts -> bytes_on_disk, rows, and the RAM the
    table costs just by existing (primary-key index + marks held in memory).
    Views have no parts -> 0 storage; their cost is paid per read.
  - QUERY MEMORY: system.query_log -> memory_usage / read_rows for recent
    queries that touched the object. For a view this is the real "is it heavy?"
    number (the cost of computing it on read).

Usage (in container):
  python scripts/checks/measure_memory.py                      # top tables by disk
  python scripts/checks/measure_memory.py fct_revenue_%        # name LIKE filter
  python scripts/checks/measure_memory.py int_revenue_fees_weekly_per_user
"""
import os, sys, clickhouse_connect

DB = "dbt"
c = clickhouse_connect.get_client(
    host=os.environ["CLICKHOUSE_URL"], port=int(os.environ["CLICKHOUSE_PORT"]),
    username=os.environ["CLICKHOUSE_USER"], password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    secure=True, query_limit=0)

pat = sys.argv[1] if len(sys.argv) > 1 else "%"

def gib(x): return f"{(x or 0)/1024**3:.2f}GiB"
def mb(x):  return f"{(x or 0)/1e6:.0f}MB"

print(f"=== STORAGE (system.parts, active) — tables matching '{pat}' ===")
rows = c.query(f"""
  SELECT table,
         sum(rows) AS rows,
         sum(bytes_on_disk) AS disk,
         sum(primary_key_bytes_in_memory_allocated) AS pk_ram,
         sum(marks_bytes) AS marks,
         count() AS parts
  FROM system.parts
  WHERE database='{DB}' AND active AND table LIKE '{pat}'
  GROUP BY table ORDER BY disk DESC LIMIT 40
""").result_rows
if not rows:
    print("  (no stored parts — these are VIEWS, or no match; views cost memory per read, see below)")
for t, r, disk, pk, marks, parts in rows:
    print(f"  {t:52} rows={r:>13,} disk={mb(disk):>8} ram(pk+marks)={mb((pk or 0)+(marks or 0)):>7} parts={parts}")

print(f"\n=== RECENT QUERY MEMORY (system.query_log, last 24h) touching '{pat}' ===")
rows = c.query(f"""
  SELECT
    extract(query, '(?:FROM|JOIN)\\\\s+`?{DB}`?\\\\.`?([A-Za-z0-9_%]+)`?') AS obj,
    count() AS n,
    max(memory_usage) AS peak,
    quantile(0.95)(memory_usage) AS p95,
    max(read_rows) AS max_read_rows,
    max(query_duration_ms) AS max_ms
  FROM system.query_log
  WHERE event_time > now() - INTERVAL 24 HOUR
    AND type='QueryFinish' AND query ILIKE '%{DB}.{pat.replace('%','')}%'
  GROUP BY obj HAVING obj != '' ORDER BY peak DESC LIMIT 30
""").result_rows
if not rows:
    print("  (no recent queries in query_log touched these objects — run one to measure)")
for obj, n, peak, p95, rr, ms in rows:
    print(f"  {obj:52} n={n:>4} peak={gib(peak):>9} p95={gib(p95):>9} read_rows={(rr or 0):>13,} t={(ms or 0)/1000:.0f}s")
