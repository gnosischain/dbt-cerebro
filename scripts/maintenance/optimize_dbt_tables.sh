#!/bin/bash
# Weekly housekeeping job: OPTIMIZE dbt-managed RMT tables that have
# accumulated too many parts.
#
# Why this exists
# ---------------
# Every `dbt run` against an incremental ReplacingMergeTree model creates
# new parts. ClickHouse expects background merges to consolidate them, but
# on small or saturated clusters merges fall behind. As parts accumulate,
# part-metadata (mark cache, primary index, granularity) consumes a
# growing share of cluster RAM — eventually starving foreground queries
# (we observed CH 241 OOM at 23k cluster-wide parts).
#
# This script issues per-partition `OPTIMIZE FINAL DEDUPLICATE` for any
# (table, partition) whose active part count exceeds OPTIMIZE_THRESHOLD,
# scoped to one schema (default `dbt`). Per-partition is far cheaper than
# whole-table FINAL because it merges only the parts for that partition,
# not the entire table.
#
# Schedule
# --------
# Intended to run weekly during off-peak (e.g. Sundays 03:00 UTC). It's
# safe to run more often if part-count grows fast.
#
# Failure modes
# -------------
# * CH 388 ("background pool is already full") on an individual OPTIMIZE
#   will abort the run with non-zero exit. Anything not optimized this run
#   gets retried next week. Don't loop-retry inside one job — the merge
#   pool needs time to drain.
# * Other ClickHouse errors propagate and exit non-zero so cron alerting
#   surfaces them.
#
# Environment overrides
# ---------------------
# OPTIMIZE_THRESHOLD     - parts/partition above which to optimize (default 50)
# OPTIMIZE_DRY_RUN       - "true" to list candidates only (default false)
# OPTIMIZE_DATABASE      - target schema (default "dbt")
# OPTIMIZE_TABLE_FILTER  - SQL LIKE pattern (default "%")
# OPTIMIZE_MAX_PARTITIONS - safety cap on OPTIMIZEs per run (default 200)
# PROJECT_DIR / PROFILES_DIR - dbt paths (default /app for the docker container)

set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/app}"
PROFILES_DIR="${PROFILES_DIR:-$PROJECT_DIR}"
OPTIMIZE_THRESHOLD="${OPTIMIZE_THRESHOLD:-50}"
OPTIMIZE_DRY_RUN="${OPTIMIZE_DRY_RUN:-false}"
OPTIMIZE_DATABASE="${OPTIMIZE_DATABASE:-dbt}"
OPTIMIZE_TABLE_FILTER="${OPTIMIZE_TABLE_FILTER:-%}"
OPTIMIZE_MAX_PARTITIONS="${OPTIMIZE_MAX_PARTITIONS:-200}"

echo "[$(date -u)] optimize_dbt_tables.sh: threshold=$OPTIMIZE_THRESHOLD database=$OPTIMIZE_DATABASE dry_run=$OPTIMIZE_DRY_RUN"

# Build the args JSON. dry_run must be JSON-bool, not string.
ARGS=$(cat <<EOF
{
  "threshold": $OPTIMIZE_THRESHOLD,
  "dry_run": $OPTIMIZE_DRY_RUN,
  "database": "$OPTIMIZE_DATABASE",
  "table_filter": "$OPTIMIZE_TABLE_FILTER",
  "max_partitions": $OPTIMIZE_MAX_PARTITIONS
}
EOF
)

dbt run-operation optimize_dbt_tables_by_threshold \
  --args "$ARGS" \
  --project-dir "$PROJECT_DIR" \
  --profiles-dir "$PROFILES_DIR"

echo "[$(date -u)] optimize_dbt_tables.sh: done"
