#!/bin/bash
# Re-pull price-dependent dbt models after the upstream prices source filled
# in a previously-skipped day.
#
# Why this exists
# ---------------
# `int_execution_token_prices_daily` is the source of truth for USD valuations.
# When its upstream (a Dune query) skips a day, every dbt model that JOINs with
# prices either drops rows (LEFT JOIN excluded by NULL price) or has 0 rows for
# that date entirely. Once the upstream is back-filled, those downstream models
# need to re-pull the affected window.
#
# Three-phase recovery (this script does all three)
# --------------------------------------------------
# Phase 1 — `tag:refill_append` cohort (heavy aggregates over balances)
#   Each model is run twice (Pass A + Pass B) with start_month/end_month
#   set, which selects `append` strategy. Between passes (and after the
#   second), `OPTIMIZE TABLE … PARTITION '<month>' FINAL DEDUPLICATE`
#   forces RMT to collapse duplicate rows so dependent aggregators see
#   merged source data on the next pass.
#
#   Why two passes: dbt resolves DAG order within a single `dbt run`
#   invocation, but RMT merges happen lazily AFTER each write. So in a
#   single-pass refill, an aggregator that runs immediately after its
#   upstream sees the upstream's *unmerged* duplicate parts and bakes a
#   multi-× inflated `sum(...)` into its row. RMT later collapses both
#   layers but the wrong aggregator value survives. Pass B re-runs every
#   aggregator against the now-merged source so the correct value gets
#   appended; the post-Pass-B OPTIMIZE keeps that correct row.
#
#   Phase 1.5 (canary) checks for adjacent-day jumps > 1.5× on a known
#   metric (GNO supply) at the month boundary; warns if Phase 1 didn't
#   converge.
#
# Phase 2 — every descendant of `int_execution_token_prices_daily` (lighter)
#   Lineage-driven (no manual tag): the dbt selector
#   `int_execution_token_prices_daily+` resolves to every model that ref()s
#   prices, transitively. The `apply_monthly_incremental_filter` macro reads
#   the `price_lookback_days` var directly, so passing it once on the command
#   line widens the window for every model in the subtree — no per-model
#   plumbing, no tag registry to keep in sync.
#
#   `tag:refill_append` is `--exclude`d — Phase 1 already recovered those
#   because their delete+insert path OOMs (CH 341) on multi-day windows.
#
# Phase 3 — rebuild downstream `fct_*` (table) and `api_*` (view)
#   `fct_*` tables are full-rebuild (`materialized='table'`); they only
#   refresh when explicitly run. After Phase 1 corrects the upstream
#   `int_*` rows, dependent `fct_*` keep their STALE values until rebuilt.
#   Phase 3 runs `tag:refill_append+` minus `tag:refill_append` so every
#   descendant of the cohort gets refreshed.
#
# Usage
# -----
#   # Re-pull for a specific gap date — script computes lookback as
#   # (today - DATE) + 1 inclusive + buffer days, and the affected month list.
#   docker exec dbt /app/scripts/maintenance/refill_after_price_gap.sh --from-date 2026-04-17
#
#   # Re-pull a fixed number of days backwards from today
#   docker exec dbt /app/scripts/maintenance/refill_after_price_gap.sh --lookback-days 11
#
#   # Skip Phase 1 (only re-run plain price-dependent models)
#   docker exec dbt /app/scripts/maintenance/refill_after_price_gap.sh \
#     --from-date 2026-04-17 --skip-balances-rewrite
#
#   # Skip Phase 2 (only fix tokens_balances_daily)
#   docker exec dbt /app/scripts/maintenance/refill_after_price_gap.sh \
#     --from-date 2026-04-17 --skip-price-dependent
#
#   # Restrict the Phase 2 model selector (default: int_execution_token_prices_daily+)
#   docker exec dbt /app/scripts/maintenance/refill_after_price_gap.sh \
#     --from-date 2026-04-17 --select int_revenue_sdai_fees_daily+
#
#   # Dry-run (print plan, no DB writes)
#   docker exec dbt /app/scripts/maintenance/refill_after_price_gap.sh \
#     --from-date 2026-04-17 --dry-run
#
# Behaviour
# ---------
# * Computes a single integer lookback that covers the gap.
# * Adds a small buffer (default 1 extra day) so partial coverage at the
#   trailing edge gets re-pulled too.
# * Phase 1 rewrites every month touched by [from-date, today] in append mode,
#   then OPTIMIZEs each touched partition.
# * Phase 2 forwards `--vars '{"price_lookback_days": N}'` to dbt run for
#   `int_execution_token_prices_daily+` (cascade-inclusive — downstream marts recompute).
#
# Environment overrides
# ---------------------
#   PROJECT_DIR  / PROFILES_DIR   - dbt paths (default /app)

set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/app}"
PROFILES_DIR="${PROFILES_DIR:-$PROJECT_DIR}"
# Lineage-driven default: every model that ref()s prices, transitively. No
# manual tag required — adding a new prices consumer is automatically covered.
DEFAULT_SELECT="int_execution_token_prices_daily+"
BUFFER_DAYS=1
SELECTOR="$DEFAULT_SELECT"
FROM_DATE=""
LOOKBACK=""
DRY_RUN=false
SKIP_PHASE1=false
SKIP_PHASE2=false

# Phase-1 cohort is tag-driven: any model tagged `refill_append` is a heavy
# aggregate whose `delete+insert` lookback path OOMs on a multi-day mutation,
# and which falls back to `append` strategy when `start_month`/`end_month`
# are set. To add a new model: tag it `refill_append` and ensure it has the
# `('append' if (start_month or incremental_end_date) else 'delete+insert')`
# strategy expression. No edits to this script needed.
PHASE1_TAG="refill_append"

usage() {
  cat >&2 <<'EOF'
Usage: refill_after_price_gap.sh
  (--from-date YYYY-MM-DD | --lookback-days N)
  [--select <selector>]                  # Phase 2 selector, default int_execution_token_prices_daily+
  [--buffer-days N]
  [--skip-balances-rewrite]              # skip Phase 1 (tokens_balances_daily rewrite)
  [--skip-price-dependent]               # skip Phase 2 (int_execution_token_prices_daily+ run)
  [--dry-run]
EOF
  exit 2
}

while [ $# -gt 0 ]; do
  case "$1" in
    --from-date)              FROM_DATE="$2"; shift 2 ;;
    --lookback-days)          LOOKBACK="$2"; shift 2 ;;
    --select)                 SELECTOR="$2"; shift 2 ;;
    --buffer-days)            BUFFER_DAYS="$2"; shift 2 ;;
    --skip-balances-rewrite)  SKIP_PHASE1=true; shift ;;
    --skip-price-dependent)   SKIP_PHASE2=true; shift ;;
    --dry-run)                DRY_RUN=true; shift ;;
    -h|--help)                usage ;;
    *)                        echo "Unknown arg: $1" >&2; usage ;;
  esac
done

if [ -z "$FROM_DATE" ] && [ -z "$LOOKBACK" ]; then
  echo "Error: provide either --from-date or --lookback-days" >&2
  usage
fi

# Compare day-aligned epochs (both at 00:00 UTC) so partial-day clock arithmetic
# can't under-count by 1.
TODAY_DAY="$(date -u +%Y-%m-%d)"
TODAY_EPOCH="$(date -u -d "${TODAY_DAY} 00:00:00 UTC" +%s 2>/dev/null \
              || date -j -f "%Y-%m-%d %H:%M:%S %Z" "${TODAY_DAY} 00:00:00 UTC" +%s 2>/dev/null)"

if [ -n "$FROM_DATE" ]; then
  FROM_EPOCH="$(date -u -d "${FROM_DATE} 00:00:00 UTC" +%s 2>/dev/null \
                || date -j -f "%Y-%m-%d %H:%M:%S %Z" "${FROM_DATE} 00:00:00 UTC" +%s 2>/dev/null)"
  if [ -z "${FROM_EPOCH:-}" ] || [ -z "${TODAY_EPOCH:-}" ]; then
    echo "Error: cannot parse --from-date='$FROM_DATE'" >&2
    exit 2
  fi
  GAP_DAYS=$(( (TODAY_EPOCH - FROM_EPOCH) / 86400 ))
  if [ "$GAP_DAYS" -lt 0 ]; then
    echo "Error: --from-date is in the future" >&2
    exit 2
  fi
  # +1 inclusive (covers from-date itself); +BUFFER_DAYS for trailing partials.
  LOOKBACK=$(( GAP_DAYS + 1 + BUFFER_DAYS ))
else
  # Derive an effective from-date so we can compute the affected month list.
  FROM_EPOCH=$(( TODAY_EPOCH - (LOOKBACK - 1) * 86400 ))
  FROM_DATE="$(date -u -d "@${FROM_EPOCH}" +%Y-%m-%d 2>/dev/null \
              || date -u -r "${FROM_EPOCH}" +%Y-%m-%d 2>/dev/null)"
  GAP_DAYS=$(( LOOKBACK - 1 - BUFFER_DAYS ))
fi

if ! [[ "$LOOKBACK" =~ ^[0-9]+$ ]] || [ "$LOOKBACK" -lt 1 ]; then
  echo "Error: lookback must be a positive integer (got '$LOOKBACK')" >&2
  exit 2
fi

# Compute affected months: every month touched by [FROM_DATE, today].
MONTHS=()
cur_epoch="$FROM_EPOCH"
last_month=""
while [ "$cur_epoch" -le "$TODAY_EPOCH" ]; do
  m="$(date -u -d "@${cur_epoch}" +%Y-%m-01 2>/dev/null \
       || date -u -r "${cur_epoch}" +%Y-%m-01 2>/dev/null)"
  if [ "$m" != "$last_month" ]; then
    MONTHS+=("$m")
    last_month="$m"
  fi
  cur_epoch=$(( cur_epoch + 86400 ))
done

echo "[$(date -u)] refill_after_price_gap.sh"
echo "  from-date           : $FROM_DATE  (gap = $GAP_DAYS d, +${BUFFER_DAYS} buffer)"
echo "  price_lookback_days : $LOOKBACK"
echo "  affected months     : ${MONTHS[*]}"
echo "  selector (phase 2)  : $SELECTOR"
echo "  phase 1 selector    : tag:$PHASE1_TAG"
echo "  skip phase 1        : $SKIP_PHASE1   (per-month append rewrite + OPTIMIZE)"
echo "  skip phase 2        : $SKIP_PHASE2"
echo "  dry-run             : $DRY_RUN"
echo

run_or_print() {
  if [ "$DRY_RUN" = "true" ]; then
    echo "[dry-run] $*"
  else
    "$@"
  fi
}

OVERALL_RC=0

# -- Phase 1 ------------------------------------------------------------------
# Two-pass per month:
#   Pass A — append-rewrite + OPTIMIZE every model. After this, source-of-
#            truth tables (e.g. int_execution_tokens_balances_daily) have
#            their correct, merged rows. But aggregators that ran during
#            Pass A may have read upstream BEFORE OPTIMIZE collapsed it,
#            so their rows can hold the inflated sums of unmerged duplicates.
#   Pass B — append-rewrite + OPTIMIZE every model AGAIN. This time every
#            aggregator's source is already merged, so it reads correct
#            values. The OPTIMIZE retains the latest (correct) row per key.
#
# Two passes are required because dbt's `--select tag:refill_append` runs
# all 12 models in one invocation, but RMT merges happen lazily AFTER each
# write — so an aggregator running mid-invocation sees its upstream's
# unmerged duplicates. There is no single-pass ordering that fixes this;
# we need a hard barrier (OPTIMIZE) between dependent layers, which means
# running twice.
#
# Phase 3 (after both passes complete) rebuilds the downstream `fct_*`
# tables and `api_*` views so they pick up the corrected `int_*` values.
if [ "$SKIP_PHASE1" = "false" ]; then
  if [ "$DRY_RUN" = "false" ]; then
    PHASE1_MODELS=$(dbt ls --select "tag:$PHASE1_TAG" --resource-type model --output name \
      --project-dir "$PROJECT_DIR" --profiles-dir "$PROFILES_DIR" 2>/dev/null \
      | grep -E '^[a-z_]+$' || true)
    if [ -z "$PHASE1_MODELS" ]; then
      echo "[phase1] no models tagged $PHASE1_TAG — skipping"
    fi
  else
    PHASE1_MODELS="<resolved at runtime from tag:$PHASE1_TAG>"
  fi

  echo "=== Phase 1: two-pass append-rewrite tag:$PHASE1_TAG + OPTIMIZE ==="
  echo "[phase1] models: $(echo "$PHASE1_MODELS" | tr '\n' ' ')"

  for m in "${MONTHS[@]}"; do
    for pass in A B; do
      echo "[phase1][pass-$pass] month=$m  rewrite (append, dbt-managed DAG order)"
      run_or_print dbt run \
        --select "tag:$PHASE1_TAG" \
        --vars "{\"start_month\":\"$m\",\"end_month\":\"$m\"}" \
        --project-dir "$PROJECT_DIR" \
        --profiles-dir "$PROFILES_DIR" \
        || { echo "[phase1][pass-$pass] rewrite failed for $m"; OVERALL_RC=2; break 2; }

      if [ "$DRY_RUN" = "false" ]; then
        for model in $PHASE1_MODELS; do
          echo "[phase1][pass-$pass] $model  month=$m  OPTIMIZE PARTITION FINAL DEDUPLICATE"
          run_or_print dbt run-operation optimize_partition_final \
            --args "{database: dbt, table_name: ${model}, partition: \"$m\"}" \
            --project-dir "$PROJECT_DIR" \
            --profiles-dir "$PROFILES_DIR" \
            || { echo "[phase1][pass-$pass] $model OPTIMIZE failed for $m"; OVERALL_RC=2; break 3; }
        done
      else
        echo "[dry-run][pass-$pass] for each model in tag:$PHASE1_TAG: optimize_partition_final partition=$m"
      fi
    done
  done
  echo
fi

# -- Phase 1.5 — sanity check ------------------------------------------------
# After the two-pass refill, scan a known canary metric for an adjacent-day
# jump > 1.5× across the month boundary. A jump means some aggregator still
# holds inflated values from an earlier write — a sign Phase 1 didn't fully
# converge. Refill is a no-op if the canary is clean.
if [ "$SKIP_PHASE1" = "false" ] && [ "$OVERALL_RC" -eq 0 ] && [ "$DRY_RUN" = "false" ]; then
  echo "=== Phase 1.5: canary check on int_execution_tokens_supply_holders_daily (GNO) ==="
  CANARY_SQL="WITH s AS (SELECT date, supply FROM dbt.int_execution_tokens_supply_holders_daily \
WHERE symbol='GNO' AND date >= toDate('${FROM_DATE}') - 7 AND date <= toDate('${FROM_DATE}') + 7) \
SELECT date, supply, supply / nullIf(lagInFrame(supply) OVER (ORDER BY date), 0) AS ratio \
FROM s ORDER BY date"
  CANARY_OUT=$(dbt run-operation run_query --args "{sql: \"$CANARY_SQL\"}" \
    --project-dir "$PROJECT_DIR" --profiles-dir "$PROFILES_DIR" 2>/dev/null || true)
  echo "$CANARY_OUT" | tail -20
  if echo "$CANARY_OUT" | awk '/^\| / && NF>=7 {gsub(/[|]/," "); r=$5+0; if(r>1.5||(r>0&&r<0.66)) print}' | grep -q .; then
    echo "[phase1.5] WARNING: adjacent-day GNO supply ratio > 1.5× detected — Phase 1 may not have converged"
    echo "[phase1.5] Inspect manually before declaring success."
    OVERALL_RC=3
  fi
  echo
fi

# -- Phase 2 ------------------------------------------------------------------
# Lineage-driven re-pull: every model downstream of the prices view, with
# the wider `price_lookback_days` window. The macro
# `apply_monthly_incremental_filter` reads this var directly, so no
# per-model plumbing is required. tag:refill_append is excluded — those were
# already recovered in Phase 1 via append + OPTIMIZE because their
# delete+insert path OOMs on a multi-day mutation.
if [ "$SKIP_PHASE2" = "false" ] && [ "$OVERALL_RC" -eq 0 ]; then
  echo "=== Phase 2: $SELECTOR --exclude tag:$PHASE1_TAG  (price_lookback_days=$LOOKBACK) ==="
  run_or_print dbt run \
    --select "$SELECTOR" \
    --exclude "tag:$PHASE1_TAG" \
    --vars "{\"price_lookback_days\": $LOOKBACK}" \
    --project-dir "$PROJECT_DIR" \
    --profiles-dir "$PROFILES_DIR" \
    || OVERALL_RC=$?
fi

# -- Phase 3 — rebuild downstream `fct_*` (table) and `api_*` (view) -----------
# `fct_*` tables are full-rebuild (`materialized='table'`) and only refresh
# when explicitly run. After Phase 1 corrects the upstream `int_*` rows,
# any dependent `fct_*` keeps its STALE values until rebuilt — and `api_*`
# views downstream of those `fct_*` tables expose the stale values too.
# This phase rebuilds every descendant of tag:refill_append, excluding the
# refill_append nodes themselves (Phase 1 handled them) and the price-
# dependent leaves (Phase 2 handled them).
if [ "$SKIP_PHASE1" = "false" ] && [ "$OVERALL_RC" -eq 0 ]; then
  echo "=== Phase 3: rebuild downstream fct_*/api_* of tag:$PHASE1_TAG ==="
  run_or_print dbt run \
    --select "tag:$PHASE1_TAG+" \
    --exclude "tag:$PHASE1_TAG" \
    --project-dir "$PROJECT_DIR" \
    --profiles-dir "$PROFILES_DIR" \
    || OVERALL_RC=$?
fi

echo "[$(date -u)] refill_after_price_gap.sh: exit=$OVERALL_RC"
exit "$OVERALL_RC"
