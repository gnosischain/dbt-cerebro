#!/bin/bash
# Shared dbt observability orchestrator.
#
# Called by cron.sh (production) and cron_preview.sh (preview) after they
# set environment-specific defaults.
#
# Required env vars:
#   EDR_REPORT_ENV        - "dev" or "prod"
#
# Optional env vars:
#   EDR_MONITOR_ENV       - set to enable edr monitor (e.g., "dev" or "prod")
#   SLACK_WEBHOOK         - Slack webhook URL for edr monitor alerts
#   OBSERVABILITY_ARTIFACT_MODE - "none" (default) or "s3"
#   MANDATORY_STEPS       - comma-separated list of step names that must pass
#                           for exit 0 (default: "dbt-run,edr-report")
#
# The script never exits early — it always completes all steps, then reports
# a summary and exits non-zero if any mandatory step failed.

PROFILES_DIR="${PROFILES_DIR:-/home/appuser/.dbt}"
PROJECT_DIR="${PROJECT_DIR:-/app}"
REPORT_PATH="${PROJECT_DIR}/reports/elementary_report.html"
EDR_TARGET="${PROJECT_DIR}/edr_target"
RUNTIME_DATA_DIR="${RUNTIME_DATA_DIR:-/data}"
SEMANTIC_METRICS_DIR="${SEMANTIC_METRICS_DIR:-${RUNTIME_DATA_DIR}/metrics}"
SEMANTIC_BUILD_SUMMARY_PATH="${PROJECT_DIR}/target/semantic_build_summary.json"
SEMANTIC_BUILD_METRICS_PATH="${PROJECT_DIR}/target/semantic_build_metrics.prom"

# Default mandatory steps (preview). Prod wrapper overrides this.
MANDATORY_STEPS="${MANDATORY_STEPS:-dbt-run,edr-report}"

mkdir -p "$SEMANTIC_METRICS_DIR"

declare -A step_exit_codes
step_results=()

run_step() {
  local name="$1"; shift
  echo "[$(date -u)] Starting: $name"
  "$@"
  local rc=$?
  step_exit_codes["$name"]=$rc
  if [ $rc -eq 0 ]; then
    step_results+=("$name=PASS")
    echo "[$(date -u)] Completed: $name"
  else
    step_results+=("$name=FAIL(rc=$rc)")
    echo "[$(date -u)] Failed: $name (exit $rc)"
  fi
  return $rc
}

# ── 0. Clean orphaned tmp tables from previous crashed runs ──────────────
run_step "cleanup-tmp-tables" \
  dbt run-operation clean_elementary_orphaned_tables \
  --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" \
  || true

run_step "cleanup-dbt-trash" \
  dbt run-operation drop_dbt_trash --args '{"database_name": "dbt"}' \
  --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" \
  || true

run_step "kill-failed-mutations" \
  dbt run-operation kill_failed_mutations \
  --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" \
  || true

# ── 1. Source freshness ──────────────────────────────────────────────────
# Note: Elementary's on_run_end hook automatically uploads freshness results
# to the elementary schema — no separate edr upload step needed.
run_step "source-freshness" \
  dbt source freshness --select source:* \
  --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" \
  || true

# ── 2. Main pipeline ────────────────────────────────────────────────────
run_step "dbt-run" \
  dbt run --select tag:production \
  --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" \
  || true

# ── 3. Tests (batched to stay under ClickHouse max_table_num_to_throw) ──
# Elementary's on_run_end hook creates temp tables per test result.
# Running all 900+ tests in one shot exceeds the 1000-table limit.
# Batching by model layer keeps temp table count manageable; each batch
# cleans up its temp tables via on_run_end before the next batch starts.

for test_batch in \
  "tag:production,resource_type:source" \
  "tag:production,path:models/consensus/staging" \
  "tag:production,path:models/execution/staging" \
  "tag:production,path:models/p2p/staging" \
  "tag:production,path:models/consensus/intermediate" \
  "tag:production,path:models/execution/intermediate" \
  "tag:production,path:models/bridges" \
  "tag:production,path:models/contracts" \
  "tag:production,path:models/consensus/marts" \
  "tag:production,path:models/execution/marts" \
  "tag:production,path:models/p2p/marts" \
  "tag:production,path:models/probelab" \
  "tag:production,path:models/crawlers_data" \
  "tag:production,path:models/ESG" \
; do
  batch_name="dbt-test:$(echo "$test_batch" | sed 's/tag:production,//')"
  run_step "$batch_name" \
    dbt test --select "$test_batch" \
    --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" \
    || true
done

# ── 4. Semantic docs and registry artifacts ──────────────────────────────
run_step "dbt-docs" \
  dbt docs generate \
  --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" \
  || true

run_step "semantic-registry" \
  python "$PROJECT_DIR/scripts/semantic/build_registry.py" --target-dir "$PROJECT_DIR/target" \
  || true

run_step "semantic-docs" \
  python "$PROJECT_DIR/scripts/semantic/build_semantic_docs.py" --target-dir "$PROJECT_DIR/target" \
  || true

if [ -f "$SEMANTIC_BUILD_METRICS_PATH" ]; then
  cp "$SEMANTIC_BUILD_METRICS_PATH" "$SEMANTIC_METRICS_DIR/semantic_build_metrics.prom"
fi

# ── 5. Elementary monitor (only when webhook + env are set) ──────────────
if [ -n "$SLACK_WEBHOOK" ] && [ -n "$EDR_MONITOR_ENV" ]; then
  run_step "edr-monitor" \
    edr monitor \
    --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" \
    --env "$EDR_MONITOR_ENV" --group-by table \
    --suppression-interval 24 \
    || true
fi

# ── 6. Elementary report (always) ────────────────────────────────────────
run_step "edr-report" \
  edr report \
  --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" \
  --env "${EDR_REPORT_ENV:-dev}" \
  --file-path "$REPORT_PATH" \
  --target-path "$EDR_TARGET" \
  || true

# ── Summary ──────────────────────────────────────────────────────────────
echo ""
echo "[$(date -u)] Run complete. Results: ${step_results[*]}"

# Determine exit code based on mandatory steps
overall_exit=0
IFS=',' read -ra MANDATORY <<< "$MANDATORY_STEPS"
for step in "${MANDATORY[@]}"; do
  # dbt-test is batched — check all steps starting with "dbt-test:"
  if [ "$step" = "dbt-test" ]; then
    batch_found=false
    for key in "${!step_exit_codes[@]}"; do
      if [[ "$key" == dbt-test:* ]]; then
        batch_found=true
        if [ "${step_exit_codes[$key]}" -ne 0 ]; then
          echo "[$(date -u)] MANDATORY STEP FAILED: $key (exit ${step_exit_codes[$key]})"
          overall_exit=1
        fi
      fi
    done
    if [ "$batch_found" = false ]; then
      echo "[$(date -u)] WARNING: no dbt-test batches were executed"
      overall_exit=1
    fi
    continue
  fi

  rc="${step_exit_codes[$step]:-}"
  if [ -z "$rc" ]; then
    # Step was not run (e.g., edr-monitor skipped) — only fail if it was mandatory
    if [ "$step" = "edr-monitor" ] && [ -z "$EDR_MONITOR_ENV" ]; then
      continue  # monitor is optional when env is not set
    fi
    echo "[$(date -u)] WARNING: mandatory step '$step' was not executed"
    overall_exit=1
  elif [ "$rc" -ne 0 ]; then
    echo "[$(date -u)] MANDATORY STEP FAILED: $step (exit $rc)"
    overall_exit=1
  fi
done

exit $overall_exit
