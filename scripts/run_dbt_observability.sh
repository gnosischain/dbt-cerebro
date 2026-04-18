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
#   DBT_TEST_SCOPE        - "full" (default) or "preview_subset"
#   DBT_RUN_BATCH_SLEEP_SECONDS - pause between generated dbt-run batches
#                                 (default: 0)
#
# The script never exits early — it always completes all steps, then reports
# a summary and exits non-zero if any mandatory step failed.

PROFILES_DIR="${PROFILES_DIR:-/home/appuser/.dbt}"
PROJECT_DIR="${PROJECT_DIR:-/app}"
REPORT_PATH="${PROJECT_DIR}/reports/elementary_report.html"
EDR_TARGET="${PROJECT_DIR}/edr_target"
RUNTIME_DATA_DIR="${RUNTIME_DATA_DIR:-/data}"
DBT_LOG_PATH="${DBT_LOG_PATH:-${RUNTIME_DATA_DIR}/logs}"
SEMANTIC_METRICS_DIR="${SEMANTIC_METRICS_DIR:-${RUNTIME_DATA_DIR}/metrics}"
SEMANTIC_BUILD_SUMMARY_PATH="${PROJECT_DIR}/target/semantic_build_summary.json"
SEMANTIC_BUILD_METRICS_PATH="${PROJECT_DIR}/target/semantic_build_metrics.prom"

# Default mandatory steps (preview). Prod wrapper overrides this.
MANDATORY_STEPS="${MANDATORY_STEPS:-dbt-run,edr-report}"

# Force orchestrator-driven dbt runs to use the writable runtime dir rather than
# bind-mounted /app/logs, which can be owned by a different host UID/GID.
export DBT_LOG_PATH

mkdir -p "$DBT_LOG_PATH"
mkdir -p "$SEMANTIC_METRICS_DIR"
mkdir -p "$(dirname "$REPORT_PATH")"

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

check_batched_step_prefix() {
  local prefix="$1"
  local found=false
  local key

  for key in "${!step_exit_codes[@]}"; do
    if [[ "$key" == "${prefix}:"* ]]; then
      found=true
      if [ "${step_exit_codes[$key]}" -ne 0 ]; then
        echo "[$(date -u)] MANDATORY STEP FAILED: $key (exit ${step_exit_codes[$key]})"
        overall_exit=1
      fi
    fi
  done

  if [ "$found" = false ]; then
    echo "[$(date -u)] WARNING: no ${prefix} batches were executed"
    overall_exit=1
  fi
}

build_test_batches() {
  test_batches=()

  case "$DBT_TEST_SCOPE" in
    full)
      test_batches=(
        "tag:production,resource_type:source"
        "tag:production,path:models/consensus/staging"
        "tag:production,path:models/execution/staging"
        "tag:production,path:models/p2p/staging"
        "tag:production,path:models/consensus/intermediate"
        "tag:production,path:models/execution/intermediate"
        "tag:production,path:models/bridges"
        "tag:production,path:models/contracts"
        "tag:production,path:models/consensus/marts"
        "tag:production,path:models/execution/marts"
        "tag:production,path:models/p2p/marts"
        "tag:production,path:models/probelab"
        "tag:production,path:models/crawlers_data"
        "tag:production,path:models/ESG"
      )
      ;;
    preview_subset)
      local api_model_path
      local rel_dir
      declare -A seen_api_dirs=()

      test_batches=(
        "tag:production,resource_type:source"
        "tag:production,path:models/crawlers_data"
        "tag:production,path:models/contracts"
      )

      while IFS= read -r api_model_path; do
        [ -n "$api_model_path" ] || continue
        rel_dir="${api_model_path#"$PROJECT_DIR/"}"
        rel_dir="${rel_dir%/*}"

        if [ -z "${seen_api_dirs[$rel_dir]:-}" ]; then
          test_batches+=("tag:production,path:${rel_dir},api_*")
          seen_api_dirs["$rel_dir"]=1
        fi
      done < <(find "$PROJECT_DIR/models" -type f -path '*/marts/api_*.sql' -print | LC_ALL=C sort)
      ;;
    *)
      echo "[$(date -u)] Unknown DBT_TEST_SCOPE: $DBT_TEST_SCOPE"
      return 64
      ;;
  esac
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

# ── 1b. Circles avatar IPFS metadata fetch ───────────────────────────────
# Refresh the deterministic queue view, then fetch any unresolved
# (avatar, metadata_digest) pairs from the IPFS gateway via the Python
# backfill script. The script handles concurrency (30 workers), per-row
# error handling (failures are persisted as rows with http_status != 200
# so they are skipped on subsequent runs via the LEFT ANTI JOIN), and
# gateway fallback across 6 distinct public gateways.
#
# This replaces the previous `fetch_and_insert_circles_metadata` dbt
# run-operation, which serialized everything through ClickHouse `url()`,
# retried bad CIDs internally for 5–10 minutes, and aborted the entire
# run on the first failure — leaving dead "no providers" CIDs to clog
# the queue forever because failures were never persisted.
run_step "circles-metadata-targets" \
  dbt run --select int_execution_circles_v2_avatar_metadata_targets \
  --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" \
  || true

run_step "circles-metadata-fetch" \
  python "$PROJECT_DIR/scripts/circles/backfill_avatar_metadata.py" \
    --concurrency 30 \
    --max-retries 1 \
    --request-timeout 15 \
  || true

# ── 2. Main pipeline ────────────────────────────────────────────────────
# Batch the current production selection automatically from the dbt graph.
# Batches are built from complete runnable chains, then grouped by chain count.
DBT_RUN_BATCH_SIZE="${DBT_RUN_BATCH_SIZE:-5}"
DBT_RUN_BATCH_SLEEP_SECONDS="${DBT_RUN_BATCH_SLEEP_SECONDS:-0}"
RUN_BATCH_PLAN="$(mktemp)"

# Fresh stash for this run — stale files from previous days would bleed into
# today's transient/permanent classification.
FAILED_BATCHES_DIR="${PROJECT_DIR}/target/failed_batches"
rm -rf "$FAILED_BATCHES_DIR"

if python "$PROJECT_DIR/scripts/refresh/dbt_run_batches.py" \
  --select tag:production \
  --batch-size "$DBT_RUN_BATCH_SIZE" \
  --project-dir "$PROJECT_DIR" \
  --profiles-dir "$PROFILES_DIR" > "$RUN_BATCH_PLAN"
then
  mapfile -t run_batches < "$RUN_BATCH_PLAN"
  for batch_index in "${!run_batches[@]}"; do
    IFS=$'\t' read -r batch_id batch_count chain_count batch_selector <<< "${run_batches[$batch_index]}"
    echo "[$(date -u)] dbt-run batch ${batch_id} (${batch_count} model(s), ${chain_count} chain(s)): ${batch_selector}"

    run_step "dbt-run:${batch_id}" \
      dbt run --select "$batch_selector" \
      --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" \
      || true

    # Stash per-batch run_results.json before the next batch overwrites it.
    if [ "${step_exit_codes["dbt-run:${batch_id}"]}" -ne 0 ] \
       && [ -f "${PROJECT_DIR}/target/run_results.json" ]; then
      mkdir -p "$FAILED_BATCHES_DIR"
      cp "${PROJECT_DIR}/target/run_results.json" \
         "${FAILED_BATCHES_DIR}/${batch_id}.json"
    fi

    if [ "$DBT_RUN_BATCH_SLEEP_SECONDS" -gt 0 ] && [ "$batch_index" -lt "$(( ${#run_batches[@]} - 1 ))" ]; then
      echo "[$(date -u)] Sleeping ${DBT_RUN_BATCH_SLEEP_SECONDS}s before next dbt-run batch"
      sleep "$DBT_RUN_BATCH_SLEEP_SECONDS"
    fi
  done
else
  plan_rc=$?
  step_exit_codes["dbt-run:plan"]=$plan_rc
  step_results+=("dbt-run:plan=FAIL(rc=$plan_rc)")
  echo "[$(date -u)] Failed: dbt-run:plan (exit $plan_rc)"
fi

rm -f "$RUN_BATCH_PLAN"

# ── 2b. Smart retry for transient ClickHouse errors ──────────────────────
# Classify failed nodes by error code. Memory-limit / timeout / network
# errors are worth a single low-concurrency retry; SQL bugs are not.
if [ -d "$FAILED_BATCHES_DIR" ]; then
  CLASSIFY_OUT="$(python "$PROJECT_DIR/scripts/refresh/classify_failed_nodes.py" \
    --stash-dir "$FAILED_BATCHES_DIR" 2>&1)"
  echo "$CLASSIFY_OUT"
  TRANSIENT_LINE="$(echo "$CLASSIFY_OUT" | grep '^TRANSIENT=' || true)"
  PERMANENT_LINE="$(echo "$CLASSIFY_OUT" | grep '^PERMANENT=' || true)"
  TRANSIENT_IDS="${TRANSIENT_LINE#TRANSIENT=}"
  PERMANENT_IDS="${PERMANENT_LINE#PERMANENT=}"

  if [ -n "$TRANSIENT_IDS" ]; then
    # dbt's --select accepts fqn selectors; unique_ids look like
    # "model.project.name" — strip to "name+" for descendants.
    retry_selector=""
    for uid in $TRANSIENT_IDS; do
      node_name="${uid##*.}"
      retry_selector="$retry_selector $node_name+"
    done
    echo "[$(date -u)] Retrying transient failures: $retry_selector"
    run_step "dbt-run:retry-transient" \
      dbt run --select $retry_selector --threads 1 \
      --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" \
      || true

    # Flip original batch exit codes for nodes that the retry recovered.
    if [ -f "${PROJECT_DIR}/target/run_results.json" ]; then
      RECOVERED="$(python -c "
import json, sys
data = json.load(open('${PROJECT_DIR}/target/run_results.json'))
ok = [r['unique_id'] for r in data.get('results', [])
      if r.get('status') == 'success']
print(' '.join(ok))
" 2>/dev/null || echo "")"
      if [ -n "$RECOVERED" ]; then
        # Remove recovered nodes from the per-batch failure stash so the
        # summary check sees only still-failing nodes.
        python - "$FAILED_BATCHES_DIR" "$RECOVERED" <<'PYEOF'
import json, sys
from pathlib import Path

stash_dir = Path(sys.argv[1])
recovered = set(sys.argv[2].split())
for path in stash_dir.glob("*.json"):
    data = json.loads(path.read_text())
    still_failed = [
        r for r in data.get("results", [])
        if not (r.get("status") == "error" and r.get("unique_id") in recovered)
    ]
    remaining_errors = [r for r in still_failed if r.get("status") == "error"]
    if not remaining_errors:
        path.unlink()
    else:
        data["results"] = still_failed
        path.write_text(json.dumps(data))
PYEOF
        # If every stashed batch was fully recovered, clear its mandatory-fail flag.
        for key in "${!step_exit_codes[@]}"; do
          if [[ "$key" == "dbt-run:"* ]] && [[ "$key" != "dbt-run:retry-transient" ]]; then
            batch_id="${key#dbt-run:}"
            if [ ! -f "${FAILED_BATCHES_DIR}/${batch_id}.json" ] \
               && [ "${step_exit_codes[$key]}" -ne 0 ]; then
              echo "[$(date -u)] Retry recovered batch: $key"
              step_exit_codes["$key"]=0
            fi
          fi
        done
      fi
    fi
  fi

  if [ -n "$PERMANENT_IDS" ]; then
    echo "[$(date -u)] Permanent (non-retryable) failures: $PERMANENT_IDS"
  fi
fi

# ── 3. Tests (batched to stay under ClickHouse max_table_num_to_throw) ──
# Elementary's on_run_end hook creates temp tables per test result.
# Running all 900+ tests in one shot exceeds the 1000-table limit.
# Batching by model layer keeps temp table count manageable; each batch
# cleans up its temp tables via on_run_end before the next batch starts.
#
# TEST_MODE controls the test_recency_filter macro behavior:
#   "daily" (default) — not_null/unique tests scan only the last 7 days
#   "full"            — all tests scan the full table (weekly runs)

TEST_MODE="${TEST_MODE:-daily}"
DBT_TEST_SCOPE="${DBT_TEST_SCOPE:-full}"
if [ "$TEST_MODE" = "full" ]; then
  DBT_TEST_VARS='--vars {test_full_refresh: true}'
else
  DBT_TEST_VARS=""
fi

if build_test_batches; then
  for test_batch in "${test_batches[@]}"; do
    batch_name="dbt-test:${test_batch#tag:production,}"
    run_step "$batch_name" \
      dbt test --select "$test_batch" \
      $DBT_TEST_VARS \
      --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" \
      || true
  done
else
  plan_rc=$?
  step_exit_codes["dbt-test:plan"]=$plan_rc
  step_results+=("dbt-test:plan=FAIL(rc=$plan_rc)")
  echo "[$(date -u)] Failed: dbt-test:plan (exit $plan_rc)"
fi

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
  if [ "$step" = "dbt-test" ] || [ "$step" = "dbt-run" ]; then
    check_batched_step_prefix "$step"
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
