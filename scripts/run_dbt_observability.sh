#!/bin/bash
# Shared dbt observability orchestrator.
#
# Called by cron_preview.sh (the GKE CronJob's entrypoint; the "preview" name is the
# AWS-era stack name it inherited) after it sets environment defaults. cron.sh is the
# retired AWS production wrapper.
#
# Required env vars:
#   EDR_REPORT_ENV        - "dev" or "prod"
#
# Optional env vars:
#   ELEMENTARY_ENABLED    - "1" to run the Elementary steps (orphan-table cleanup,
#                           artifact-catalog refresh, edr monitor, edr report) and the
#                           Elementary anomaly/schema tests. Default "0": off since
#                           2026-09-15 — the result tables were never created on the GCP
#                           warehouse and the dbt-run Slack alarms are Grafana-managed.
#   EDR_MONITOR_ENV       - with ELEMENTARY_ENABLED=1, set to enable edr monitor
#   SLACK_WEBHOOK         - with ELEMENTARY_ENABLED=1, webhook for edr monitor alerts
#   MANDATORY_STEPS       - comma-separated list of step names that must pass
#                           for exit 0 (default: "dbt-run")
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

# Default mandatory steps. edr-report left this list when Elementary was switched off.
MANDATORY_STEPS="${MANDATORY_STEPS:-dbt-run}"
ELEMENTARY_ENABLED="${ELEMENTARY_ENABLED:-0}"
# With Elementary off (dbt_project.yml: elementary +enabled: false) its ~960 anomaly/schema
# tests compile to no-ops, but they would still be scheduled: they are declared in this
# project's schema files (package gnosis_dbt, only the macro is Elementary's) and the batches
# reach them indirectly through their models. Exclude them by test name so they cost nothing.
if [ "$ELEMENTARY_ENABLED" = "1" ]; then
  ELEMENTARY_TEST_EXCLUDE=""
else
  ELEMENTARY_TEST_EXCLUDE="--exclude test_name:volume_anomalies test_name:freshness_anomalies test_name:column_anomalies test_name:schema_changes"
fi

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
        "path:seeds"
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
        "tag:production,path:models/rpc_state_indexer"
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
        "path:seeds"
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
if [ "$ELEMENTARY_ENABLED" = "1" ]; then
  run_step "cleanup-tmp-tables" \
    dbt run-operation clean_elementary_orphaned_tables \
    --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" \
    || true
fi

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

if [ "${DBT_RUN_SINGLE_PARSE:-0}" = "1" ]; then
  # Single-parse path (cron preview): ONE runner invocation over the whole
  # selection. dbt_incremental_runner topo-orders plain + microbatch models
  # itself and parses the project once (in-process, reused for every plain model
  # AND every slice) instead of spawning a re-parsing process per batch. It
  # stashes per-node failures to target/failed_batches/, so the classify +
  # smart-retry block below is unchanged. DBT_RUN_BATCH_SLEEP_SECONDS becomes the
  # inter-slice/flush delay.
  single_parse_args=()
  if [ -n "${MICROBATCH_BOOTSTRAP_LOOKBACK_DAYS:-}" ]; then
    single_parse_args+=(--bootstrap-lookback-days "$MICROBATCH_BOOTSTRAP_LOOKBACK_DAYS")
  fi
  if [ -n "${MICROBATCH_MAX_END_DATE:-}" ]; then
    single_parse_args+=(--max-end-date "$MICROBATCH_MAX_END_DATE")
  fi
  if [ "$DBT_RUN_BATCH_SLEEP_SECONDS" -gt 0 ]; then
    single_parse_args+=(--delay "$DBT_RUN_BATCH_SLEEP_SECONDS")
  fi
  echo "[$(date -u)] dbt-run single-parse: one runner invocation over tag:production"
  run_step "dbt-run:all" \
    python "$PROJECT_DIR/scripts/refresh/dbt_incremental_runner.py" \
      --select tag:production \
      --project-dir "$PROJECT_DIR" --profiles-dir "$PROFILES_DIR" \
      "${single_parse_args[@]}" \
    || true
elif python "$PROJECT_DIR/scripts/refresh/dbt_run_batches.py" \
  --select tag:production \
  --batch-size "$DBT_RUN_BATCH_SIZE" \
  --project-dir "$PROJECT_DIR" \
  --profiles-dir "$PROFILES_DIR" > "$RUN_BATCH_PLAN"
then
  mapfile -t run_batches < "$RUN_BATCH_PLAN"
  for batch_index in "${!run_batches[@]}"; do
    IFS=$'\t' read -r batch_id batch_count chain_count batch_selector <<< "${run_batches[$batch_index]}"
    echo "[$(date -u)] dbt-run batch ${batch_id} (${batch_count} model(s), ${chain_count} chain(s)): ${batch_selector}"

    # Microbatch runner is a strict superset of `dbt run`: for plain models in
    # the selector it issues a single passthrough invocation, and for models
    # annotated with meta.full_refresh.incremental.enabled it slices the load
    # into bounded per-day windows that resolve to `incremental_strategy=append`
    # — eliminating ALTER ... DELETE mutations on the daily path. See
    # scripts/refresh/dbt_incremental_runner.py.
    microbatch_extra_args=()
    if [ -n "${MICROBATCH_BOOTSTRAP_LOOKBACK_DAYS:-}" ]; then
      microbatch_extra_args+=(--bootstrap-lookback-days "$MICROBATCH_BOOTSTRAP_LOOKBACK_DAYS")
    fi
    if [ -n "${MICROBATCH_MAX_END_DATE:-}" ]; then
      microbatch_extra_args+=(--max-end-date "$MICROBATCH_MAX_END_DATE")
    fi
    run_step "dbt-run:${batch_id}" \
      python "$PROJECT_DIR/scripts/refresh/dbt_incremental_runner.py" \
        --select "$batch_selector" \
        --project-dir "$PROJECT_DIR" --profiles-dir "$PROFILES_DIR" \
        "${microbatch_extra_args[@]}" \
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
    # Retry policy. The old behaviour was ONE pass over `<model>+` for every
    # transient failure at once, run immediately after the batch that failed.
    # Two consequences, both seen on 2026-09-22:
    #   * No backoff. A Code 241 means the SERVER is out of memory; retrying
    #     seconds later hits the same saturated server. Both models failed again
    #     within 59s and were left failed for the night.
    #   * One batch. Three parents and their 62 descendants went in a single
    #     invocation, so when two parents failed dbt skipped all 58 children --
    #     including 34 user-facing api marts (28 revenue, 6 HOPR) whose OTHER
    #     parent had recovered fine.
    # Now: retry the PARENTS only, several times, waiting longer each round; then
    # build descendants for whichever parents came back. A stubborn model no
    # longer takes its siblings' subtrees down with it.
    # Of the given model names, print those that did NOT succeed in the most recent
    # `dbt run` (target/run_results.json). Fails CLOSED: if the results cannot be read,
    # every name is reported still failing. Returning "nothing is still failing" on an
    # error would mark broken models recovered and then build descendants on top of them.
    still_failing_in_last_run() {
      python - "$1" "$PROJECT_DIR" <<'PYSTILL'
import json, sys
from pathlib import Path

pending = set(sys.argv[1].split())
results = Path(sys.argv[2]) / 'target' / 'run_results.json'
try:
    data = json.loads(results.read_text())
    ok = {r['unique_id'].split('.')[-1]
          for r in data.get('results', []) if r.get('status') == 'success'}
except Exception:
    ok = set()
print(' '.join(sorted(pending - ok)))
PYSTILL
    }

    RETRY_ATTEMPTS="${RETRY_ATTEMPTS:-3}"
    RETRY_BACKOFF_SECONDS="${RETRY_BACKOFF_SECONDS:-300}"

    pending=""
    for uid in $TRANSIENT_IDS; do
      pending="$pending ${uid##*.}"
    done
    recovered_parents=""
    retry_rc=0
    attempt=1

    while [ -n "$(echo "$pending" | tr -d ' ')" ] && [ "$attempt" -le "$RETRY_ATTEMPTS" ]; do
      if [ "$attempt" -gt 1 ]; then
        wait_s=$(( RETRY_BACKOFF_SECONDS * (attempt - 1) ))
        echo "[$(date -u)] Waiting ${wait_s}s before retry attempt ${attempt} (memory pressure needs time to clear)"
        sleep "$wait_s"
      fi
      echo "[$(date -u)] Retry attempt ${attempt}/${RETRY_ATTEMPTS} for:$pending"
      dbt run --select $pending --threads 1 \
        --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" || true

      still_failing="$(still_failing_in_last_run "$pending")" || still_failing="$pending"
      if [ -z "${still_failing+x}" ]; then still_failing="$pending"; fi
      for n in $pending; do
        case " $still_failing " in *" $n "*) ;; *) recovered_parents="$recovered_parents $n" ;; esac
      done
      pending="$still_failing"
      attempt=$(( attempt + 1 ))
    done

    if [ -n "$(echo "$pending" | tr -d ' ')" ]; then
      echo "[$(date -u)] Still failing after ${RETRY_ATTEMPTS} attempts:$pending"
    fi

    # Build descendants of the parents that came back, so a sibling's failure
    # does not strand them. `<name>+` includes the parent, which is a no-op here.
    desc_rc=0
    if [ -n "$(echo "$recovered_parents" | tr -d ' ')" ]; then
      desc_selector=""
      for n in $recovered_parents; do
        desc_selector="$desc_selector ${n}+"
      done
      echo "[$(date -u)] Building descendants of recovered parents:$desc_selector"
      dbt run --select $desc_selector --threads 1 \
        --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" || desc_rc=1

      # This run can rebuild a model the ladder gave up on: when a still-failing model
      # is a CHILD of a parent that recovered, `<parent>+` builds it again, by which
      # time the warehouse has often cleared. So decide the verdict only now, against
      # THIS run's results. The verdict used to be latched before this step: on
      # 2026-09-25 int_execution_gnosis_app_user_events failed all three attempts, then
      # built here at 09:24, and the job still exited 1 on a FAIL fixed at 09:17.
      if [ -n "$(echo "$pending" | tr -d ' ')" ]; then
        rescued_from="$pending"
        pending="$(still_failing_in_last_run "$pending")" || pending="$rescued_from"
        for n in $rescued_from; do
          case " $pending " in *" $n "*) ;; *)
            echo "[$(date -u)] Rescued by the descendants build: $n" ;;
          esac
        done
      fi
    fi

    # Fail only on what is genuinely left: a model no attempt could build, or a real
    # failure in the descendants build itself.
    retry_rc=0
    if [ -n "$(echo "$pending" | tr -d ' ')" ] || [ "$desc_rc" -ne 0 ]; then
      retry_rc=1
    fi
    if [ -n "$(echo "$pending" | tr -d ' ')" ]; then
      echo "[$(date -u)] Unrecovered after the ladder and the descendants build:$pending"
    fi

    step_exit_codes["dbt-run:retry-transient"]=$retry_rc
    if [ "$retry_rc" -eq 0 ]; then
      step_results+=("dbt-run:retry-transient=PASS")
      echo "[$(date -u)] Completed: dbt-run:retry-transient"
    else
      step_results+=("dbt-run:retry-transient=FAIL(rc=$retry_rc)")
      echo "[$(date -u)] Failed: dbt-run:retry-transient (exit $retry_rc)"
    fi

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

# ── 2c. Per-model run status metrics (Prometheus textfile) ───────────────
# Parse the accumulated dbt log for terminal per-model status and diff it
# against the production model set in the manifest. Emits a *.prom file that
# the observability server exposes via /metrics, so Grafana can show completed
# vs pending models and a real progress ratio instead of raw-log grep. Runs
# after the smart-retry so recovered models report success. Never fatal.
run_step "model-status-metrics" \
  python "$PROJECT_DIR/scripts/observability/emit_model_status_metrics.py" \
    --manifest "$PROJECT_DIR/target/manifest.json" \
    --log "$DBT_LOG_PATH/dbt.log" \
    --out "$SEMANTIC_METRICS_DIR/dbt_model_status.prom" \
  || true

# ── 2d. Elementary artifact catalog refresh (exactly once per run) ───────
# Artifact autoupload is disabled globally (dbt_project.yml
# disable_dbt_artifacts_autoupload=true) so the on-run-end hook stays cheap on the
# hundreds of run/slice invocations. Re-enable it for THIS single fast-view
# invocation so the Elementary catalog (dbt_models/dbt_tests/dbt_sources/...) is
# refreshed once, before edr report. upload_dbt_artifacts() needs `results`, so it
# must ride a real `dbt run` (a run-operation has none); a single view rebuild is
# cheap, idempotent, and always produces results. Override the selector with
# ARTIFACT_REFRESH_SELECTOR if this model is renamed.
if [ "$ELEMENTARY_ENABLED" = "1" ]; then
ARTIFACT_REFRESH_SELECTOR="${ARTIFACT_REFRESH_SELECTOR:-api_execution_live_trades_freshness}"
_artifact_uploads_before="$(grep -c 'Uploading dbt artifacts' "$DBT_LOG_PATH/dbt.log" 2>/dev/null)"
_artifact_uploads_before="${_artifact_uploads_before:-0}"
run_step "elementary-artifacts-refresh" \
  dbt run --select "$ARTIFACT_REFRESH_SELECTOR" \
  --vars '{"disable_dbt_artifacts_autoupload": false}' \
  --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" \
  || true
# Exactly-once guard: artifact upload is disabled on every other invocation, so it
# MUST have happened here. Elementary logs "Uploading dbt artifacts" (debug) to
# dbt.log; assert the count increased, else the catalog may be stale.
_artifact_uploads_after="$(grep -c 'Uploading dbt artifacts' "$DBT_LOG_PATH/dbt.log" 2>/dev/null)"
_artifact_uploads_after="${_artifact_uploads_after:-0}"
if [ "$_artifact_uploads_after" -le "$_artifact_uploads_before" ]; then
  echo "[$(date -u)] WARNING: elementary-artifacts-refresh uploaded no artifacts (before=$_artifact_uploads_before after=$_artifact_uploads_after, selector '$ARTIFACT_REFRESH_SELECTOR') — Elementary catalog may be stale"
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
      $ELEMENTARY_TEST_EXCLUDE \
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

# Standalone data-quality tests (tests/data_quality/*.sql) are NOT covered by
# the path:models/... batches above — they detect the failure classes in
# docs/lessons/ (dropped decoded logs, raw block holes, duplicate windows,
# sparse carry-forward series). Daily set every run; the heavier weekly set
# (raw-vs-decoded parity, density sweeps) rides the TEST_MODE=full run.
run_step "dbt-test:data-quality-daily" \
  dbt test --select tag:data_quality_daily --exclude tag:data_quality_weekly $ELEMENTARY_TEST_EXCLUDE \
  --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" \
  || true
if [ "$TEST_MODE" = "full" ]; then
  run_step "dbt-test:data-quality-weekly" \
    dbt test --select tag:data_quality_weekly $ELEMENTARY_TEST_EXCLUDE \
    --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" \
    || true
fi

# ── 4. Semantic docs and registry artifacts ──────────────────────────────
# Exclude dev-tagged models: prod builds only tag:production, so dev/WIP
# models are never materialised and a bare docs generate aborts on their
# missing tables. Keeps the catalog to the published (prod) surface.
run_step "dbt-docs" \
  dbt docs generate --exclude tag:dev \
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

# ── 5. Elementary monitor (only when enabled and webhook + env are set) ──
if [ "$ELEMENTARY_ENABLED" = "1" ] && [ -n "$SLACK_WEBHOOK" ] && [ -n "$EDR_MONITOR_ENV" ]; then
  run_step "edr-monitor" \
    edr monitor \
    --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" \
    --env "$EDR_MONITOR_ENV" --group-by table \
    --suppression-interval 24 \
    || true
fi

# ── 6. Elementary report (only when enabled) ─────────────────────────────
if [ "$ELEMENTARY_ENABLED" = "1" ]; then
  run_step "edr-report" \
    edr report \
    --profiles-dir "$PROFILES_DIR" --project-dir "$PROJECT_DIR" \
    --env "${EDR_REPORT_ENV:-dev}" \
    --file-path "$REPORT_PATH" \
    --target-path "$EDR_TARGET" \
    || true
fi

# ── Summary ──────────────────────────────────────────────────────────────
# step_results is appended by run_step at the moment a step finishes, so it still
# reads FAIL for a batch that dbt-run:retry-transient later recovered (the recovery
# flips step_exit_codes, which is what the exit code below actually uses). That made
# a fully-recovered run print "dbt-run:all=FAIL(rc=2)" on its last line while exiting
# 0 -- read on its own it looks like a failed run, and it was reported as one.
# Reconcile the display against the authoritative map before printing.
summary_results=()
for entry in "${step_results[@]}"; do
  entry_name="${entry%%=*}"
  if [[ "$entry" == *"=FAIL"* ]] && [ "${step_exit_codes[$entry_name]:-1}" -eq 0 ]; then
    summary_results+=("${entry_name}=PASS(recovered)")
  else
    summary_results+=("$entry")
  fi
done

echo ""
echo "[$(date -u)] Run complete. Results: ${summary_results[*]}"

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
