#!/bin/bash
# Preview cron wrapper — sets dev defaults and delegates to orchestrator.
export EDR_REPORT_ENV=dev
export MANDATORY_STEPS="dbt-run,edr-report"
export DBT_RUN_BATCH_SLEEP_SECONDS="${DBT_RUN_BATCH_SLEEP_SECONDS:-10}"
export DBT_TEST_SCOPE="${DBT_TEST_SCOPE:-preview_subset}"
# Monitor only runs if SLACK_WEBHOOK is present
[ -n "$SLACK_WEBHOOK" ] && export EDR_MONITOR_ENV=dev
exec /app/scripts/run_dbt_observability.sh
