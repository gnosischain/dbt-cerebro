#!/bin/bash
# Production cron wrapper — sets prod defaults and delegates to orchestrator.
export EDR_REPORT_ENV=prod
export EDR_MONITOR_ENV=prod
export MANDATORY_STEPS="dbt-run,dbt-test,source-freshness,edr-report,edr-monitor"
exec /app/scripts/run_dbt_observability.sh
