#!/bin/bash
# Retired AWS production wrapper. No cluster runs this: the GKE CronJob runs
# cron_preview.sh (see infrastructure-gnosis-analytics-deployments/google/deployments/
# gnosis-analytics/dbt-cerebro/configmap.tf). Kept so the orchestrator's contract has a
# second worked example; edr-* steps left the mandatory list when Elementary was switched
# off (2026-09-15).
export EDR_REPORT_ENV=prod
export MANDATORY_STEPS="dbt-run,dbt-test,source-freshness"
exec /app/scripts/run_dbt_observability.sh
