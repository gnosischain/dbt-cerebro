#!/bin/bash
set -e

echo "[$(date -u)] Starting dbt production run"
dbt run --select tag:production

echo "[$(date -u)] Running dbt tests"
dbt test --select tag:production

echo "[$(date -u)] Generating Elementary report"
edr report \
  --profiles-dir /home/appuser/.dbt \
  --project-dir /app \
  --file-path /app/reports/elementary_report.html \
  --target-path /app/edr_target || echo "[$(date -u)] WARNING: edr report failed"

echo "[$(date -u)] Cron run complete"
