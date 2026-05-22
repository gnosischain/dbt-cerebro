#!/bin/bash
# Continuous live pipeline loop — runs every 45 seconds.
# On first start it does a full-refresh of all live models (bootstraps the
# incremental contract event tables), then runs incrementally on each cycle.

# profiles.yml reads CLICKHOUSE_DATABASE; K8s sets CLICKHOUSE_SCHEMA
export CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-$CLICKHOUSE_SCHEMA}"

CYCLE_SECONDS=45

echo "[live] Starting bootstrap (full-refresh)..."
dbt run --select tag:live --threads 4 --full-refresh
echo "[live] Bootstrap complete. Entering incremental loop."

while true; do
    START=$(date +%s)
    dbt run --select tag:live --threads 4
    END=$(date +%s)
    ELAPSED=$((END - START))
    SLEEP=$((CYCLE_SECONDS - ELAPSED))
    [ "$SLEEP" -gt 0 ] && sleep "$SLEEP"
done
