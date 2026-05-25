#!/bin/bash
# Continuous live pipeline loop — runs every 45 seconds.
# On first start it runs single-threaded to safely create any missing
# incremental tables, then switches to --threads 4 for the incremental loop.

# profiles.yml reads CLICKHOUSE_DATABASE; K8s sets CLICKHOUSE_SCHEMA
export CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-$CLICKHOUSE_SCHEMA}"

CYCLE_SECONDS=45

# Run single-threaded on startup so that if any incremental tables are missing,
# dbt creates them one at a time rather than 4 heavy decode_logs queries in
# parallel, which overwhelms the ClickHouse memory limit.
# On restarts the tables already exist, so this is just a fast incremental pass.
echo "[live] Starting bootstrap (threads=1)..."
dbt run --select tag:live --threads 1
echo "[live] Bootstrap complete. Entering incremental loop."

while true; do
    START=$(date +%s)
    dbt run --select tag:live --threads 4
    END=$(date +%s)
    ELAPSED=$((END - START))
    SLEEP=$((CYCLE_SECONDS - ELAPSED))
    [ "$SLEEP" -gt 0 ] && sleep "$SLEEP"
done
