-- Freshness monitor for the full `tag:live` chain, not just the raw decode
-- layer: 4 raw contract-decode tables (refreshed by live.sh's 45s
-- `dbt run --select tag:live` loop) + int_live__dex_trades_raw (the unified
-- staging->intermediate table behind the api_execution_live_trades* marts,
-- refreshed on its own ~5min cadence per its own model description, owing to
-- a heavier 2h delete+insert self-heal) + api_execution_live_trades_freshness
-- (a dedicated raw-source ingestion-lag view already built for this purpose —
-- api_execution_live_trades.sql's own docstring says "For ingestion-level
-- staleness, query api_execution_live_trades_freshness separately").
--
-- Deliberately NOT checking the 4 stg_live__dex_trades_* staging models or
-- the api_execution_live_trades / _hourly_48h / _stats marts: all are plain
-- `materialized='view'` with zero independent refresh lag beyond
-- int_live__dex_trades_raw, so checking that one table covers all of them.
--
-- Design intent: a 45s dbt loop with a 2h source TTL and a 30min self-heal
-- window. Nothing today alerts if the loop silently stalls (K8s deployment
-- has no liveness probe, and it isn't covered by the dbt-cerebro
-- PodMonitor). elementary.freshness_anomalies is a seasonal anomaly
-- detector, not a fit for a 45s-cadence table, so this is a plain
-- hard-threshold check instead.
--
-- Threshold: 60 minutes by default (var override), generous relative to
-- both observed cadences (45s and ~5min) while still catching a real stall
-- promptly rather than the original 3h. Returns offending
-- tables/rows; passing = zero rows.
--
-- NOTE: not yet wired into run_dbt_observability.sh's build_test_batches() —
-- none of the existing `tag:production,path:models/...` batches match a
-- standalone tests/*.sql file with no path under models/. Needs an explicit
-- batch entry (or a tag this test carries plus a matching selector) added to
-- that shared script to run automatically in the daily/preview cron; until
-- then, run manually with `dbt test --select contracts_live_tables_freshness`.






SELECT
    'contracts_UniswapV3_Pool_events_live' AS table_name
    ,max(block_timestamp) AS max_block_timestamp
    ,dateDiff('minute', max(block_timestamp), now()) AS minutes_stale
FROM `dbt`.`contracts_UniswapV3_Pool_events_live`
HAVING minutes_stale > 60
UNION ALL

SELECT
    'contracts_BalancerV2_Vault_events_live' AS table_name
    ,max(block_timestamp) AS max_block_timestamp
    ,dateDiff('minute', max(block_timestamp), now()) AS minutes_stale
FROM `dbt`.`contracts_BalancerV2_Vault_events_live`
HAVING minutes_stale > 60
UNION ALL

SELECT
    'contracts_BalancerV3_Vault_events_live' AS table_name
    ,max(block_timestamp) AS max_block_timestamp
    ,dateDiff('minute', max(block_timestamp), now()) AS minutes_stale
FROM `dbt`.`contracts_BalancerV3_Vault_events_live`
HAVING minutes_stale > 60
UNION ALL

SELECT
    'contracts_Swapr_v3_AlgebraPool_events_live' AS table_name
    ,max(block_timestamp) AS max_block_timestamp
    ,dateDiff('minute', max(block_timestamp), now()) AS minutes_stale
FROM `dbt`.`contracts_Swapr_v3_AlgebraPool_events_live`
HAVING minutes_stale > 60
UNION ALL

SELECT
    'int_live__dex_trades_raw' AS table_name
    ,max(block_timestamp) AS max_block_timestamp
    ,dateDiff('minute', max(block_timestamp), now()) AS minutes_stale
FROM `dbt`.`int_live__dex_trades_raw`
HAVING minutes_stale > 60
UNION ALL


SELECT
    'api_execution_live_trades_freshness' AS table_name
    ,newest_block_timestamp AS max_block_timestamp
    ,intDiv(lag_seconds, 60) AS minutes_stale
FROM `dbt`.`api_execution_live_trades_freshness`
HAVING minutes_stale > 60