

SELECT sub.*, (SELECT toDate(max(block_timestamp)) FROM `dbt`.`int_execution_circles_v2_avatars`) AS as_of_date
FROM (
-- Snapshot of network-level Circles v2 counts: avatars (total + by type),
-- active trusts, tokens, wrappers. Thin passthrough over
-- fct_execution_circles_v2_stats_current.

SELECT
    measure,
    value
FROM `dbt`.`fct_execution_circles_v2_stats_current`
ORDER BY measure
) AS sub