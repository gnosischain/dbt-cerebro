

-- Gnosis App Weekly Economically Active Users — addresses that were both
-- active in a given week AND earned >= 1 Circles reward (cashback or
-- inviter fee) in that same week. Blacklist flag preserved.
--
-- The "active" side uses the Gnosis-App-only (in-app) weekly signals so that
-- WEAU is a strict subset of the headline Gnosis App WAU (Lineage A) and
-- WEAU/WAU is a clean activation-rate ratio. Earners are already GA-scoped.

WITH intersected AS (
    SELECT
        s.week,
        s.address
    FROM `dbt`.`int_execution_gnosis_app_weekly_signals_in_app` s
    INNER JOIN `dbt`.`int_execution_gnosis_app_weekly_earners` e
        ON e.week    = s.week
       AND e.address = s.address
),

flagged AS (
    SELECT
        i.week,
        i.address,
        b.address IS NOT NULL AS is_blacklisted
    FROM intersected i
    LEFT JOIN `dbt`.`stg_crawlers_data__circles_blacklisted` b
        ON b.address = i.address
)

SELECT
    week,
    is_blacklisted,
    count(DISTINCT address) AS cnt
FROM flagged
GROUP BY week, is_blacklisted
ORDER BY week, is_blacklisted