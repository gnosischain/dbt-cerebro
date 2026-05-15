

-- Gnosis App Weekly Economically Active Users — addresses that were both
-- active in a given week AND earned >= 1 Circles reward (cashback or
-- inviter fee) in that same week. Blacklist flag preserved.
--
-- Mirrors the Dune circles-v2-kpis dashboard
-- `active_user_this_week_exclude_blacklist` output computed on the
-- intersected set.

WITH intersected AS (
    SELECT
        s.week,
        s.address
    FROM `dbt`.`int_execution_gnosis_app_weekly_signals` s
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