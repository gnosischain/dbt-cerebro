

-- Gnosis App Weekly Active Users — distinct addresses active that week,
-- broken down by blacklist flag. Mirrors the Dune circles-v2-kpis dashboard
-- `active_user_this_week_exclude_blacklist` output.
--
-- The blacklist join lives here (full-rebuild mart) so a fresh
-- crawlers_data.circles_blacklisted snapshot from click-runner re-applies
-- on the next run without needing to backfill upstream.

WITH base AS (
    SELECT
        s.week,
        s.address,
        b.address IS NOT NULL AS is_blacklisted
    FROM `dbt`.`int_execution_gnosis_app_weekly_signals` s
    LEFT JOIN `dbt`.`stg_crawlers_data__circles_blacklisted` b
        ON b.address = s.address
)

SELECT
    week,
    is_blacklisted,
    count(DISTINCT address) AS cnt
FROM base
GROUP BY week, is_blacklisted
ORDER BY week, is_blacklisted