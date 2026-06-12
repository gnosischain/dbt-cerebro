

-- Circles-ecosystem weekly active reach — NOT a Gnosis App growth metric.
-- Distinct addresses active that week across the WHOLE Circles network
-- (register/trust/personal-mint via any wallet) + filled Cometh swaps + Gnosis
-- App activity, broken down by blacklist flag. Mirrors the Dune circles-v2-kpis
-- `active_user_this_week_exclude_blacklist` output. Carries the 2025-11-12 floor
-- (in the signals) and the blacklist join.
--
-- The headline Gnosis-App WAU is fct_execution_gnosis_app_users_weekly
-- (Lineage A, app-only, with new/returning/reactivated). THIS model is the
-- separate whole-Circles-network number for Circles/Garage reporting.

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