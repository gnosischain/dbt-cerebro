{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','kpi','tier0',
          'api:gnosis_app_kpi_weekly_active_users','granularity:last_week']
  )
}}

-- Latest complete week's Gnosis App Weekly Active Users + WoW change pct.
-- Reads the SAME lineage as DAU/MAU (fct_execution_gnosis_app_users_weekly,
-- Gnosis-App-only `active_users`) so DAU/WAU/MAU are one consistent family.
-- The whole-Circles-network number is the separate
-- api:gnosis_app_circles_ecosystem_weekly_active_users metric.

SELECT sub.*, (SELECT toDate(max(week)) FROM {{ ref('fct_execution_gnosis_app_users_weekly') }}) AS as_of_date
FROM (
WITH weeks AS (
    SELECT week, active_users
    FROM {{ ref('fct_execution_gnosis_app_users_weekly') }}
    WHERE week < toStartOfWeek(today(), 1)
),
ranked AS (
    SELECT week, active_users,
           row_number() OVER (ORDER BY week DESC) AS rn
    FROM weeks
)
SELECT
    anyIf(active_users, rn = 1)                                                  AS value,
    round((anyIf(active_users, rn = 1) - anyIf(active_users, rn = 2))
          / nullIf(anyIf(active_users, rn = 2), 0) * 100, 1)                     AS change_pct
FROM ranked
) AS sub
