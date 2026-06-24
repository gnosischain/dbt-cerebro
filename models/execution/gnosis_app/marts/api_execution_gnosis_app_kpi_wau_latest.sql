{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','kpi','tier0',
          'api:gnosis_app_kpi_wau','granularity:last_week']
  )
}}

SELECT sub.*, (SELECT toDate(max(week)) FROM {{ ref('fct_execution_gnosis_app_users_weekly') }}) AS as_of_date
FROM (
WITH weeks AS (
    SELECT week, active_users
    FROM {{ ref('fct_execution_gnosis_app_users_weekly') }}
    WHERE week < toStartOfWeek(today())
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
