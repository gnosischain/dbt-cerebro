{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','kpi','tier0',
          'api:gnosis_app_kpi_dau','granularity:last_day']
  )
}}

{# KPI: DAU yesterday + pct change vs the day before. #}

WITH days AS (
    SELECT date, active_users
    FROM {{ ref('fct_execution_gnosis_app_users_daily') }}
    WHERE date < today()
),
ranked AS (
    SELECT date, active_users,
           row_number() OVER (ORDER BY date DESC) AS rn
    FROM days
)
SELECT
    anyIf(active_users, rn = 1)                                                  AS value,
    round((anyIf(active_users, rn = 1) - anyIf(active_users, rn = 2))
          / nullIf(anyIf(active_users, rn = 2), 0) * 100, 1)                     AS change_pct
FROM ranked
