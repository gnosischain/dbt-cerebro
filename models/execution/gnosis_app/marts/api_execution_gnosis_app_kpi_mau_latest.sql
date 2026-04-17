{{
  config(
    materialized='view',
    tags=['production','execution','gnosis_app','kpi','tier0',
          'api:gnosis_app_kpi_mau','granularity:last_month']
  )
}}

WITH months AS (
    SELECT month, active_users
    FROM {{ ref('fct_execution_gnosis_app_users_monthly') }}
    WHERE month < toStartOfMonth(today())
),
ranked AS (
    SELECT month, active_users,
           row_number() OVER (ORDER BY month DESC) AS rn
    FROM months
)
SELECT
    anyIf(active_users, rn = 1)                                                  AS value,
    round((anyIf(active_users, rn = 1) - anyIf(active_users, rn = 2))
          / nullIf(anyIf(active_users, rn = 2), 0) * 100, 1)                     AS change_pct
FROM ranked
