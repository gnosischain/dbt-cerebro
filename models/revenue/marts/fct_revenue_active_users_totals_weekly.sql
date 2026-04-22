{% set active_threshold_usd = 6.0 %}

{{
  config(
    materialized='view',
    tags=['production','revenue','revenue_cross']
  )
}}

WITH per_user AS (
    SELECT
        week,
        user,
        sum(annual_rolling_fees) AS annual_rolling_fees
    FROM {{ ref('int_revenue_fees_weekly_per_user') }}
    GROUP BY week, user
)

SELECT
    week,
    countIf(annual_rolling_fees >= {{ active_threshold_usd }}) AS users_cnt,
    round(sumIf(annual_rolling_fees, annual_rolling_fees >= {{ active_threshold_usd }}), 2) AS annual_rolling_fees_total
FROM per_user
GROUP BY week
