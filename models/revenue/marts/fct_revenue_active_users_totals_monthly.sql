{% set active_threshold_usd = 0.5 %}

{{
  config(
    materialized='view',
    tags=['production','revenue','revenue_cross']
  )
}}

WITH per_user AS (
    SELECT
        month,
        user,
        sum(month_fees) AS month_fees
    FROM {{ ref('int_revenue_fees_monthly_per_user') }}
    GROUP BY month, user
)

SELECT
    month,
    countIf(month_fees >= {{ active_threshold_usd }}) AS users_cnt,
    round(sumIf(month_fees, month_fees >= {{ active_threshold_usd }}), 2) AS fees_total
FROM per_user
GROUP BY month
