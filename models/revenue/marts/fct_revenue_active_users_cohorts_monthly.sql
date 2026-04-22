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
    {{ cohort_bucket_monthly('month_fees') }} AS cohort,
    round(sum(month_fees), 2) AS fees_total,
    countIf(month_fees > 0)   AS users_cnt
FROM per_user
WHERE month_fees >= 0.01
GROUP BY month, cohort
