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
    {{ cohort_bucket_yearly('annual_rolling_fees', include_below_one=true) }} AS cohort,
    round(sum(annual_rolling_fees), 2) AS annual_rolling_fees_total,
    countIf(annual_rolling_fees > 0)   AS users_cnt
FROM per_user
WHERE annual_rolling_fees > 0
GROUP BY week, cohort
