{{
  config(
    materialized='view',
    tags=['production','revenue','revenue_gpay']
  )
}}

SELECT
    week,
    symbol,
    {{ cohort_bucket_yearly('annual_rolling_fees', include_below_one=false) }} AS cohort,
    round(sum(annual_rolling_fees), 2) AS annual_rolling_fees_total,
    countIf(annual_rolling_fees > 0)   AS users_cnt
FROM {{ ref('int_revenue_fees_weekly_per_user') }}
WHERE stream_type = 'gpay'
  AND annual_rolling_fees >= 1
GROUP BY week, symbol, cohort
