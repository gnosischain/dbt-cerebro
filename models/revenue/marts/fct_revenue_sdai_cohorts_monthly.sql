{{
  config(
    materialized='view',
    tags=['production','revenue','revenue_sdai']
  )
}}

SELECT
    month,
    {{ cohort_bucket_monthly('month_fees') }} AS cohort,
    round(sum(month_fees), 2) AS fees_total,
    countIf(month_fees > 0)   AS users_cnt
FROM {{ ref('int_revenue_fees_monthly_per_user') }}
WHERE stream_type = 'sdai'
  AND month_fees >= 0.01
GROUP BY month, cohort
