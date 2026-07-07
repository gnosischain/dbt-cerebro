{{
  config(
    materialized='view',
    tags=['production', 'celo', 'gpay', 'tier1', 'api:celo_gpay_retention_volume', 'granularity:monthly']
  )
}}

SELECT
    toString(activity_month) AS date,
    toString(cohort_month)   AS label,
    amount_usd               AS value
FROM {{ ref('fct_celo_gpay_retention_monthly') }}
ORDER BY date, label
