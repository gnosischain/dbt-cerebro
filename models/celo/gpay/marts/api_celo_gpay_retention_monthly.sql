{{
  config(
    materialized='view',
    tags=['production', 'celo', 'gpay', 'tier1', 'api:celo_gpay_retention', 'granularity:monthly']
  )
}}

SELECT
    toString(activity_month) AS date,
    toString(cohort_month)   AS label,
    users                    AS value
FROM {{ ref('fct_celo_gpay_retention_monthly') }}
ORDER BY date, label
