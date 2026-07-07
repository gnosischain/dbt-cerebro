{{
  config(
    materialized='view',
    tags=['production', 'celo', 'gpay', 'tier1', 'api:celo_gpay_retention_pct', 'granularity:monthly']
  )
}}

SELECT
    toString(activity_month) AS x,
    toString(cohort_month)   AS y,
    retention_pct            AS retention_pct,
    users                    AS value_abs,
    amount_retention_pct     AS amount_retention_pct,
    amount_usd               AS value_usd
FROM {{ ref('fct_celo_gpay_retention_monthly') }}
ORDER BY y, x
