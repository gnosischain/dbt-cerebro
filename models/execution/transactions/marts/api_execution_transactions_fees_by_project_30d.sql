{{
  config(
    materialized='view', 
    tags=['production','execution','transactions']
)
}}

SELECT
  project,
  SUM(fee_native_sum) AS fee_native,
  SUM(fee_usd_sum)    AS fee_usd
FROM {{ ref('int_execution_transactions_by_project_daily') }}
WHERE day > now() - INTERVAL 30 DAY
GROUP BY project
ORDER BY fee_usd DESC