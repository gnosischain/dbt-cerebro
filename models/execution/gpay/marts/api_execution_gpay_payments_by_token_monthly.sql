{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_payments_by_token_monthly','granularity:monthly']
  )
}}

SELECT
    month           AS date,
    token           AS label,
    activity_count  AS value
FROM {{ ref('fct_execution_gpay_actions_by_token_monthly') }}
WHERE action = 'Payment'
ORDER BY date, label
