{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_cashback_recipients_weekly','granularity:weekly']
  )
}}

SELECT
    week AS date,
    recipients AS value
FROM {{ ref('fct_execution_gpay_cashback_recipients_weekly') }}
ORDER BY date
