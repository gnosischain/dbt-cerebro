{{
  config(
    materialized='view',
    tags=['production','execution','gpay','tier1','api:gpay_payments_by_token_weekly','granularity:weekly']
  )
}}

SELECT
    week     AS date,
    token    AS label,
    payments AS value
FROM {{ ref('fct_execution_gpay_volume_payments_by_token_weekly') }}
ORDER BY date, label
