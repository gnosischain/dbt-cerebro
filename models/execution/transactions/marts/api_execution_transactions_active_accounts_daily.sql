{{
  config(
    materialized='view',
    tags=['production', 'execution', 'tier1', 'api:transactions_initiators_count', 'granularity:daily'])
}}

SELECT
  date,
  active_accounts AS value
FROM {{ ref('fct_execution_transactions_active_accounts_daily') }}
WHERE date < today()
ORDER BY date ASC
