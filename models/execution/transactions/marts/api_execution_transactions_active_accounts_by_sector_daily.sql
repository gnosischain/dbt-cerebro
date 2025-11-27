{{ config(materialized='view', tags=['production','execution','transactions', 'tier1', 'api: initiator_accounts_by_sector_d']) }}

SELECT
  date,
  sector AS label,
  active_accounts AS value
FROM {{ ref('fct_execution_transactions_by_sector_daily') }}
WHERE date < today()
ORDER BY date ASC, label ASC