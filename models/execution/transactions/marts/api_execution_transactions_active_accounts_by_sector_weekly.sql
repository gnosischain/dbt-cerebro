{{ config(materialized='view', tags=['production','execution','transactions', 'tier1', 'api: initiator_accounts_by_sector_w']) }}

SELECT
  week AS date,
  sector AS label,
  active_accounts AS value
FROM {{ ref('fct_execution_transactions_by_sector_weekly') }}
ORDER BY date ASC, label ASC