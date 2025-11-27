{{ config(materialized='view', tags=['production','execution','transactions', 'tier0', 'api: initiator_accounts_by_project_total']) }}
SELECT bucket AS label, value
FROM {{ ref('fct_execution_transactions_by_project_snapshots') }} t
WHERE t.label = 'ActiveAccounts' AND window = 'All'
ORDER BY value DESC