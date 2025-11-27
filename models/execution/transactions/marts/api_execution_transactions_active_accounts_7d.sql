{{ config(materialized='view', tags=['production','execution','transactions', 'tier0', 'api: initiator_accounts_7d']) }}
SELECT value, change_pct
FROM {{ ref('fct_execution_transactions_snapshots') }}
WHERE label = 'ActiveAccounts' AND window = '7D'