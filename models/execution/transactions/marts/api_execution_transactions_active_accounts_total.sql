{{ config(materialized='view', tags=['production','execution','transactions', 'tier0', 'api: initiator_accounts_total']) }}
SELECT value
FROM {{ ref('fct_execution_transactions_snapshots') }}
WHERE label = 'ActiveAccounts' AND window = 'All'