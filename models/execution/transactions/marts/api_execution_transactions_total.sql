{{ config(materialized='view', tags=['production','execution','transactions', 'tier0', 'api: cnt_total']) }}
SELECT value
FROM {{ ref('fct_execution_transactions_snapshots') }}
WHERE label = 'Transactions' AND window = 'All'