{{ 
    config(
        materialized='view', 
        tags=['production','execution','tier0', 'api:transactions_count', 'granularity:last_7d']) 
    }}
SELECT value, change_pct
FROM {{ ref('fct_execution_transactions_snapshots') }}
WHERE label = 'Transactions' AND window = '7D'