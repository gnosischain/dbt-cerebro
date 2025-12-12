{{ 
    config(
        materialized='view', 
        tags=['production','execution', 'tier0', 'api:transactions_initiators_count', 'granularity:last_7d']) 
}}
SELECT value, change_pct
FROM {{ ref('fct_execution_transactions_snapshots') }}
WHERE label = 'ActiveAccounts' AND window = '7D'