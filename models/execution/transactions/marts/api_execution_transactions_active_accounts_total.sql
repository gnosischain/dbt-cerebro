{{ 
    config(
        materialized='view', 
        tags=['production','execution', 'tier0', 'api:transactions_initiators_count', 'granularity:all_time']) 
}}
SELECT value
FROM {{ ref('fct_execution_transactions_snapshots') }}
WHERE label = 'ActiveAccounts' AND window = 'All'