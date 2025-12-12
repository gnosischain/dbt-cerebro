{{ 
    config(
        materialized='view', 
        tags=['production','execution', 'tier0', 'api:transactions_fees', 'granularity:all_time']) 
}}

SELECT value
FROM {{ ref('fct_execution_transactions_snapshots') }}
WHERE label = 'FeesNative' AND window = 'All'