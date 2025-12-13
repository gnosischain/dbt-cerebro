{{ 
    config(
        materialized='view', 
        tags=['production','execution', 'tier0', 'api:transactions_initiators_count_per_project', 'granularity:all_time']) 
}}

SELECT bucket AS label, value
FROM {{ ref('fct_execution_transactions_by_project_snapshots') }} t
WHERE t.label = 'ActiveAccounts' AND window = 'All'
ORDER BY value DESC