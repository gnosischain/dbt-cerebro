{{ 
    config(
        materialized='view', 
        tags=['production','execution', 'tier0', 'api:transactions_fees_per_project', 'granularity:all_time']) 
}}

SELECT t.bucket AS label, t.value
FROM {{ ref('fct_execution_transactions_by_project_snapshots') }} AS t
WHERE t.label = 'FeesNative' AND t.window = 'All'
ORDER BY t.value DESC