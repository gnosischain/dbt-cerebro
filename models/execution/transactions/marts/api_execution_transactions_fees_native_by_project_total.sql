{{ config(materialized='view', tags=['production','execution','transactions']) }}
SELECT bucket AS label, value, change_pct
FROM {{ ref('fct_execution_transactions_by_project_snapshots') }}
WHERE label = 'FeesNative' AND window = 'All'
ORDER BY value DESC