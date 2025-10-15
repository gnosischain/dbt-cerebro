{{ config(materialized='view', tags=['production','execution','transactions']) }}
SELECT t.bucket AS label, t.value, t.change_pct
FROM {{ ref('fct_execution_transactions_by_project_snapshots') }} AS t
WHERE t.label = 'FeesNative' AND t.window = 'All'
ORDER BY t.value DESC