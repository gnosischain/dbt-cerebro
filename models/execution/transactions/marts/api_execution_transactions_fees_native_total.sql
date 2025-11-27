{{ config(materialized='view', tags=['production','execution','transactions', 'tier0', 'api: fees_total']) }}
SELECT value
FROM {{ ref('fct_execution_transactions_snapshots') }}
WHERE label = 'FeesNative' AND window = 'All'