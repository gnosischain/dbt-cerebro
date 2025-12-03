{{ config(materialized='view', tags=['production','execution','tokens', 'tier0', 'api: holders_total']) }}
SELECT value
FROM {{ ref('fct_execution_tokens_snapshots') }}
WHERE label = 'Holders' AND window = 'All'

