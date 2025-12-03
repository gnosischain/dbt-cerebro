{{ config(materialized='view', tags=['production','execution','tokens', 'tier0', 'api: supply_total']) }}
SELECT value
FROM {{ ref('fct_execution_tokens_snapshots') }}
WHERE label = 'Supply' AND window = 'All'

