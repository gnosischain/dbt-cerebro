{{ config(materialized='view', tags=['production','execution','tokens', 'tier0', 'api: active_senders_total']) }}
SELECT value
FROM {{ ref('fct_execution_tokens_snapshots') }}
WHERE label = 'ActiveSenders' AND window = 'All'

