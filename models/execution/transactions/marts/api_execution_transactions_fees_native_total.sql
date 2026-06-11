{{ 
    config(
        materialized='view', 
        tags=['production','execution', 'tier0', 'api:transactions_fees', 'granularity:all_time']) 
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_execution_transactions_by_project_daily') }}) AS as_of_date
FROM (
SELECT value
FROM {{ ref('fct_execution_transactions_snapshots') }}
WHERE label = 'FeesNative' AND window = 'All'
) AS sub
