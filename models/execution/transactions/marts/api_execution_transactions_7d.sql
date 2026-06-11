{{ 
    config(
        materialized='view', 
        tags=['production','execution','tier0', 'api:transactions_count', 'granularity:last_7d']) 
    }}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_execution_transactions_by_project_daily') }}) AS as_of_date
FROM (
SELECT value, change_pct
FROM {{ ref('fct_execution_transactions_snapshots') }}
WHERE label = 'Transactions' AND window = '7D'
) AS sub
