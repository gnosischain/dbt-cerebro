{{
    config(
        materialized='view',
        tags=['production', 'execution', 'yields', 'api:yields_user_kpis', 'granularity:all_time', 'tier1']
    )
}}

SELECT sub.*, (SELECT toDate(max(date)) FROM {{ ref('int_execution_lending_aave_user_balances_daily') }}) AS as_of_date
FROM (
SELECT *
FROM {{ ref('fct_execution_yields_user_lifetime_metrics') }}
) AS sub
