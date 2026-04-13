{{
    config(
        materialized='view',
        tags=['production','execution','yields','api:yields_user_kpis','granularity:all_time']
    )
}}

SELECT *
FROM {{ ref('fct_execution_yields_user_lifetime_metrics') }}
