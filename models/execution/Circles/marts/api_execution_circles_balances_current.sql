{{
    config(
        materialized='view',
        tags=['production', 'execution', 'circles', 'balances', 'api']
    )
}}

SELECT *
FROM {{ ref('fct_execution_circles_balances_current') }}
