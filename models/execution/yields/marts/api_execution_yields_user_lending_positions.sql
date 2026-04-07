{{
    config(
        materialized='view',
        tags=['production','execution','yields','api:yields_user_lending_positions']
    )
}}

SELECT *
FROM {{ ref('fct_execution_yields_user_lending_positions_latest') }}
