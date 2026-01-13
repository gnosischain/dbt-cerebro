{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:yields_lending_borrow', 'granularity:daily']
    )
}}

SELECT
    date,
    symbol AS token,
    token_class,
    protocol AS label,
    borrow_apy_variable_daily AS value
FROM {{ ref('fct_execution_yields_lending_daily') }}
WHERE borrow_apy_variable_daily IS NOT NULL
ORDER BY date DESC, token, label
