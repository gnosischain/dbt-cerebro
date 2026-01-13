{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:yields_lending', 'granularity:daily']
    )
}}

SELECT
    date,
    symbol AS token,
    token_class,
    protocol AS label,
    apy_daily AS value
FROM {{ ref('fct_execution_yields_lending_daily') }}
ORDER BY date DESC, token, label
