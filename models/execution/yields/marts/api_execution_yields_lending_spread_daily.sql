{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:yields_lending_spread', 'granularity:daily']
    )
}}

SELECT
    date,
    symbol AS token,
    token_class,
    protocol AS label,
    spread_variable AS value
FROM {{ ref('fct_execution_yields_lending_daily') }}
WHERE spread_variable IS NOT NULL
ORDER BY date DESC, token, label
