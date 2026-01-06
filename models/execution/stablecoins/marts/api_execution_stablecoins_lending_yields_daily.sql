{{
    config(
        materialized='view',
        tags=['production','execution','tier1','api:stablecoins_lending_yields', 'granularity:daily']
    )
}}

SELECT
    date,
    symbol AS token,
    protocol AS label,
    apy_daily AS value
FROM {{ ref('fct_execution_stablecoins_lending_yields_daily') }}
ORDER BY date DESC, token, label