{{
    config(
        materialized='view',
        tags=['production','execution','tier0','api:yields_lending_borrowers_count', 'granularity:last_7d']
    )
}}

SELECT
    token,
    value,
    change_pct
FROM {{ ref('fct_execution_yields_lending_latest') }}
WHERE label = 'Borrowers' AND window = '7D'
ORDER BY token
