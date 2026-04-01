{{
    config(
        materialized='view',
        tags=['production','execution','tier0','api:execution_lending_borrowers_count', 'granularity:last_7d']
    )
}}

SELECT
    token,
    value,
    change_pct
FROM {{ ref('fct_execution_lending_latest') }}
WHERE label = 'Borrowers' AND window = '7D' AND token = 'ALL'
