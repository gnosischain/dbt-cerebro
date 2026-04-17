{{
    config(
        materialized='view',
        tags=['production','execution','tier0','api:execution_lending_borrowers_count', 'granularity:last_7d']
    )
}}

-- One row per protocol plus an ALL-protocols aggregate. See lenders_count_7d header.

SELECT
    token,
    protocol,
    value,
    change_pct
FROM {{ ref('fct_execution_lending_latest') }}
WHERE label = 'Borrowers' AND window = '7D' AND token = 'ALL'
