{{ 
    config(
        materialized='view',
        tags=['deprecated','execution', 'tier1', 'granularity:daily']
    )
}}

SELECT
    date
    ,bytes/POWER(10,9) AS value
FROM {{ ref('fct_execution_state_full_size_daily') }}
