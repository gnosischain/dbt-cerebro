{{ 
    config(
        materialized='view',
        tags=['production','execution','state','size']
    )
}}

SELECT
    date
    ,SUM(bytes_diff) OVER (ORDER BY date ASC) AS bytes
FROM {{ ref('int_execution_state_size_full_diff_daily') }}
WHERE date < today()