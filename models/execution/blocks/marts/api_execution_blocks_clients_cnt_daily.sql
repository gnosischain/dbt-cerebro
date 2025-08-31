{{ 
    config(
        materialized='view',
        tags=['production','execution','blocks']
    )
}}


SELECT
    date
    ,client
    ,cnt AS value
FROM {{ ref('fct_execution_blocks_clients_daily') }}
ORDER BY date, client