{{ 
    config(
        materialized='view',
        tags=['production','execution', 'tier1', 'api:blocks_per_clients_count', 'granularity:daily']
    )
}}


SELECT
    date
    ,client
    ,cnt AS value
FROM {{ ref('fct_execution_blocks_clients_daily') }}
ORDER BY date, client