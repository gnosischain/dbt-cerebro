{{ 
    config(
        materialized='view',
        tags=['production','execution', 'tier1', 'api:blocks_per_clients_pct', 'granularity:daily']
    )
}}

SELECT
    date
    ,client
    ,ROUND(fraq * 100, 2) AS value
FROM {{ ref('fct_execution_blocks_clients_daily') }}
ORDER BY date, client