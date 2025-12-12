{{ 
    config(
        materialized='view',
        tags=['production','p2p', 'tier0', 'api:discv5_clients_count', 'granularity:latest']
    ) 
}}

SELECT
    metric
    ,label
    ,value
FROM {{ ref('int_p2p_discv5_clients_daily') }}
WHERE date = (SELECT MAX(date) FROM  {{ ref('int_p2p_discv5_clients_daily') }} )
ORDER BY metric, label
