{{ 
    config(
        materialized='view',
        tags=['production','p2p','discv4','clients']
    ) 
}}

SELECT
    metric
    ,label
    ,value
FROM {{ ref('int_p2p_discv4_clients_daily') }}
WHERE date = (SELECT MAX(date) FROM  {{ ref('int_p2p_discv4_clients_daily') }} )
ORDER BY metric, label
