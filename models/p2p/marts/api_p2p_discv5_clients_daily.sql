{{ 
    config(
        materialized='view',
        tags=['production','p2p', 'tier1', 'api:discv5_clients_count', 'granularity:daily']
    ) 
}}

SELECT
    date
    ,metric
    ,label
    ,value
FROM {{ ref('int_p2p_discv5_clients_daily') }}
WHERE date < today()
ORDER BY date, metric, label
