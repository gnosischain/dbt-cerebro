{{ 
    config(
        materialized='view',
        tags=['production','p2p','discv5','clients', 'tier1', 'api: discv5_clients_d']
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
