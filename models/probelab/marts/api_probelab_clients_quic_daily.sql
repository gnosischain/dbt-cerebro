
{{ 
    config(
        materialized='view',
        tags=['production','probelab', 'clients', 'tier1', 'api: clients_quic_d']
    ) 
}}

SELECT 
    toStartOfDay(max_crawl_created_at) AS date
    ,agent_version_type AS client
    ,quic_support AS quic
    ,__count AS value
FROM 
    {{ ref('stg_crawlers_data__probelab_quic_support_over_7d') }} 
ORDER BY date ASC, client ASC, quic ASC