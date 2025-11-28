
{{ 
    config(
        materialized='view',
        tags=['production','probelab', 'clients', 'tier1', 'api: clients_cloud_d']
    ) 
}}

SELECT 
    toStartOfDay(max_crawl_created_at) AS date
    ,agent_version_type AS client
    ,cloud_provider AS cloud
    ,toInt32(floor(__count)) AS value
FROM 
    {{ ref('stg_crawlers_data__probelab_cloud_provider_avg_1d') }} 
ORDER BY date ASC, client ASC, cloud ASC


        
