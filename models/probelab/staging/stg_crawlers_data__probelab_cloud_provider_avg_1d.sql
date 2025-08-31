{{ 
    config(
        materialized='view',
        tags=['production','crawlers_data', 'probelab_cloud_provider_avg_1d']
    ) 
}}

SELECT
    agent_version_type,
    min_crawl_created_at,
    max_crawl_created_at,
    cloud_provider,
    __count,
    __samples,
    __pct,
    __total
FROM 
    {{ source('crawlers_data','probelab_cloud_provider_avg_1d') }} 