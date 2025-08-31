{{ 
    config(
        materialized='view',
        tags=['production','crawlers_data', 'probelab_quic_support_over_7d']
    ) 
}}

SELECT
    agent_version_type,
    min_crawl_created_at,
    max_crawl_created_at,
    crawl_created_at,
    quic_support,
    __count,
    __pct,
    __total
FROM 
    {{ source('crawlers_data','probelab_quic_support_over_7d') }} 