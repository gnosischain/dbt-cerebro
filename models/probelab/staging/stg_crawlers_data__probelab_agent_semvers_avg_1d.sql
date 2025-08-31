{{ 
    config(
        materialized='view',
        tags=['production','crawlers_data', 'probelab_agent_semvers_avg_1d']
    ) 
}}

SELECT
    agent_version_type,
    min_crawl_created_at,
    max_crawl_created_at,
    agent_version_semver,
    agent_version_semver_str,
    __count,
    __samples,
    __pct,
    __total
FROM 
    {{ source('crawlers_data','probelab_agent_semvers_avg_1d') }} 