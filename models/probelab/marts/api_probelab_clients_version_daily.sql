
{{ 
    config(
        materialized='view',
        tags=['production','probelab']
    ) 
}}

SELECT 
    toStartOfDay(max_crawl_created_at) AS date
    ,agent_version_type AS client
    ,agent_version_semver_str AS version
    ,toInt32(floor(__count)) AS value
FROM 
    {{ ref('stg_crawlers_data__probelab_agent_semvers_avg_1d') }} 
ORDER BY date ASC, client ASC, version ASC