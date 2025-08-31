
{{ 
    config(
        materialized='view',
        tags=['production','probelab']
    ) 
}}

SELECT 
    toStartOfDay(max_crawl_created_at) AS date
    ,agent_version_type AS client
    ,country
    ,toInt32(floor(__count)) AS value
FROM 
    {{ ref('stg_crawlers_data__probelab_countries_avg_1d') }} 
ORDER BY date ASC, client ASC, country ASC