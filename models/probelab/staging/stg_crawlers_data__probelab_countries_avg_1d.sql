SELECT
    agent_version_type,
    min_crawl_created_at,
    max_crawl_created_at,
    country_name,
    country,
    __count,
    __samples,
    __pct,
    __total
FROM 
    {{ source('crawlers_data','probelab_countries_avg_1d') }} 