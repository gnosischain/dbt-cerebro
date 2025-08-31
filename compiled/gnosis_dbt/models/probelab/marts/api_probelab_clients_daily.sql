

SELECT 
    toStartOfDay(max_crawl_created_at) AS date
    ,agent_version_type AS client
    ,any_value(toInt32(floor(__total))) AS value
FROM 
    `dbt`.`stg_crawlers_data__probelab_agent_semvers_avg_1d` 
GROUP BY 1, 2
ORDER BY date ASC, client ASC