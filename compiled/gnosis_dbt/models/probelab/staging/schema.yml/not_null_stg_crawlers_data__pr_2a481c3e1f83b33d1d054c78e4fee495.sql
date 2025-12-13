
    
    



select max_crawl_created_at
from `dbt`.`stg_crawlers_data__probelab_agent_semvers_avg_1d`
where max_crawl_created_at is null


