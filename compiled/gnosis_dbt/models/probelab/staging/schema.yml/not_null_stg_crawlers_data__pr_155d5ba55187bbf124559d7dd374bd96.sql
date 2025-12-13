
    
    



select max_crawl_created_at
from `dbt`.`stg_crawlers_data__probelab_quic_support_over_7d`
where max_crawl_created_at is null


