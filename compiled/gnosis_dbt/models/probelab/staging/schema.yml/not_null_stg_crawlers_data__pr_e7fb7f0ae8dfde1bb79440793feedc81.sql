
    
    



select min_crawl_created_at
from (select * from `dbt`.`stg_crawlers_data__probelab_quic_support_over_7d` where toDate(max_crawl_created_at) >= today() - 7) dbt_subquery
where min_crawl_created_at is null


