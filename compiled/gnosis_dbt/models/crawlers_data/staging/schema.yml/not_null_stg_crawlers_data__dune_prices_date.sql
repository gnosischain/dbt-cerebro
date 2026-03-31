
    
    



select date
from (select * from `dbt`.`stg_crawlers_data__dune_prices` where toDate(date) >= today() - 7) dbt_subquery
where date is null


