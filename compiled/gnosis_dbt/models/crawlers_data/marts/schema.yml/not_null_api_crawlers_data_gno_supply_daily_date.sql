
    
    



select date
from (select * from `dbt`.`api_crawlers_data_gno_supply_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


