
    
    



select date
from (select * from `dbt`.`fct_ubo_known_containers_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


