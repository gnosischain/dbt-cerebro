
    
    



select date
from (select * from `dbt`.`int_execution_circles_v2_tokens_supply_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


