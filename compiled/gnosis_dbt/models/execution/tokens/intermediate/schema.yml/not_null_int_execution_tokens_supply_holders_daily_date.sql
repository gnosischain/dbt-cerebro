
    
    



select date
from (select * from `dbt`.`int_execution_tokens_supply_holders_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


