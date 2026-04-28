
    
    



select token_address
from (select * from `dbt`.`int_execution_circles_v2_tokens_supply_daily` where toDate(date) >= today() - 7) dbt_subquery
where token_address is null


