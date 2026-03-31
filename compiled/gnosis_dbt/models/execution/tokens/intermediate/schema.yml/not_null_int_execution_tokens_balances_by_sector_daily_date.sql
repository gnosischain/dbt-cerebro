
    
    



select date
from (select * from `dbt`.`int_execution_tokens_balances_by_sector_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


