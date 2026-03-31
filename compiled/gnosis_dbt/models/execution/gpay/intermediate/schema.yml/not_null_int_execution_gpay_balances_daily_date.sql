
    
    



select date
from (select * from `dbt`.`int_execution_gpay_balances_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


