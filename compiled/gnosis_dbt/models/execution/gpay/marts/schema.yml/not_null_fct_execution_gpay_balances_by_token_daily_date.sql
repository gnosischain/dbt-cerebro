
    
    



select date
from (select * from `dbt`.`fct_execution_gpay_balances_by_token_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


