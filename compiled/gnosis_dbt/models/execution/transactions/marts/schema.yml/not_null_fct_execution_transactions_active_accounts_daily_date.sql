
    
    



select date
from (select * from `dbt`.`fct_execution_transactions_active_accounts_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


