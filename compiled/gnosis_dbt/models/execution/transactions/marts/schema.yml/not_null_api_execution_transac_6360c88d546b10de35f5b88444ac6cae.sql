
    
    



select date
from (select * from `dbt`.`api_execution_transactions_active_accounts_by_sector_weekly` where toDate(date) >= today() - 7) dbt_subquery
where date is null


