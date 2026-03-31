
    
    



select date
from (select * from `dbt`.`api_execution_transactions_gas_share_by_project_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


