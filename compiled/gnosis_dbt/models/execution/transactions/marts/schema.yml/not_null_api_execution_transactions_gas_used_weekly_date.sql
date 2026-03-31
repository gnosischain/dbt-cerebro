
    
    



select date
from (select * from `dbt`.`api_execution_transactions_gas_used_weekly` where toDate(date) >= today() - 7) dbt_subquery
where date is null


