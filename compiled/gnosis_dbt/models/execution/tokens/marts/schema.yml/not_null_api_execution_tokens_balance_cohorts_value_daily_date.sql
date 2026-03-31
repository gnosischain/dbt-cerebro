
    
    



select date
from (select * from `dbt`.`api_execution_tokens_balance_cohorts_value_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


