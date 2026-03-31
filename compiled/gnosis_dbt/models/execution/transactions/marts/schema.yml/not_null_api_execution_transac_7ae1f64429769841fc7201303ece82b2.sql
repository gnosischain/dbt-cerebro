
    
    



select date
from (select * from `dbt`.`api_execution_transactions_by_project_monthly_top5` where toDate(date) >= today() - 7) dbt_subquery
where date is null


