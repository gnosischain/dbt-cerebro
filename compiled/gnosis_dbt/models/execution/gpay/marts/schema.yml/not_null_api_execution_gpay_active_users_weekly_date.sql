
    
    



select date
from (select * from `dbt`.`api_execution_gpay_active_users_weekly` where toDate(date) >= today() - 7) dbt_subquery
where date is null


