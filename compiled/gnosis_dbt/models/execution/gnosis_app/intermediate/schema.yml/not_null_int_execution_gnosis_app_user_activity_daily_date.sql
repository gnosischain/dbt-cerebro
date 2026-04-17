
    
    



select date
from (select * from `dbt`.`int_execution_gnosis_app_user_activity_daily` where date >= today() - 7) dbt_subquery
where date is null


