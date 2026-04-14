
    
    



select date
from (select * from `dbt`.`int_execution_gpay_spend_activity_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


