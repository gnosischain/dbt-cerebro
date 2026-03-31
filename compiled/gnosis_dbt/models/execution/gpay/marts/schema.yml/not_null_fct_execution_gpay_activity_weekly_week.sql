
    
    



select week
from (select * from `dbt`.`fct_execution_gpay_activity_weekly` where toDate(week) >= today() - 7) dbt_subquery
where week is null


