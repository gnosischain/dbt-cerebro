
    
    



select date
from (select * from `dbt`.`int_execution_state_size_full_diff_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


