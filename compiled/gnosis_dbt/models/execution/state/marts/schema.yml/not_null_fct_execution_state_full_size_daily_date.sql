
    
    



select date
from (select * from `dbt`.`fct_execution_state_full_size_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


