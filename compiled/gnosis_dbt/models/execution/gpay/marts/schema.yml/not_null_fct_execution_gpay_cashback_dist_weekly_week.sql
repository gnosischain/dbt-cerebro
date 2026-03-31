
    
    



select week
from (select * from `dbt`.`fct_execution_gpay_cashback_dist_weekly` where toDate(week) >= today() - 7) dbt_subquery
where week is null


