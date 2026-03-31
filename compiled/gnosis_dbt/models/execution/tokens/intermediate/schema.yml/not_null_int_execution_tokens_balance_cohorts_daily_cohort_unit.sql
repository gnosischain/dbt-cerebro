
    
    



select cohort_unit
from (select * from `dbt`.`int_execution_tokens_balance_cohorts_daily` where toDate(date) >= today() - 7) dbt_subquery
where cohort_unit is null


