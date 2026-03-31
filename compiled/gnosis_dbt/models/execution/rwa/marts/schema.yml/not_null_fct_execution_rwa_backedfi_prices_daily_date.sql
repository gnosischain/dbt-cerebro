
    
    



select date
from (select * from `dbt`.`fct_execution_rwa_backedfi_prices_daily` where toDate(date) >= today() - 7) dbt_subquery
where date is null


