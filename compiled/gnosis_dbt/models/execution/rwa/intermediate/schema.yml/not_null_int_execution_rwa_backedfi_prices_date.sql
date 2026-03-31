
    
    



select date
from (select * from `dbt`.`int_execution_rwa_backedfi_prices` where toDate(date) >= today() - 7) dbt_subquery
where date is null


