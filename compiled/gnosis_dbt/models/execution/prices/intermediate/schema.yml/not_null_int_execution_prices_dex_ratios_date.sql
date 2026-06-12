
    
    



select date
from (select * from `dbt`.`int_execution_prices_dex_ratios` where toDate(date) >= today() - 7) dbt_subquery
where date is null


