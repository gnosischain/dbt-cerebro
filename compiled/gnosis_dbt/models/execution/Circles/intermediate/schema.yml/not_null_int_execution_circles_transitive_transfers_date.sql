
    
    



select date
from (select * from `dbt`.`int_execution_circles_transitive_transfers` where toDate(date) >= today() - 7) dbt_subquery
where date is null


