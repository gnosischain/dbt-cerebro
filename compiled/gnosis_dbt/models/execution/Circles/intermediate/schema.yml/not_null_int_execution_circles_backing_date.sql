
    
    



select date
from (select * from `dbt`.`int_execution_circles_backing` where toDate(date) >= today() - 7) dbt_subquery
where date is null


