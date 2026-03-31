
    
    



select date
from (select * from `dbt`.`int_execution_circles_v2_avatars` where toDate(date) >= today() - 7) dbt_subquery
where date is null


