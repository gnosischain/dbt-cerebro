
    
    



select date
from (select * from `dbt`.`api_execution_circles_avatars` where toDate(date) >= today() - 7) dbt_subquery
where date is null


