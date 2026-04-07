
    
    



select avatar
from (select * from `dbt`.`int_execution_circles_v1_avatars` where toDate(block_timestamp) >= today() - 7) dbt_subquery
where avatar is null


