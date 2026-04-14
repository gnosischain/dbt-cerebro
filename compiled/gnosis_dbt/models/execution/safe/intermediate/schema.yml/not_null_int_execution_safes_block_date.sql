
    
    



select block_date
from (select * from `dbt`.`int_execution_safes` where toDate(block_date) >= today() - 7) dbt_subquery
where block_date is null


