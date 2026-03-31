
    
    



select block_timestamp
from (select * from `dbt`.`int_execution_gpay_activity` where toDate(date) >= today() - 7) dbt_subquery
where block_timestamp is null


