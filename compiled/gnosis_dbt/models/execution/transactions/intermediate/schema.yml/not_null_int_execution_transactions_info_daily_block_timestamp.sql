
    
    



select block_timestamp
from (select * from `dbt`.`int_execution_transactions_info_daily` where toDate(date) >= today() - 7) dbt_subquery
where block_timestamp is null


