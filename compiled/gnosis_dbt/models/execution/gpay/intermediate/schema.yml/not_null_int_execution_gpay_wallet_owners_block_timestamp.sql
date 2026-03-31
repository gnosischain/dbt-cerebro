
    
    



select block_timestamp
from (select * from `dbt`.`int_execution_gpay_wallet_owners` where toDate(block_timestamp) >= today() - 7) dbt_subquery
where block_timestamp is null


