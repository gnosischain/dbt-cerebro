
    
    



select transaction_hash
from (select * from `dbt`.`stg_execution__storage_diffs` where toDate(block_timestamp) >= today() - 7) dbt_subquery
where transaction_hash is null


