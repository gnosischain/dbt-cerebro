
    
    



select transaction_index
from (select * from `dbt`.`stg_execution__storage_diffs` where toDate(block_timestamp) >= today() - 7) dbt_subquery
where transaction_index is null


