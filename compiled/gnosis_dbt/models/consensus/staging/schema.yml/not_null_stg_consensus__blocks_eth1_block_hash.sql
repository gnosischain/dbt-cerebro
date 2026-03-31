
    
    



select eth1_block_hash
from (select * from `dbt`.`stg_consensus__blocks` where toDate(slot_timestamp) >= today() - 7) dbt_subquery
where eth1_block_hash is null


