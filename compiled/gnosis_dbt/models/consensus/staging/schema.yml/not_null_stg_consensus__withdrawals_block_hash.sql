
    
    



select block_hash
from (select * from `dbt`.`stg_consensus__withdrawals` where toDate(slot_timestamp) >= today() - 7) dbt_subquery
where block_hash is null


