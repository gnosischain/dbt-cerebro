
    
    



select validator_index
from (select * from `dbt`.`stg_consensus__withdrawals` where toDate(slot_timestamp) >= today() - 7) dbt_subquery
where validator_index is null


