
    
    



select deposit_index
from (select * from `dbt`.`stg_consensus__deposits` where toDate(slot_timestamp) >= today() - 7) dbt_subquery
where deposit_index is null


