
    
    



select slot_timestamp
from (select * from `dbt`.`stg_consensus__blocks` where toDate(slot_timestamp) >= today() - 7) dbt_subquery
where slot_timestamp is null


