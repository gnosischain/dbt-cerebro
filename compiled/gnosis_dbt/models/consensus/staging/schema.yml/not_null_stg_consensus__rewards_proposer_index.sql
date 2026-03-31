
    
    



select proposer_index
from (select * from `dbt`.`stg_consensus__rewards` where toDate(slot_timestamp) >= today() - 7) dbt_subquery
where proposer_index is null


