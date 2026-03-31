
    
    



select committee_index
from (select * from `dbt`.`stg_consensus__attestations` where toDate(slot_timestamp) >= today() - 7) dbt_subquery
where committee_index is null


