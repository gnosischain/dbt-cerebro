
    
    



select validator_index
from (select * from `dbt`.`int_consensus_entry_queue_daily` where toDate(date) >= today() - 7) dbt_subquery
where validator_index is null


