
    
    



select block_timestamp
from (select * from `dbt`.`int_execution_zodiac_modifier_module_events` where toDate(block_timestamp) >= today() - 7) dbt_subquery
where block_timestamp is null


