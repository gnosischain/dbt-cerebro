





with validation_errors as (

    select
        modifier_address, block_timestamp, log_index
    from (select * from `dbt`.`int_execution_zodiac_modifier_module_events` where toDate(block_timestamp) >= today() - 7) dbt_subquery
    group by modifier_address, block_timestamp, log_index
    having count(*) > 1

)

select *
from validation_errors


