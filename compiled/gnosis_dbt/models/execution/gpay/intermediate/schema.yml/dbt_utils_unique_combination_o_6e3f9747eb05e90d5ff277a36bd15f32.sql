





with validation_errors as (

    select
        spender_module_address, block_timestamp, log_index
    from (select * from `dbt`.`int_execution_gpay_spender_events` where toDate(block_timestamp) >= today() - 7) dbt_subquery
    group by spender_module_address, block_timestamp, log_index
    having count(*) > 1

)

select *
from validation_errors


