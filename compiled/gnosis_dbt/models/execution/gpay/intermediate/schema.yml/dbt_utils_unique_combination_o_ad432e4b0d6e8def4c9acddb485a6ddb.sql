





with validation_errors as (

    select
        roles_module_address, block_timestamp, log_index
    from (select * from `dbt`.`int_execution_gpay_roles_events` where toDate(block_timestamp) >= today() - 7) dbt_subquery
    group by roles_module_address, block_timestamp, log_index
    having count(*) > 1

)

select *
from validation_errors


