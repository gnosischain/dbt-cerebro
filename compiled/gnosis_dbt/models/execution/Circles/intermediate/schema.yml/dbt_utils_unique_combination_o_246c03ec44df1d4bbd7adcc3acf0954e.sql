





with validation_errors as (

    select
        block_timestamp, group_address, token_id, batch_index, transaction_hash, log_index, event_name
    from (select * from `dbt`.`int_execution_circles_v2_group_collateral_diffs` where toDate(block_timestamp) >= today() - 7) dbt_subquery
    group by block_timestamp, group_address, token_id, batch_index, transaction_hash, log_index, event_name
    having count(*) > 1

)

select *
from validation_errors


