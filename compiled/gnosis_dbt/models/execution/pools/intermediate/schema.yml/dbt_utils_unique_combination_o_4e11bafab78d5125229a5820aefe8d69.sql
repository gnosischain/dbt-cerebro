





with validation_errors as (

    select
        block_timestamp, transaction_hash, log_index, token_address
    from (select * from `dbt`.`int_execution_pools_dex_liquidity_events` where toDate(block_timestamp) >= today() - 7) dbt_subquery
    group by block_timestamp, transaction_hash, log_index, token_address
    having count(*) > 1

)

select *
from validation_errors


